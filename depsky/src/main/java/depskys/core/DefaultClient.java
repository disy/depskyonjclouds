package depskys.core;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import jec.ReedSolDecoder;
import jec.ReedSolEncoder;

import org.yaml.snakeyaml.Yaml;

import depskys.clouds.CloudRepliesControlSet;
import depskys.clouds.DepSkyCloudManager;
import depskys.clouds.replys.DataCloudReply;
import depskys.clouds.replys.ICloudReply;
import depskys.clouds.replys.MetaCloudReply;
import depskys.clouds.requests.GeneralCloudRequest;
import depskys.core.configuration.Account;
import depskys.core.configuration.Configuration;
import depskys.core.exceptions.CouldNotGetDataException;
import depskys.core.exceptions.DepSkyException;
import depskys.core.exceptions.IDepSkyWriteException;
import depskys.core.exceptions.NoDataAvailableException;
import depskys.core.exceptions.TooManyCloudsBrokeException;

/**
 * Class for using DepSky
 * 
 * @author tiago oliveira
 * @author bruno quaresma
 *         Modified by @author Andreas Rain, University of Konstanz
 */
public class DefaultClient implements IDepSkyClient {

    public int N, F, T = 2/* jss_shares=f+1 */, NUM_BITS = 192;

    private int mClientId;
    private long mSequence = -1;
    private DepSkyCloudManager[] mCloudManagers;
    private DepSkyManager mDepSkyManager;
    private HashMap<Long, CloudRepliesControlSet> mReplies;
    private boolean mParallelRequests = false; // Optimized Read or Normal Read

    private List<ICloudReply> mLastReadReplies; // pointer for planet lab stats
    private List<ICloudReply> mLastMetadataReplies; // pointer for planet lab stats
    private long mLastReadMetadataSequence = -1;
    private int mLastReadRepliesMaxVerIdx = -1;
    private boolean mSentOne = false;
    private byte[] mResponse = null;
    private ReedSolDecoder mDecoder;
    private ReedSolEncoder mEncoder;

    private final String mConfigPath;

    private ExecutorService mCloudService;

    /**
     * 
     * @param clientId
     *            - client ID
     * @param useModel
     *            - if false, DepSky will work locally (all the data is stored in local machine - you
     *            must run the server first, see README file),
     *            if true, cloudofclouds are used instead
     * 
     */
    public DefaultClient(int clientId, String pConfigPath) {
        this.mClientId = clientId;
        this.mConfigPath = pConfigPath;
        List<Account> credentials = null;
        try {
            credentials = readCredentials();
        } catch (FileNotFoundException e) {
            System.out.println("account.props.yml file dosen't exist!");
            e.printStackTrace();
        } catch (ParseException e) {
            System.out.println("account.props.yml misconfigured!");
            e.printStackTrace();
        }

        this.mCloudManagers = new DepSkyCloudManager[credentials.size()];

        this.mDepSkyManager = new DepSkyManager(null, this);

        int c = 0;
        for (Account acc : credentials) {
            DepSkyCloudManager cloudManager = new DepSkyCloudManager(acc, mDepSkyManager, this);
            mCloudManagers[c] = cloudManager;
            c++;
        }

        mDepSkyManager.setCloudManagers(mCloudManagers);
        mDepSkyManager.initClouds();

        this.mReplies = new HashMap<Long, CloudRepliesControlSet>();
        this.N = credentials.size();
        this.F = 1;
        this.mEncoder = new ReedSolEncoder(N / 2, (int)Math.ceil(Double.valueOf(N) / 2), 8);
        this.mDecoder = new ReedSolDecoder(N / 2, (int)Math.ceil(Double.valueOf(N) / 2), 8);

        startCloudManagers();
    }

    /**
     * Read the last version written for the file associated with reg
     * 
     * @param pUnit
     *            - the DataUnit associated with the file
     * @throws InterruptedException 
     * 
     */
    public synchronized byte[] read(DepSkyDataUnit pUnit) throws DepSkyException, InterruptedException {

        mParallelRequests = true;
        CloudRepliesControlSet rcs = null;

        try {
            long seq = getNextSequence();
            rcs = new CloudRepliesControlSet(N, seq);
            // broadcast to all clouds to get the metadata file and after the version requested
            mReplies.put(seq, rcs);
            broadcastGetMetadata(seq, pUnit, DepSkyManager.READ_PROTO, null);
            int nullResponses = 0;
            // process value data responses
            mLastReadMetadataSequence = seq;
            rcs.getWaitReplies().acquire(); // blocks until this semaphore is released
            mLastReadReplies = rcs.getReplies();
            List<MetaCloudReply> maxVersionReplys = new ArrayList<>();
            Long maxVersion = null;

            // process metadata replies, choose from which cloud to read the data.
            for (ICloudReply reply : rcs.getReplies()) {
                MetaCloudReply r;
                if (reply != null && reply instanceof MetaCloudReply) {
                    r = (MetaCloudReply)reply;
                } else if (reply == null) {
                    nullResponses++;
                    continue;
                } else {
                    throw new IllegalStateException("Retrieved a data reply instead of a metadata");
                }
                
                if(maxVersion == null){
                    maxVersion = r.getDataUnit().getMaxVersion();
                }

                if (nullResponses >= N - F) {
                    throw new NoDataAvailableException();
                } else if (nullResponses > F) {
                    throw new TooManyCloudsBrokeException();
                }

                Long rVersion = Long.valueOf(r.getVersionNumber());

                // See if this version is maximal
                if (maxVersion.equals(rVersion)) {
                    // if so, add it to the replies
                    maxVersionReplys.add(r);
                }
            }

            if (maxVersionReplys.size() == 0) {
                throw new NoDataAvailableException();
            }

            // Setting the cloud reply set only to the size of the max version replies
            rcs = new CloudRepliesControlSet(maxVersionReplys.size(), seq);
            mReplies.put(seq, rcs);
            
            for(MetaCloudReply maxVersionReply : maxVersionReplys){
                DepSkyDataUnit readUnit =
                    new DepSkyDataUnit(maxVersionReply.getDataUnit().getContainerName(), maxVersionReply
                        .getDataUnit().getGivenVersionValueDataFileName(maxVersionReply.getVersionNumber()));
                readUnit.setCloudRequirement(maxVersionReplys.size(), 1);

                GeneralCloudRequest request =
                    new GeneralCloudRequest(DepSkyCloudManager.GET_DATA, readUnit, maxVersionReply
                        .getSequenceNumber(), maxVersionReply.getStartTime());
                request.setProtoOp(maxVersionReply.getProtoOp());
                request.setVersionHash(maxVersionReply.getVersionHash());
                request.setVersionNumber(maxVersionReply.getVersionNumber());
                
                mDepSkyManager.doRequest(maxVersionReply.getProviderId(), request);
            }
            
            rcs.getWaitReplies().acquire(); // blocks until this semaphore is released
            mLastReadReplies = rcs.getReplies();
            
            if(!mLastReadReplies.isEmpty()){
                for(ICloudReply dataReply : mLastReadReplies){
                    if(dataReply instanceof DataCloudReply && ((DataCloudReply) dataReply).getResponse() != null){
                        return ((DataCloudReply) dataReply).getResponse();
                    }
                }
            }
            
            throw new CouldNotGetDataException();

        } finally {
            mParallelRequests = false;
            if (rcs != null) {
                mReplies.remove(rcs.getSequence());
            }
        }
    }

    /**
     * Writes the value value in the corresponding dataUnit reg
     * 
     * @param pDataUnit
     *            - the DataUnit associated with the file
     * @param value
     *            - value to be written
     * @return the hash of the value written
     * 
     */
    public synchronized byte[] write(DepSkyDataUnit pDataUnit, byte[] value) throws DepSkyException {

        CloudRepliesControlSet rcs = null, wrcs = null;
        
            long seq = getNextSequence();
            rcs = new CloudRepliesControlSet(N, seq);
            // broadcast to all clouds to get the metadata file and after the version requested
            mReplies.put(seq, rcs);
            broadcastGetMetadata(seq, pDataUnit, DepSkyManager.READ_PROTO, null);
            int nullResponses = 0;
            // process value data responses
            mLastReadMetadataSequence = seq;
            try {
                rcs.getWaitReplies().acquire();
            } catch (InterruptedException e1) {
                throw new IDepSkyWriteException("While acquiring the metadata the client got interrupted.");
            } // blocks until this semaphore is released
            mLastReadReplies = rcs.getReplies();
            List<MetaCloudReply> maxVersionReplys = new ArrayList<>();
            Long maxVersion = null;

            // process metadata replies, choose from which cloud to read the data.
            for (ICloudReply reply : rcs.getReplies()) {
                MetaCloudReply r;
                if (reply != null && reply instanceof MetaCloudReply) {
                    r = (MetaCloudReply)reply;
                } else if (reply == null) {
                    nullResponses++;
                    continue;
                } else {
                    throw new IllegalStateException("Retrieved a data reply instead of a metadata");
                }
                
                if(maxVersion == null){
                    maxVersion = r.getDataUnit().getMaxVersion();
                }

                if (nullResponses >= N - F) {
                    throw new NoDataAvailableException();
                } else if (nullResponses > F) {
                    throw new TooManyCloudsBrokeException();
                }

                Long rVersion = Long.valueOf(r.getVersionNumber());

                // See if this version is maximal
                if (maxVersion.equals(rVersion)) {
                    // if so, add it to the replies
                    maxVersionReplys.add(r);
                }
            }
            long nextVersion = DepSkyManager.MAX_CLIENTS + mClientId;
            if (maxVersionReplys.size() != 0) {
                nextVersion = maxVersion + DepSkyManager.MAX_CLIENTS + mClientId;
            }

            seq = getNextSequence();
            wrcs = new CloudRepliesControlSet(maxVersionReplys.size(), seq);
            mReplies.put(seq, wrcs);
            byte[] allDataHash = generateSHA1Hash(value);
            
            broadcastWriteValueRequests(seq, pDataUnit, value, nextVersion + "", allDataHash);

            try {
                wrcs.getWaitReplies().acquire();
            } catch (InterruptedException e) {
                throw new IDepSkyWriteException("While acquiring the write request the client got interrupted.");
            }
            mLastReadReplies = wrcs.getReplies();

            pDataUnit.setLastVersionNumber(nextVersion);
            return allDataHash;

    }

    /**
     * {@inheritDoc}
     */
    public int getClientId() {
        return this.mClientId;
    }

    /**
     * Delete all the data (data and metadata files) associated with this Data Unit
     * 
     * @param reg
     *            - Data Unit
     */
    public void deleteContainer(DepSkyDataUnit reg) throws Exception {

        CloudRepliesControlSet rcs = null;
        try {
            long seq = getNextSequence();
            rcs = new CloudRepliesControlSet(N, seq);
            mReplies.put(seq, rcs);
            broadcastGetMetadata(seq, reg, DepSkyManager.DELETE_ALL, null);
            rcs.getWaitReplies().acquire();
            mLastMetadataReplies = rcs.getReplies();

        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public boolean sendingParallelRequests() {
        return mParallelRequests;
    }

    /**
     * Start all connections with the clouds (this operations only get success if all the four
     * connections are performed correctly)
     * 
     */
    private void startCloudManagers() {
        System.out.println("starting drivers...");

        mCloudService = Executors.newFixedThreadPool(4);
        for (DepSkyCloudManager manager : mCloudManagers) {
            mCloudService.submit(manager);
        }

        System.out.println("All drivers started.");
    }

    /**
     * 
     * @return the identifier for the next broadcast
     */
    private synchronized long getNextSequence() {
        mSequence++;
        return mSequence;
    }

    /*
     * Get the metadata file (used for read, write and delete operations)
     */
    private void broadcastGetMetadata(long pSequence, DepSkyDataUnit pUnit, int protoOp, byte[] hashMatching) {

        for (int i = 0; i < mCloudManagers.length; i++) {
            GeneralCloudRequest r =
                new GeneralCloudRequest(DepSkyCloudManager.GET_META, pUnit, pSequence, System
                    .currentTimeMillis());
            r.setProtoOp(protoOp);
            r.setHashMatching(hashMatching);
            r.setDataUnit(pUnit);

            mDepSkyManager.doRequest(mCloudManagers[i].getCloudId(), r);
        }

    }

   

    /*
     * Broadcast to write new data when using DepSky-A
     */
    private void broadcastWriteValueRequests(long sequence, DepSkyDataUnit pDataUnit, byte[] value, String version,
        byte[] allDataHash) {

        for (int i = 0; i < mCloudManagers.length; i++) {
            GeneralCloudRequest r = new GeneralCloudRequest(DepSkyCloudManager.NEW_DATA, pDataUnit, sequence, System.currentTimeMillis());
            r.setProtoOp(DepSkyManager.WRITE_PROTO);
            r.setVersionNumber(version);
            r.setAllDataHash(allDataHash);
            r.setData(value);
            mDepSkyManager.doRequest(mCloudManagers[i].getCloudId(), r);
        }
    }
    
    private byte[] generateSHA1Hash(byte[] data) {
        try {
            MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
            return sha1.digest(data);

        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * Read the credentials of drivers accounts
     */
    private List<Account> readCredentials() throws FileNotFoundException, ParseException {
        InputStream in = null;
        Yaml yaml = null;
        Configuration config = null;
        if (mConfigPath == null) {
            String yamlPath =
                new StringBuilder().append("src").append(File.separator).append("main")
                    .append(File.separator).append("resources").append(File.separator).append(
                        "account.props.yml").toString();
            in = new FileInputStream(new File(yamlPath));
        } else {
            in = new FileInputStream(new File(mConfigPath));
        }
        yaml = new Yaml();
        config = (Configuration)yaml.load(in);
        return config.getClouds();
    }

    public HashMap<Long, CloudRepliesControlSet> getReplies() {
        return mReplies;
    }

    public void setReplies(HashMap<Long, CloudRepliesControlSet> replies) {
        this.mReplies = replies;
    }

    public List<ICloudReply> getLastReadReplies() {
        return mLastReadReplies;
    }

    public void setLastReadReplies(List<ICloudReply> lastReadReplies) {
        this.mLastReadReplies = lastReadReplies;
    }

    public List<ICloudReply> getLastMetadataReplies() {
        return mLastMetadataReplies;
    }

    public void setLastMetadataReplies(List<ICloudReply> lastMetadataReplies) {
        this.mLastMetadataReplies = lastMetadataReplies;
    }

    public long getLastReadMetadataSequence() {
        return mLastReadMetadataSequence;
    }

    public void setLastReadMetadataSequence(long lastReadMetadataSequence) {
        this.mLastReadMetadataSequence = lastReadMetadataSequence;
    }

    public int getLastReadRepliesMaxVerIdx() {
        return mLastReadRepliesMaxVerIdx;
    }

    public void setLastReadRepliesMaxVerIdx(int lastReadRepliesMaxVerIdx) {
        this.mLastReadRepliesMaxVerIdx = lastReadRepliesMaxVerIdx;
    }

    public boolean isSentOne() {
        return mSentOne;
    }

    public void setSentOne(boolean sentOne) {
        this.mSentOne = sentOne;
    }

    public byte[] getResponse() {
        return mResponse;
    }

    public void setResponse(byte[] response) {
        this.mResponse = response;
    }

    public void setClientId(int clientId) {
        this.mClientId = clientId;
    }
}
