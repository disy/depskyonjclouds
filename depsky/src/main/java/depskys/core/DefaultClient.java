package depskys.core;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import jec.ReedSolDecoder;
import jec.ReedSolEncoder;

import org.yaml.snakeyaml.Yaml;

import pvss.InvalidVSSScheme;
import pvss.PVSSEngine;
import pvss.PublicInfo;
import pvss.PublishedShares;
import pvss.Share;
import util.Pair;
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
     * @param reg
     *            - the DataUnit associated with the file
     * @param value
     *            - value to be written
     * @return the hash of the value written
     * 
     */
    public synchronized byte[] write(DepSkyDataUnit reg, byte[] value) throws Exception {

        CloudRepliesControlSet rcs = null, wrcs = null;

        try {
            int seq = getNextSequence();
            rcs = new CloudRepliesControlSet(N, seq);
            mReplies.put(seq, rcs);
            // broadcast to all clouds to get the Metadata associated with this dataUnit
            broadcastGetMetadata(seq, reg, DepSkyManager.WRITE_PROTO, null);
            rcs.getWaitReplies().acquire(); // blocks until the semaphore is released
            mLastMetadataReplies = rcs.getReplies();
            // process replies and actualize version
            int nullCounter = 0;
            long maxVersionFound = -1;

            // proccess metadata replies
            for (int i = 0; i < rcs.getReplies().size(); i++) {
                DataCloudReply r = rcs.getReplies().get(i);
                if (r.getmResponse() == null || r.getmType() != DepSkyCloudManager.GET_DATA
                    || r.getmVersionNumber() == null) {
                    nullCounter++;
                    continue;
                } else {
                    long version = Long.parseLong(r.getmVersionNumber());
                    if (version > maxVersionFound) {
                        maxVersionFound = version;
                    }
                }
            }

            // when is the first write for this dataUnit (none version was found)
            if (nullCounter > F) {
                maxVersionFound = 0;
            }

            // calcule the name of the version to be written
            long nextVersion = maxVersionFound + DepSkyManager.MAX_CLIENTS + mClientId;

            seq = getNextSequence();
            wrcs = new CloudRepliesControlSet(N, seq);
            mReplies.put(seq, wrcs);
            byte[] allDataHash = generateSHA1Hash(value);

            // do the broadcast depending on the protocol selected for use (CA, A, only erasure codes or only
            // secret sharing)
            if (reg.isErsCodes()) {
                broadcastWriteValueErasureCodes(mSequence, reg, value, nextVersion + "", allDataHash);
            } else if (reg.isSecSharing()) {
                broadcastWriteValueSecretKeyShares(mSequence, reg, value, nextVersion + "", allDataHash);
            } else if (reg.isPVSS()) {
                broadcastWriteValueErasureCodesAndSecretKeyShares(mSequence, reg, value, nextVersion + "",
                    allDataHash);
            } else {
                broadcastWriteValueRequests(seq, reg, value, nextVersion + "", allDataHash);
            }

            wrcs.getWaitReplies().acquire();
            mLastReadReplies = wrcs.getReplies();

            reg.mLastVersionNumber = nextVersion;
            return allDataHash;
        } catch (Exception ex) {
            System.out.println("DEPSKYS WRITE ERROR:");
            // ex.printStackTrace();
            throw ex;
        }

    }

    /**
     * {@inheritDoc}
     */
    public int getClientId() {
        return this.mClientId;
    }

    /**
     * Read the version of the file associated reg that match with hashMatching
     * 
     * @param reg
     *            - the DataUnit associated with the file
     * @param hashMatching
     *            - the hash that represents the file version for read
     * @return the read data
     * @throws Exception
     * 
     */
    public synchronized byte[] readMatching(DepSkyDataUnit reg, byte[] hashMatching) throws Exception {

        mParallelRequests = true;
        mLastMetadataReplies = null;
        CloudRepliesControlSet rcs = null;

        try {

            int seq = getNextSequence();
            rcs = new CloudRepliesControlSet(N, seq);
            // broadcast to all clouds to get the metadata file and after the version requested
            broadcastGetMetadata(seq, reg, DepSkyManager.READ_PROTO, hashMatching);
            mReplies.put(seq, rcs);
            int nullResponses = 0;

            mLastReadMetadataSequence = seq;
            rcs.getWaitReplies().acquire(); // blocks until this semaphore is released
            mLastReadReplies = rcs.getReplies();
            int[] versionReceived = new int[N];
            int maxVerCounter = 0, oldVerCounter = 0;

            // process replies to analyze if we get correct responses
            for (int i = 0; i < rcs.getReplies().size(); i++) {
                DataCloudReply r = rcs.getReplies().get(i);
                if (r.getmResponse() == null || r.getmType() != DepSkyCloudManager.GET_DATA
                    || r.getmVersionNumber() == null || r.getmDataUnit() == null) {
                    nullResponses++;
                    // Fault check #1
                    if (nullResponses > N - F) {
                        throw new Exception(
                            "READ ERROR: DepSky-S DataUnit does not exist or client is offline (internet connection failed)");
                    } else if (nullResponses > F) {
                        // System.out.println(r.response + "  \n" + r.type + "  \n" + r.vNumber + "  \n" +
                        // r.reg.toString());
                        throw new Exception("READ ERROR: at least f + 1 clouds failed");
                    }
                } else {
                    Long maxVersionFound = r.getmDataUnit().getMaxVersion();
                    // process replies
                    if (reg.isPVSS()) {
                        // Data Unit using PVSS (retuns when having f + 1 sequential replies with
                        // maxVersionFound)
                        if (r.getmDataUnit().getMaxVersion() == null
                            || maxVersionFound.longValue() == Long.parseLong(r.getmVersionNumber())) {
                            // have max version
                            versionReceived[i] = 1;
                            maxVerCounter++;
                        } else {
                            // have old version
                            versionReceived[i] = 2;
                            oldVerCounter++;
                        }
                        reg = r.getmDataUnit();
                    } else {
                        // Data Unit NOT using PVSS (returns first reply with maxVersionFound in metadata)
                        if (maxVersionFound.longValue() == Long.parseLong(r.getmVersionNumber())) {
                            // reg.clearAllCaches();
                            mLastReadRepliesMaxVerIdx = i;
                            return (byte[])r.getmResponse();
                        }
                    }
                }
            }// for replies

            // get the value of each response (each cloud could have differents blocks)
            Share[] keyshares = new Share[N];
            Map<String, byte[]> erasurec = new HashMap<>();
            if (reg.isErsCodes() || reg.isSecSharing() || reg.isPVSS()) {

                for (int i = 0; i < rcs.getReplies().size(); i++) {
                    if (maxVerCounter >= T && versionReceived[i] != 1) {
                        DataCloudReply resp = rcs.getReplies().get(i);
                        resp.invalidateResponse();
                    } else if (oldVerCounter >= T && versionReceived[i] != 2) {
                        DataCloudReply resp = rcs.getReplies().get(i);
                        resp.invalidateResponse();
                    }
                }
                for (DataCloudReply r : rcs.getReplies()) {
                    int i = 0;
                    if (r.getmResponse() != null) {
                        byte[] ecksobjbytes = (byte[])r.getmResponse();
                        ByteArrayInputStream bais = new ByteArrayInputStream(ecksobjbytes);
                        ObjectInputStream ois = new ObjectInputStream(bais);
                        ECKSObject ecksobj;
                        ecksobj = (ECKSObject)ois.readObject();
                        if (ecksobj.getECfilename() != null)
                            erasurec.put(ecksobj.getECfilename(), ecksobj.getECbytes());
                        Share sk_share = ecksobj.getSKshare();
                        if (sk_share != null) {
                            keyshares[sk_share.getIndex()] = sk_share;
                            if (i < 1)
                                this.mResponse = ecksobj.getECbytes();
                            i++;
                        }
                    }
                }
            }

            // put together all blocks from the diferents clouds
            if (reg.isErsCodes()) {
                return readErasureCodes(reg, erasurec);
            } else if (reg.isSecSharing()) {
                return readSecretSharing(reg, keyshares);
            } else if (reg.isPVSS()) {
                return readSecretSharingErasureCodes(reg, keyshares, erasurec);
            }// is PVSS

            // not pvss or something went wrong with metadata
            throw new Exception("READ ERROR: Could not get data after processing metadata");

        } catch (Exception ex) {
            // ex.printStackTrace();
            throw ex;
        } finally {
            mParallelRequests = false;
            if (rcs != null) {
                mReplies.remove(rcs.getSequence());
            }
        }

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
            int seq = getNextSequence();
            rcs = new CloudRepliesControlSet(N, seq);
            mReplies.put(seq, rcs);
            broadcastGetMetadata(seq, reg, DepSkyManager.DELETE_ALL, null);
            rcs.getWaitReplies().acquire();
            mLastMetadataReplies = rcs.getReplies();

        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * NOT SUPORTED YET (waiting that all clouds support ACLs by container)
     */
    public void setAcl(DepSkyDataUnit reg, String permission, LinkedList<Pair<String, String>> cannonicalIds)
        throws Exception {

        CloudRepliesControlSet rcs = null, wrcs = null;

        try {
            int seq = getNextSequence();
            rcs = new CloudRepliesControlSet(N, seq);
            mReplies.put(seq, rcs);
            broadcastGetMetadata(seq, reg, DepSkyManager.ACL_PROTO, null);
            rcs.getWaitReplies().acquire();
            mLastMetadataReplies = rcs.getReplies();
            // process replies and actualize version
            int nullCounter = 0;
            long maxVersionFound = -1;
            for (int i = 0; i < rcs.getReplies().size(); i++) {
                DataCloudReply r = rcs.getReplies().get(i);
                if (r.getmResponse() == null || r.getmType() != DepSkyCloudManager.GET_DATA
                    || r.getmVersionNumber() == null) {
                    nullCounter++;
                    continue;
                } else {
                    long version = Long.parseLong(r.getmVersionNumber());
                    if (version > maxVersionFound) {
                        maxVersionFound = version;
                    }
                }
            }
            if (nullCounter > F) {
                // fazer qualquer coisa
            }

            seq = getNextSequence();
            wrcs = new CloudRepliesControlSet(N, seq);
            mReplies.put(seq, wrcs);
            broadcastSetContainersACL(seq, reg, DepSkyManager.ACL_PROTO, maxVersionFound, permission,
                cannonicalIds);

            wrcs.getWaitReplies().acquire();
            mLastReadReplies = wrcs.getReplies();

        } catch (Exception ex) {
            // System.out.println("DEPSKYS WRITE ERROR: " + ex.getMessage());
            // ex.printStackTrace();
            throw ex;
        }
    }

    /**
     * Method that releases (when receive N-F replies (in most of the cases)) all the locks made by
     * broadcasts
     * 
     * @param reply
     *            - reply received by each broadcast containing the response of the clouds
     * 
     */
    public void dataReceived(DataCloudReply reply) {

        if (!mReplies.containsKey(reply.getmSequenceNumber())) {
            // System.out.println("NOTE: sequence " + reply.sequence
            // + " replies already removed - " + reply);
            return;
        }

        CloudRepliesControlSet rcs = mReplies.get(reply.getmSequenceNumber());

        if (rcs != null) {
            rcs.getReplies().add(reply);
        } else {
            return;
        }

        // individual test measures (only add reply)
        if (reply.getmSequenceNumber() < 0) {
            return;
        }

        // processing reply
        if (reply.getmProtoOp() == DepSkyManager.ACL_PROTO) {
            if (rcs.getReplies().size() == 4) {
                rcs.getWaitReplies().release();
            }
        } else if (reply.getmProtoOp() == DepSkyManager.READ_PROTO
            && reply.getmVersionNumber().equals("true") && rcs.getReplies().size() >= N - F) { // read quorum
                                                                                               // when is a
                                                                                               // single file
                                                                                               // (written
                                                                                               // with
                                                                                               // operation
            // writeQuorum)
            rcs.getWaitReplies().release();

        } else if (reply.getmProtoOp() == DepSkyManager.READ_PROTO && reply.getmDataUnit() != null
            && reply.getmDataUnit().cloudVersions != null
            && reply.getmDataUnit().cloudVersions.size() >= (N - F) && rcs.getReplies() != null
            && rcs.getReplies().size() > 0 && !reply.getmDataUnit().isPVSS() && !reply.ismIsMetadataFile()) {
            // normal & optimized read trigger (reg without PVSS)
            Long maxVersion = reply.getmDataUnit().getMaxVersion();
            Long foundVersion = reply.getmDataUnit().getCloudVersion(reply.getmProviderId());
            if (maxVersion != null && maxVersion.longValue() == foundVersion.longValue()) {
                rcs.getWaitReplies().release();
                return;
            } else {
                // System.out.println(reply.cloudId + " does not have max version "
                // + maxVersion + " but has " + foundVersion);
            }
        } else if (reply.getmProtoOp() == DepSkyManager.READ_PROTO && reply.getmDataUnit() != null
            && reply.getmDataUnit().cloudVersions != null
            && reply.getmDataUnit().cloudVersions.size() >= (N - F) && reply.getmDataUnit().isPVSS()
            && rcs.getReplies() != null && rcs.getReplies().size() > F) {
            // SecretSharing read trigger (reg with PVSS)
            Long maxVersion = reply.getmDataUnit().getMaxVersion();
            int maxcounter = 0, othercounter = 0;
            for (int i = 0; i < rcs.getReplies().size(); i++) {
                DataCloudReply r = rcs.getReplies().get(i);
                if (r.getmResponse() != null && maxVersion != null
                    && Long.parseLong(r.getmVersionNumber()) == maxVersion.longValue()) {
                    maxcounter++;
                } else {
                    othercounter++;
                }
            }
            // check release -> have F + 1 shares of same version
            if (maxcounter > F || othercounter > F) {
                rcs.getWaitReplies().release();
                return;
            }
        } else if (reply.getmProtoOp() == DepSkyManager.READ_PROTO && rcs.getReplies().size() >= N - F) {
            int nonNull = 0, nulls = 0;
            for (int i = 0; i < rcs.getReplies().size(); i++) {
                if (rcs.getReplies().get(i).getmResponse() != null) {
                    nonNull++;
                } else {
                    nulls++;
                }
            }
            // release wait messages semaphore
            if (nonNull >= N - F || rcs.getReplies().size() > N - F || nulls > N - 2) {
                rcs.getWaitReplies().release();
                return;
            }
        } else if (reply.getmProtoOp() >= DepSkyManager.WRITE_PROTO && rcs.getReplies().size() >= N - F
            && reply.getmDataUnit() != null) {
            // write trigger (writes in all clouds)
            rcs.getWaitReplies().release();
            return;
        }

        // wait 4 replies when is about the start of the connections with the clouds and the delete operation
        if (rcs.getReplies().size() > N - F && reply.getmProtoOp() != DepSkyManager.WRITE_PROTO) {
            rcs.getWaitReplies().release();
            mReplies.remove(rcs.getSequence());
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

    /*
     * Compute the original data block when using secret sharing and erasure codes to replicate
     * the data
     */
    private synchronized byte[] readSecretSharingErasureCodes(DepSkyDataUnit reg, Share[] keyshares,
        Map<String, byte[]> erasurec) throws Exception {

        int originalSize = Integer.parseInt(reg.getErCodesReedSolMeta().split(";")[0]);
        byte[] enc_sk = recombineSecretKeyShares(reg, keyshares);
        byte[] ecmeta = reg.getErCodesReedSolMeta().replace(";", "\r\n").getBytes();
        erasurec.put("metadata", ecmeta);
        byte[] decode = concatAll(mDecoder.decode(erasurec), originalSize);
        return MyAESCipher.myDecrypt(new SecretKeySpec(enc_sk, "AES"), decode);
    }

    /*
     * Compute the original data block when using secret sharing to replicate the data
     */
    private synchronized byte[] readSecretSharing(DepSkyDataUnit reg, Share[] keyshares) throws Exception {

        byte[] enc_sk = recombineSecretKeyShares(reg, keyshares);

        return MyAESCipher.myDecrypt(new SecretKeySpec(enc_sk, "AES"), this.mResponse);
    }

    /*
     * Compute the original data block when using erasure codes to replicate the data
     */
    private synchronized byte[] readErasureCodes(DepSkyDataUnit reg, Map<String, byte[]> erasurec)
        throws Exception {

        int originalSize = Integer.parseInt(reg.getErCodesReedSolMeta().split(";")[0]);
        byte[] ecmeta = reg.getErCodesReedSolMeta().replace(";", "\r\n").getBytes();
        erasurec.put("metadata", ecmeta);
        byte[] decode = concatAll(mDecoder.decode(erasurec), originalSize);

        return decode;
    }

    /**
     * 
     * @return the identifier for the next broadcast
     */
    private synchronized int getNextSequence() {
        mSequence++;
        return mSequence;
    }

    // to be utilized by the lock algorithm
    private void writeQuorum(DepSkyDataUnit reg, byte[] value, String filename) {

        CloudRepliesControlSet rcs = null;
        try {
            int seq = getNextSequence();
            rcs = new CloudRepliesControlSet(N, seq);
            mReplies.put(seq, rcs);

            for (int i = 0; i < mCloudManagers.length; i++) {
                GeneralCloudRequest r =
                    new GeneralCloudRequest(DepSkyCloudManager.NEW_DATA, reg.getContainerId(mCloudManagers[i]
                        .getCloudId()), filename, System.currentTimeMillis());
                r.setmSeqNumber(mSequence);
                r.setmW_data(value);
                r.setReg(reg);
                r.setmProtoOp(DepSkyManager.WRITE_PROTO);
                r.setmIsMetadataFile(true);
                mDepSkyManager.doRequest(mCloudManagers[i].getCloudId(), r);
            }

            rcs.getWaitReplies().acquire();
        } catch (Exception e) {

        }
    }

    // to be utilized by the lock algorithm
    private LinkedList<byte[]> readQuorum(DepSkyDataUnit reg, String filename) {

        CloudRepliesControlSet rcs = null;
        try {
            int seq = getNextSequence();
            rcs = new CloudRepliesControlSet(N, seq);
            mReplies.put(seq, rcs);

            for (int i = 0; i < mCloudManagers.length; i++) {
                GeneralCloudRequest r =
                    new GeneralCloudRequest(DepSkyCloudManager.GET_DATA, reg.getContainerId(mCloudManagers[i]
                        .getCloudId()), filename, System.currentTimeMillis());
                r.setmSeqNumber(mSequence);
                r.setReg(reg);
                r.setmProtoOp(DepSkyManager.READ_PROTO);
                mDepSkyManager.doRequest(mCloudManagers[i].getCloudId(), r);
            }

            rcs.getWaitReplies().acquire();
            LinkedList<byte[]> readvalue = new LinkedList<byte[]>();
            for (int i = 0; i < rcs.getReplies().size(); i++) {
                DataCloudReply r = rcs.getReplies().get(i);
                if (r.getmVersionNumber().equals("true") && r.getmResponse() != null) {
                    byte[] data = (byte[])r.getmResponse();
                    readvalue.add(data);
                }
            }
            return readvalue;
        } catch (Exception e) {

        }
        return null;
    }

    // to be utilized by the lock algorithm
    private LinkedList<LinkedList<String>> listQuorum(DepSkyDataUnit reg) throws Exception {

        CloudRepliesControlSet rcs = null;
        try {
            int seq = getNextSequence();
            rcs = new CloudRepliesControlSet(N, seq);
            mReplies.put(seq, rcs);

            for (int i = 0; i < mCloudManagers.length; i++) {
                GeneralCloudRequest r =
                    new GeneralCloudRequest(DepSkyCloudManager.LIST, reg.getContainerId(mCloudManagers[i]
                        .getCloudId()), "", System.currentTimeMillis());
                r.setmSeqNumber(mSequence);
                r.setmIsMetadataFile(true);
                r.setReg(reg);
                mDepSkyManager.doRequest(mCloudManagers[i].getCloudId(), r);
            }

            LinkedList<LinkedList<String>> listPerClouds = new LinkedList<LinkedList<String>>();
            rcs.getWaitReplies().acquire();
            int nullcounter = 0;
            for (int i = 0; i < rcs.getReplies().size(); i++) {
                DataCloudReply r = rcs.getReplies().get(i);

                if (r.getmListNames() == null) {
                    nullcounter++;
                } else {
                    listPerClouds.add(r.getmListNames());
                }
            }
            if (nullcounter > N) {
                throw new Exception("No cloud responded to the request.");
            }
            return listPerClouds;

        } catch (Exception e) {

        }
        return null;
    }

    // to be utilized by the lock algorithm
    private void deleteData(DepSkyDataUnit reg, String name) {

        CloudRepliesControlSet rcs = null;
        try {
            int seq = getNextSequence();
            rcs = new CloudRepliesControlSet(N, seq);
            mReplies.put(seq, rcs);

            for (int i = 0; i < mCloudManagers.length; i++) {
                GeneralCloudRequest r =
                    new GeneralCloudRequest(DepSkyCloudManager.DEL_DATA, reg.getContainerId(mCloudManagers[i]
                        .getCloudId()), name, System.currentTimeMillis());
                r.setmSeqNumber(mSequence);
                r.setmIsMetadataFile(true);
                r.setReg(reg);
                mDepSkyManager.doRequest(mCloudManagers[i].getCloudId(), r);
            }
            rcs.getWaitReplies().acquire();

        } catch (Exception e) {

        }

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

    /**
     * TODO: adapt to jclouds
     * NOT CURRENTLY USED
     */
    private void broadcastSetContainersACL(int sequence, DepSkyDataUnit reg, int protoOp, long version,
        String permission, LinkedList<Pair<String, String>> cannonicalIds) {

        // for (int i = 0; i < drivers.length; i++) {
        // Pair<String, String> pair = cannonicalIds.get(i);
        // if (drivers[i] instanceof AmazonS3Driver && pair.getKey().equals("AMAZON-S3")) {
        // perDriverAclRequest(sequence, reg, protoOp, version, i, permission, pair.getValue());
        // } else if (drivers[i] instanceof GoogleStorageDriver && pair.getKey().equals("GOOGLE-STORAGE")) {
        // perDriverAclRequest(sequence, reg, protoOp, version, i, permission, pair.getValue());
        // } else if (drivers[i] instanceof WindowsAzureDriver && pair.getKey().equals("WINDOWS-AZURE")) {
        // perDriverAclRequest(sequence, reg, protoOp, version, i, permission, pair.getValue());
        // } else if (drivers[i] instanceof RackSpaceDriver && pair.getKey().equals("RACKSPACE")) {
        // perDriverAclRequest(sequence, reg, protoOp, version, i, permission, pair.getValue());
        // }
        // }
    }

    /**
     * TODO: adapt to jclouds
     * NOT CURRENTLY USED
     */
    private void perDriverAclRequest(int sequence, DepSkyDataUnit reg, int protoOp, long version,
        int driverPosition, String permission, String canonicalId) {

        // CloudRequest r =
        // new CloudRequest(DepSkySCloudManager.SET_ACL, sequence, drivers[driverPosition].getSessionKey(),
        // reg.getContainerName(), reg.getGivenVersionValueDataFileName(version + ""), reg, protoOp,
        // permission, canonicalId);
        // manager.doRequest(drivers[driverPosition].getDriverId(), r);
    }

    /*
     * Broadcast to write new data when using DepSky-A
     */
    private void broadcastWriteValueRequests(int sequence, DepSkyDataUnit reg, byte[] value, String version,
        byte[] allDataHash) {

        for (int i = 0; i < mCloudManagers.length; i++) {
            GeneralCloudRequest r =
                new GeneralCloudRequest(DepSkyCloudManager.NEW_DATA, reg.getContainerName(), reg
                    .getGivenVersionValueDataFileName(version), System.currentTimeMillis());
            r.setmSeqNumber(sequence);
            r.setmProtoOp(DepSkyManager.WRITE_PROTO);
            r.setmAllDataHash(allDataHash);
            r.setmVersionNumber(version);
            r.setmW_data(value);
            r.setReg(reg);
            mDepSkyManager.doRequest(mCloudManagers[i].getCloudId(), r);
        }
    }

    /*
     * Broadcast to write new data when using DepSky-CA
     */
    private void broadcastWriteValueErasureCodesAndSecretKeyShares(int sequence, DepSkyDataUnit reg,
        byte[] value, String version, byte[] allDataHash) throws Exception {

        SecretKey key = MyAESCipher.generateSecretKey();
        byte[] ciphValue = MyAESCipher.myEncrypt(key, value);

        Share[] keyshares = getKeyShares(reg, key.getEncoded());

        Map<String, byte[]> valueErasureCodes = mEncoder.encode(ciphValue);
        byte[] metabytes = valueErasureCodes.get("metadata");
        valueErasureCodes.remove("metadata");
        reg.setErCodesReedSolMeta(metabytes);

        // MERGE 1 ERASURE CODE AND 1 KEY SHARE AND SEND TO CLOUDS
        ByteArrayOutputStream data2write;
        ObjectOutputStream oos;
        Object[] ec_fnames = valueErasureCodes.keySet().toArray();
        for (int i = 0; i < mCloudManagers.length; i++) {
            data2write = new ByteArrayOutputStream();
            oos = new ObjectOutputStream(data2write);
            String ec_filename = (String)ec_fnames[i];
            ECKSObject obj = new ECKSObject(keyshares[i], ec_filename, valueErasureCodes.get(ec_filename));
            oos.writeObject(obj);
            oos.close();
            // send request
            GeneralCloudRequest r =
                new GeneralCloudRequest(DepSkyCloudManager.NEW_DATA, reg.getContainerName(), reg
                    .getGivenVersionValueDataFileName(version), System.currentTimeMillis());
            r.setmSeqNumber(sequence);
            r.setmProtoOp(DepSkyManager.WRITE_PROTO);
            r.setmAllDataHash(allDataHash);
            r.setmVersionNumber(version);
            r.setReg(reg);
            r.setmW_data(data2write.toByteArray());
            mDepSkyManager.doRequest(mCloudManagers[i].getCloudId(), r);
        }

    }

    /*
     * Broadcast to write new data when using only erasure codes (not use secret sharing)
     */
    private void broadcastWriteValueErasureCodes(int sequence, DepSkyDataUnit reg, byte[] value,
        String version, byte[] allDataHash) throws Exception {
        Map<String, byte[]> valueErasureCodes = mEncoder.encode(value);
        byte[] metabytes = valueErasureCodes.get("metadata");
        valueErasureCodes.remove("metadata");
        // SET META OF ENCODE FILE
        reg.setErCodesReedSolMeta(metabytes);
        // SEND EACH ERASURE CODE TO EACH CLOUD
        ByteArrayOutputStream data2write;
        ObjectOutputStream oos;
        Object[] ec_fnames = valueErasureCodes.keySet().toArray();
        String aux = "";

        for (int i = 0; i < mCloudManagers.length; i++) {
            data2write = new ByteArrayOutputStream();
            oos = new ObjectOutputStream(data2write);
            String ec_filename = (String)ec_fnames[i];
            ECKSObject obj = new ECKSObject(ec_filename, valueErasureCodes.get(ec_filename));
            oos.writeObject(obj);
            oos.close();
            // send request

            aux = reg.getContainerId(mCloudManagers[i].getCloudId());
            if (aux == null) {
                for (int j = 0; j < mCloudManagers.length; j++) {
                    if (reg.getContainerId(mCloudManagers[j].getCloudId()) != null)
                        aux = reg.getContainerId(mCloudManagers[j].getCloudId());
                }
            }

            GeneralCloudRequest r =
                new GeneralCloudRequest(DepSkyCloudManager.NEW_DATA, aux, reg
                    .getGivenVersionValueDataFileName(version), System.currentTimeMillis());
            r.setmSeqNumber(sequence);
            r.setmProtoOp(DepSkyManager.WRITE_PROTO);
            r.setmAllDataHash(allDataHash);
            r.setmVersionNumber(version);
            r.setReg(reg);
            r.setmW_data(data2write.toByteArray());
            mDepSkyManager.doRequest(mCloudManagers[i].getCloudId(), r);
        }
    }

    /*
     * Broadcast to write new data when using only secret sharing (not use erasure codes)
     */
    private void broadcastWriteValueSecretKeyShares(int sequence, DepSkyDataUnit reg, byte[] value,
        String version, byte[] allDataHash) throws Exception {

        SecretKey key = MyAESCipher.generateSecretKey();
        byte[] ciphValue = MyAESCipher.myEncrypt(key, value);

        Share[] keyshares = getKeyShares(reg, key.getEncoded());

        ByteArrayOutputStream data2write;
        ObjectOutputStream oos;
        for (int i = 0; i < mCloudManagers.length; i++) {
            data2write = new ByteArrayOutputStream();
            oos = new ObjectOutputStream(data2write);
            ECKSObject obj = new ECKSObject(keyshares[i], ciphValue);
            oos.writeObject(obj);
            oos.close();
            // send request
            GeneralCloudRequest r =
                new GeneralCloudRequest(DepSkyCloudManager.NEW_DATA, reg.getContainerName(), reg
                    .getGivenVersionValueDataFileName(version), System.currentTimeMillis());
            r.setmSeqNumber(sequence);
            r.setmProtoOp(DepSkyManager.WRITE_PROTO);
            r.setmAllDataHash(allDataHash);
            r.setmVersionNumber(version);
            r.setReg(reg);
            r.setmW_data(data2write.toByteArray());
            mDepSkyManager.doRequest(mCloudManagers[i].getCloudId(), r);
        }

    }

    private Share[] getKeyShares(DepSkyDataUnit reg, byte[] secretkey) throws Exception {

        PVSSEngine engine = PVSSEngine.getInstance(N, T, NUM_BITS);
        PublicInfo info = engine.getPublicInfo();
        reg.setPVSSinfo(info);
        BigInteger[] secretKeys = engine.generateSecretKeys();
        BigInteger[] publicKeys = new BigInteger[N];
        for (int i = 0; i < N; i++) {
            publicKeys[i] = engine.generatePublicKey(secretKeys[i]);
        }
        PublishedShares publishedShares = engine.generalPublishShares(secretkey, publicKeys, 1);// generate
                                                                                                // shares
        Share[] shares = new Share[N];
        for (int i = 0; i < N; i++) {
            shares[i] = publishedShares.getShare(i, secretKeys[i], info, publicKeys);
        }

        return shares;
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

    private byte[] recombineSecretKeyShares(DepSkyDataUnit reg, Share[] shares) throws IOException,
        ClassNotFoundException, InvalidVSSScheme {
        PVSSEngine engine = PVSSEngine.getInstance(reg.info);
        Share[] orderedShares = new Share[N];
        // share ordering for recombination to process or else it fails
        for (int i = 0; i < shares.length; i++) {
            Share s = shares[i];
            if (s == null) {
                continue;
            }
            orderedShares[s.getIndex()] = s;
        }

        return engine.generalCombineShares(orderedShares);
    }

    private byte[] concatAll(byte[][] decode, int originalSize) {

        // put all blocks together after decode
        byte[] result = new byte[originalSize];
        int offset = 0;
        for (int i = 0; i < decode.length; i++) {
            if (offset + decode[i].length < originalSize) {
                // copy all
                System.arraycopy(decode[i], 0, result, offset, decode[i].length);
                offset += decode[i].length;
            } else {
                // copy originalSize-offset
                System.arraycopy(decode[i], 0, result, offset, originalSize - offset);
                break;
            }
        }
        return result;
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

    public HashMap<Integer, CloudRepliesControlSet> getReplies() {
        return mReplies;
    }

    public void setReplies(HashMap<Integer, CloudRepliesControlSet> replies) {
        this.mReplies = replies;
    }

    public List<DataCloudReply> getLastReadReplies() {
        return mLastReadReplies;
    }

    public void setLastReadReplies(List<DataCloudReply> lastReadReplies) {
        this.mLastReadReplies = lastReadReplies;
    }

    public List<DataCloudReply> getLastMetadataReplies() {
        return mLastMetadataReplies;
    }

    public void setLastMetadataReplies(List<DataCloudReply> lastMetadataReplies) {
        this.mLastMetadataReplies = lastMetadataReplies;
    }

    public int getLastReadMetadataSequence() {
        return mLastReadMetadataSequence;
    }

    public void setLastReadMetadataSequence(int lastReadMetadataSequence) {
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
