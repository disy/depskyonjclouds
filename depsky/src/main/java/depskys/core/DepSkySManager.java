package depskys.core;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.Signature;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import depskys.clouds.CloudRepliesControlSet;
import depskys.clouds.CloudReply;
import depskys.clouds.CloudRequest;
import depskys.clouds.DepSkySCloudManager;
import depskys.clouds.ICloudDataManager;
import depskys.other.DepSkySKeyLoader;

/**
 * Class that process and construct new metadata files, and also do some security evaluations
 * 
 * @author tiago oliveira
 * @author bruno
 *         Modified by @author Andreas Rain, University of Konstanz
 * 
 */
public class DepSkySManager implements ICloudDataManager {

    public static final int RSA_SIG_LEN = 128;
    public static final int MAX_CLIENTS = 1000;// (ids: 0 to 999)
    public static final int READ_PROTO = 0;
    public static final int WRITE_PROTO = 1;
    public static final int ACL_PROTO = 2;
    public static final int DELETE_ALL = 3;
    public static final String CRLF = "\r\n";

    /**
     * The available cloud managers that can be used.
     */
    private DepSkySCloudManager[] mCloudManagers;
    /**
     * DepSkySKeyLoader
     */
    private DepSkySKeyLoader mKeyLoader;
    /**
     * Backreference to the client
     */
    private DepSkySClient mDepSkySClient;
    
    /**
     * Multiple clouds running in background
     */
    private ConcurrentHashMap<String, LinkedList<DepSkyMetadata>> cloud1;
    private ConcurrentHashMap<String, LinkedList<DepSkyMetadata>> cloud2;
    private ConcurrentHashMap<String, LinkedList<DepSkyMetadata>> cloud3;
    private ConcurrentHashMap<String, LinkedList<DepSkyMetadata>> cloud4;

    public DepSkySManager(DepSkySCloudManager[] pCloudManagers, IDepSkySProtocol depskys) {
        this.mCloudManagers = pCloudManagers;
        this.mKeyLoader = new DepSkySKeyLoader(null);
        this.mDepSkySClient = (DepSkySClient)depskys;

        cloud1 = new ConcurrentHashMap<String, LinkedList<DepSkyMetadata>>();
        cloud2 = new ConcurrentHashMap<String, LinkedList<DepSkyMetadata>>();
        cloud3 = new ConcurrentHashMap<String, LinkedList<DepSkyMetadata>>();
        cloud4 = new ConcurrentHashMap<String, LinkedList<DepSkyMetadata>>();

    }

    /**
     * Clear all request in queue to process
     */
    public void clearAllRequestsToProcess() {
        for (int i = 0; i < mCloudManagers.length; i++) {
            mCloudManagers[i].resetRequests();
        }
    }

    /**
     * Process metadata read from clouds. If is about an operation of write type, after verify a
     * correct metadata, the broadcast ends. If is operation fo read type,after verify a correct
     * metadata, is done a request to read the data requested
     * 
     * @param metadataReply
     *            - reply that contains information about the response of each request
     * 
     */
    public void processMetadata(CloudReply metadataReply) {
        try {
            metadataReply.setmReceiveTime(System.currentTimeMillis());
            ByteArrayInputStream biss = new ByteArrayInputStream((byte[])metadataReply.getmResponse());
            ObjectInputStream ins = new ObjectInputStream(biss);

            LinkedList<DepSkyMetadata> allmetadata = new LinkedList<DepSkyMetadata>();

            int size = ins.readInt();
            byte[] metadataInit = new byte[size];
            ins.read(metadataInit);

            size = ins.readInt();
            byte[] allMetadataSignature = new byte[size];
            ins.read(allMetadataSignature);

            biss.close();
            ins.close();

            biss = new ByteArrayInputStream(metadataInit);
            ins = new ObjectInputStream(biss);

            size = ins.readInt();
            for (int i = 0; i < size; i++) {
                DepSkyMetadata meta = new DepSkyMetadata();
                meta.readExternal(ins);
                allmetadata.add(meta);
            }

            biss.close();
            ins.close();

            String datareplied = null;
            DepSkyMetadata dm = null;
            int cont = 0;

            if (metadataReply.getmProtoOp() == DepSkySManager.DELETE_ALL) {
                String[] namesToDelete = new String[allmetadata.size() + 1];
                namesToDelete[0] = metadataReply.getmDataUnit().getMetadataFileName();
                for (int i = 1; i < allmetadata.size() + 1; i++) {
                    namesToDelete[i] = allmetadata.get(i - 1).getVersionFileId();
                }

                DepSkySCloudManager manager = getCloudManagerByProviderId(metadataReply.getmProviderId());
                CloudRequest r =
                    new CloudRequest(DepSkySCloudManager.DEL_CONT, metadataReply.getmContainerName(), null,
                        metadataReply.getmStartTime());
                r.setmSeqNumber(metadataReply.getmSequenceNumber());
                r.setmNamesToDelete(namesToDelete);
                r.setmProtoOp(DepSkySManager.DELETE_ALL);
                r.setReg(metadataReply.getmDataUnit());
                manager.doRequest(r);

                return;
            }

            // if is a read MAtching operation
            if (metadataReply.getmHashMatching() != null) {
                for (int i = 0; i < allmetadata.size(); i++) {
                    if (Arrays.equals(allmetadata.get(i).getAllDataHash(), metadataReply.getmHashMatching())) {
                        dm = allmetadata.get(i);
                        datareplied = dm.getMetadata();
                        if (datareplied.length() < 1) {
                            throw new Exception("invalid metadata size received");
                        }
                        cont = allmetadata.size() + 1;
                    }
                }
                if (cont < allmetadata.size() + 1)
                    throw new Exception("no matching version available");
            } else { // if is a normal read (last version read)
                dm = allmetadata.getFirst();
                datareplied = dm.getMetadata();
                if (datareplied.length() < 1) {
                    throw new Exception("invalid metadata size received");
                }
            }

            byte[] mdinfo = datareplied.getBytes();
            byte[] signature = dm.getsignature();
            Properties props = new Properties();
            props.load(new ByteArrayInputStream(mdinfo));
            // METADATA info
            String verNumber = props.getProperty("versionNumber");
            String verHash = props.getProperty("versionHash");
            String verValueFileId = props.getProperty("versionFileId");
            String verPVSSinfo = props.getProperty("versionPVSSinfo");
            String verECinfo = props.getProperty("versionECinfo");
            long versionfound = Long.parseLong(verNumber);
            // extract client id
            Long writerId = versionfound % MAX_CLIENTS;
            // metadata signature check
            if (!verifyMetadataSignature(writerId.intValue(), mdinfo, signature)
                || !verifyMetadataSignature(writerId.intValue(), metadataInit, allMetadataSignature)) {
                // invalid signature
                System.out.println("...........................");
                throw new Exception("Signature verification failed for " + metadataReply);
            }

            // set the data unit to the protocol in use
            if ((verPVSSinfo != null || verECinfo != null)/* && metadataReply.reg.info == null */) {
                if (verPVSSinfo != null && verECinfo == null) {
                    metadataReply.getmDataUnit().setUsingSecSharing(true);
                    metadataReply.getmDataUnit().setPVSSinfo(verPVSSinfo.split(";"));
                }
                if (verECinfo != null && verPVSSinfo == null) {
                    metadataReply.getmDataUnit().setUsingErsCodes(true);
                    if (metadataReply.getmProtoOp() == DepSkySManager.READ_PROTO) {
                        metadataReply.getmDataUnit().setErCodesReedSolMeta(verECinfo);
                    }
                }
                if (verECinfo != null && verPVSSinfo != null) {
                    metadataReply.getmDataUnit().setUsingPVSS(true);
                    metadataReply.getmDataUnit().setPVSSinfo(verPVSSinfo.split(";"));
                    if (metadataReply.getmProtoOp() == DepSkySManager.READ_PROTO) {
                        metadataReply.getmDataUnit().setErCodesReedSolMeta(verECinfo);

                    }
                }
            }
            long ts = versionfound - writerId;// remove client id from versionNumber
            metadataReply.setmVersionNumber(ts + "");// version received
            metadataReply.setmVHash(verHash);
            metadataReply.setmValueFileId(verValueFileId);// added

            if (metadataReply.getmProtoOp() == DepSkySManager.ACL_PROTO) {

                mDepSkySClient.dataReceived(metadataReply);
                return;
            }

            if (metadataReply.getmProtoOp() == DepSkySManager.WRITE_PROTO) {
                if (metadataReply.getmProviderId().equals("cloud1")) {
                    cloud1.put(metadataReply.getmContainerName(), allmetadata);
                } else if (metadataReply.getmProviderId().equals("cloud2")) {
                    cloud2.put(metadataReply.getmContainerName(), allmetadata);
                } else if (metadataReply.getmProviderId().equals("cloud3")) {
                    cloud3.put(metadataReply.getmContainerName(), allmetadata);
                } else if (metadataReply.getmProviderId().equals("cloud4")) {
                    cloud4.put(metadataReply.getmContainerName(), allmetadata);

                    mDepSkySClient.dataReceived(metadataReply);
                    return;
                }
                synchronized (this) {
                    if (metadataReply.getmSequenceNumber() == mDepSkySClient.lastReadMetadataSequence) {
                        if (mDepSkySClient.lastMetadataReplies == null) {
                            mDepSkySClient.lastMetadataReplies = new ArrayList<CloudReply>();
                        }
                        mDepSkySClient.lastMetadataReplies.add(metadataReply);
                        metadataReply.getmDataUnit().setCloudVersion(metadataReply.getmProviderId(), ts);
                        // System.out.println("IN:CLOUD VERSION " + ts + " for " + metadataReply.cloudId);
                    }
                    if (metadataReply.getmSequenceNumber() >= 0 && canReleaseAndReturn(metadataReply)) {
                        // check release
                        return;
                    }
                    if (!mDepSkySClient.sendingParallelRequests() && mDepSkySClient.sentOne) {
                        // depskys.dataReceived(metadataReply);
                    } else {
                        mDepSkySClient.sentOne = true;
                        DepSkySCloudManager manager =
                            getCloudManagerByProviderId(metadataReply.getmProviderId());
                        CloudRequest r =
                            new CloudRequest(DepSkySCloudManager.GET_DATA, metadataReply.getmContainerName(),
                                verValueFileId, metadataReply.getmStartTime());
                        r.setReg(metadataReply.getmDataUnit());
                        r.setmSeqNumber(metadataReply.getmSequenceNumber());
                        r.setmProtoOp(metadataReply.getmProtoOp());
                        r.setmVersionNumber(String.valueOf(ts));
                        r.setmVersionHash(verHash);
                        r.setmMetadataReceiveTime(metadataReply.getmMetadataReceiveTime());
                        manager.doRequest(r);// request valuedata file
                    }
                }// end synch this
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            System.out.println("ERROR_PROCESSING_METADATA: " + metadataReply);
            metadataReply.invalidateResponse();
        }
    }

    private boolean canReleaseAndReturn(CloudReply mdreply) {
        /* required in the case where we are waiting for n - f metadata replies and already have the value */
        try {
            CloudRepliesControlSet rcs = mDepSkySClient.replies.get(mdreply.getmSequenceNumber());

            if (rcs != null) {
                if (mdreply.getmDataUnit().cloudVersions.size() >= mDepSkySClient.N - mDepSkySClient.F
                    && rcs.getReplies().size() > mDepSkySClient.F && mdreply.getmDataUnit().isPVSS()) {
                    // System.out.println("pvssRnR_REPLIES SIZE = " + rcs.replies.size());
                    for (int i = 0; i < rcs.getReplies().size() - 1; i++) {
                        CloudReply r = rcs.getReplies().get(i);
                        if (r.getmResponse() != null
                            && r.getmVersionNumber() != null
                            && rcs.getReplies().get(i).getmVersionNumber().equals(
                                mdreply.getmDataUnit().getMaxVersion() + "")
                            && rcs.getReplies().get(i + 1).getmVersionNumber().equals(
                                mdreply.getmDataUnit().getMaxVersion() + "")) {
                            // System.out.println("RELEASED#processMetadata");
                            rcs.getWaitReplies().release();
                            return true;
                        }
                    }
                }
                if (mdreply.getmDataUnit().cloudVersions.size() >= mDepSkySClient.N - mDepSkySClient.F
                    && rcs.getReplies().size() > 0 && !mdreply.getmDataUnit().isPVSS()) {
                    // System.out.println("normalRnR_REPLIES SIZE = " + rcs.replies.size());
                    for (int i = 0; i < rcs.getReplies().size(); i++) {
                        CloudReply r = rcs.getReplies().get(i);
                        if (r.getmResponse() != null
                            && r.getmVersionNumber() != null
                            && rcs.getReplies().get(i).getmVersionNumber().equals(
                                mdreply.getmDataUnit().getMaxVersion() + "")) {
                            // System.out.println("RELEASED#processMetadata");
                            rcs.getWaitReplies().release();
                            return true;
                        }
                    }
                }

            }
        } catch (Exception e) {
            // e.printStackTrace();
        }
        return false;
    }

    /**
     * Verify the data integrity
     * 
     * @param valuedataReply
     *            - reply with the metadata that contains the metedata e data for
     *            a DataUnit
     * 
     */
    public void checkDataIntegrity(CloudReply valuedataReply) {
        try {
            byte[] value = (byte[])valuedataReply.getmResponse(); // data value
            String valuehash = getHexString(getHash(value)); // hash of data value
            valuedataReply.setmReceiveTime(System.currentTimeMillis());
            if (valuehash.equals(valuedataReply.getmVHash())) { // comparing hash of data value with the hash
                // presented in metadata file
                mDepSkySClient.dataReceived(valuedataReply);
            } else {
                throw new Exception("integrity verification failed... " + valuedataReply);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            valuedataReply.invalidateResponse();
            valuedataReply.setmExceptionMessage(ex.getMessage());
        }
    }

    /**
     * After the data file is write, new metadata is processed and then stored in the clouds
     * 
     * @param reply
     *            - reply after a request processed
     */
    public void writeNewMetadata(CloudReply reply) {
        ByteArrayOutputStream allmetadata = null;
        ObjectOutputStream out = null;
        try {
            // build new version metadata
            allmetadata = new ByteArrayOutputStream();
            out = new ObjectOutputStream(allmetadata);
            String valueDataFileId = (String)reply.getmResponse();
            String mprops =
                "versionNumber = " + reply.getmVersionNumber() + CRLF + "versionHash = "
                    + getHexString(getHash(reply.getmValue())) + CRLF + "allDataHash = "
                    + reply.getmAllDataHash() + CRLF + "versionFileId = " + valueDataFileId + CRLF;
            if (reply.getmDataUnit().isErsCodes()) {
                mprops += "versionECinfo = " + reply.getmDataUnit().getErCodesReedSolMeta() + CRLF;
            } else if (reply.getmDataUnit().isSecSharing()) {
                mprops += "versionPVSSinfo = " + reply.getmDataUnit().getPVSSPublicInfoAsString() + CRLF;
            } else if (reply.getmDataUnit().isPVSS()) {
                // if this refers to a Data Unit with PVSS
                mprops += "versionPVSSinfo = " + reply.getmDataUnit().getPVSSPublicInfoAsString() + CRLF;
                mprops += "versionECinfo = " + reply.getmDataUnit().getErCodesReedSolMeta() + CRLF;
            }

            // getting the last versions metadata information
            DepSkyMetadata newMD =
                new DepSkyMetadata(mprops, getSignature(mprops.getBytes()), reply.getmAllDataHash(),
                    valueDataFileId);
            LinkedList<DepSkyMetadata> oldMetadata = new LinkedList<DepSkyMetadata>();

            Map<String, LinkedList<DepSkyMetadata>> maptoUse = null;

            switch (reply.getmProviderId()) {
            case "cloud1":
                maptoUse = cloud1;
                break;
            case "cloud2":
                maptoUse = cloud2;
                break;
            case "cloud3":
                maptoUse = cloud3;
                break;
            case "cloud4":
                maptoUse = cloud4;
                break;
            }

            if (maptoUse.containsKey(reply.getmContainerName())) {
                oldMetadata = new LinkedList<DepSkyMetadata>(maptoUse.get(reply.getmContainerName()));
                maptoUse.remove(reply.getmContainerName());
            }

            oldMetadata.addFirst(newMD);
            out.writeInt(oldMetadata.size());
            for (int i = 0; i < oldMetadata.size(); i++) {
                oldMetadata.get(i).writeExternal(out);
            }
            out.close();
            allmetadata.close();
            byte[] metadataInit = allmetadata.toByteArray();
            byte[] allMetadataSignature = getSignature(metadataInit);

            allmetadata = new ByteArrayOutputStream();
            out = new ObjectOutputStream(allmetadata);

            out.writeInt(metadataInit.length);
            out.write(metadataInit);
            out.writeInt(allMetadataSignature.length);
            out.write(allMetadataSignature);

            allmetadata.flush();
            out.flush();

            // request to write new metadata file
            DepSkySCloudManager manager = getCloudManagerByProviderId(reply.getmProviderId());
            CloudRequest r =
                new CloudRequest(DepSkySCloudManager.NEW_DATA, reply.getmContainerName(), reply
                    .getmDataUnit().regId
                    + "metadata", reply.getmStartTime());
            r.setmSeqNumber(reply.getmSequenceNumber());
            r.setReg(reply.getmDataUnit());
            r.setmAllDataHash(allmetadata.toByteArray());
            r.setmProtoOp(reply.getmProtoOp());
            r.setmIsMetadataFile(true);
            r.setmHashMatching(reply.getmHashMatching());
            manager.doRequest(r);

        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            try {
                out.close();
                allmetadata.close();
            } catch (IOException e) {

            }
        }

    }

    /**
     * Signs a byte array
     * 
     * @param v
     *            - content to sing
     * @return the signature of v
     */
    public byte[] getSignature(byte[] v) {
        try {
            Signature sig = Signature.getInstance("SHA1withRSA");
            sig.initSign(mKeyLoader.loadPrivateKey(mDepSkySClient.getClientId()));
            sig.update(v);
            return sig.sign();
        } catch (Exception ex) {
            // ex.printStackTrace();
        }
        return null;
    }

    /**
     * Verify if a given signature for a given byte array is valid
     * 
     * @param clientId
     *            - client id
     * @param v
     *            - metadata
     * @param signature
     *            - metadata signature
     * @return true is a valid signature, false otherwise
     */
    public boolean verifyMetadataSignature(int clientId, byte[] v, byte[] signature) {
        try {
            Signature sig = Signature.getInstance("SHA1withRSA");
            sig.initVerify(mKeyLoader.loadPublicKey(clientId));
            sig.update(v);
            return sig.verify(signature);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return false;
    }

    public DepSkySCloudManager getCloudManagerByProviderId(String id) {
        for (DepSkySCloudManager manager : mCloudManagers) {
            if (manager.getmProvider().equals(id)) {
                return manager;
            }
        }
        return null;
    }

    /**
     * Compute a hash for a given byte array
     * 
     * @param o
     *            the byte array to be hashed
     * @return the hash of the byte array
     */
    private byte[] getHash(byte[] v) throws NoSuchAlgorithmException {
        // MessageDigest md = MessageDigest.getInstance("SHA-1");
        return MessageDigest.getInstance("SHA-1").digest(v);
    }

    // base16 char table (aux in getHexString)
    private static final byte[] HEX_CHAR_TABLE = {
        (byte)'0', (byte)'1', (byte)'2', (byte)'3', (byte)'4', (byte)'5', (byte)'6', (byte)'7', (byte)'8',
        (byte)'9', (byte)'a', (byte)'b', (byte)'c', (byte)'d', (byte)'e', (byte)'f'
    };

    private static String getHexString(byte[] raw) throws UnsupportedEncodingException {
        byte[] hex = new byte[2 * raw.length];
        int index = 0;
        for (byte b : raw) {
            int v = b & 0xFF;
            hex[index++] = HEX_CHAR_TABLE[v >>> 4];
            hex[index++] = HEX_CHAR_TABLE[v & 0xF];
        }
        return new String(hex, "ASCII");
    }

    public void doRequest(String cloudId, CloudRequest request) {
        getCloudManagerByProviderId(cloudId).doRequest(request);
    }
    
    public void setCloudManagers(DepSkySCloudManager[] pCloudManagers){
        mCloudManagers = pCloudManagers;
    }
    
}
