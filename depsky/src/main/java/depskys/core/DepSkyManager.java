package depskys.core;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.Signature;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import depskys.clouds.DepSkyCloudManager;
import depskys.clouds.ICloudDataManager;
import depskys.clouds.replys.DataCloudReply;
import depskys.clouds.replys.MetaCloudReply;
import depskys.clouds.requests.GeneralCloudRequest;
import depskys.other.DepSkySKeyLoader;

/**
 * Class that process and construct new metadata files, and also do some security evaluations
 * 
 * @author tiago oliveira
 * @author bruno
 *         Modified by @author Andreas Rain, University of Konstanz
 * 
 */
public class DepSkyManager implements ICloudDataManager {

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
    private DepSkyCloudManager[] mCloudManagers;
    /**
     * DepSkySKeyLoader
     */
    private DepSkySKeyLoader mKeyLoader;
    /**
     * Backreference to the client
     */
    private DefaultClient mDepSkySClient;
    
    /**
     * Multiple clouds running in background
     */
    private Map<String, ConcurrentHashMap<String, LinkedList<DepSkyMetadata>>> mClouds;

    public DepSkyManager(DepSkyCloudManager[] pCloudManagers, IDepSkyClient depskys) {
        this.mCloudManagers = pCloudManagers;
        this.mKeyLoader = new DepSkySKeyLoader(null);
        this.mDepSkySClient = (DefaultClient)depskys;

        mClouds = new HashMap<String, ConcurrentHashMap<String,LinkedList<DepSkyMetadata>>>();
    }
    
    public void initClouds(){
        for(DepSkyCloudManager cloud : this.mCloudManagers){
            mClouds.put(cloud.getCloudId(), new ConcurrentHashMap<String, LinkedList<DepSkyMetadata>>());
        }
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

    public DepSkyCloudManager getCloudManagerByProviderId(String id) {
        for (DepSkyCloudManager manager : mCloudManagers) {
            if (manager.getCloudId().equals(id)) {
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

    public void doRequest(String cloudId, GeneralCloudRequest request) {
        getCloudManagerByProviderId(cloudId).doRequest(request);
    }
    
    public void setCloudManagers(DepSkyCloudManager[] pCloudManagers){
        mCloudManagers = pCloudManagers;
    }

    @Override
    public void processMetadata(MetaCloudReply metadataReply) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void checkDataIntegrity(DataCloudReply valuedataReply) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void writeNewMetadata(DataCloudReply reply) {
        // TODO Auto-generated method stub
        
    }
    
}
