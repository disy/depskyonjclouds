package depskys.core;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import depskys.clouds.DepSkyCloudManager;
import depskys.clouds.ICloudDataManager;
import depskys.clouds.replys.DataCloudReply;
import depskys.clouds.replys.MetaCloudReply;
import depskys.clouds.requests.GeneralCloudRequest;

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
     * Multiple clouds running in background
     */
    private Map<String, ConcurrentHashMap<String, LinkedList<DepSkyMetadata>>> mClouds;

    public DepSkyManager() {
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

    public DepSkyCloudManager getCloudManagerByProviderId(String id) {
        for (DepSkyCloudManager manager : mCloudManagers) {
            if (manager.getCloudId().equals(id)) {
                return manager;
            }
        }
        return null;
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
        HashFunction hf = Hashing.md5();
        HashCode hc = hf.newHasher().putBytes(valuedataReply.getResponse()).hash();
        if(valuedataReply.getAllDataHash().equals(hc.asBytes())){
            valuedataReply.setValidResponse(true);
        }
        
    }

    @Override
    public void writeNewMetadata(DataCloudReply reply) {
        // TODO Auto-generated method stub
        
    }
    
}
