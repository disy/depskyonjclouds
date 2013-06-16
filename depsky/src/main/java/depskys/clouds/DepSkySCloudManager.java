package depskys.clouds;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.contains;

import java.io.File;
import java.util.LinkedList;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;

import org.jclouds.ContextBuilder;
import org.jclouds.apis.ApiMetadata;
import org.jclouds.apis.Apis;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.PageSet;
import org.jclouds.blobstore.domain.StorageMetadata;
import org.jclouds.filesystem.reference.FilesystemConstants;
import org.jclouds.providers.ProviderMetadata;
import org.jclouds.providers.Providers;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;

import depskys.core.IDepSkySProtocol;
import depskys.core.configuration.Account;

/**
 * Class that represents all the request and replies for each cloud
 * Note there is N object of this class (each object e connected with each cloud)
 * 
 * @author tiago oliveira
 * @author bruno quaresma
 * 
 *         Modified by @author Andreas Rain, University of Konstanz
 */
public class DepSkySCloudManager implements Callable<Void> {

    public static final int INIT_SESS = 0;
    public static final int NEW_CONT = 1;
    public static final int DEL_CONT = 2;
    public static final int NEW_DATA = 3;
    public static final int GET_DATA = 4;
    public static final int DEL_DATA = 5;
    public static final int GET_DATA_ID = 6;
    public static final int GET_CONT_AND_DATA_ID = 7;
    public static final int SET_ACL = 8;
    public static final int LIST = 9;

    /** JClouds properties */
    public static final Map<String, ApiMetadata> allApis = Maps.uniqueIndex(Apis
        .viewableAs(BlobStoreContext.class), Apis.idFunction());

    public static final Map<String, ProviderMetadata> appProviders = Maps.uniqueIndex(Providers
        .viewableAs(BlobStoreContext.class), Providers.idFunction());

    public static final Set<String> allKeys = ImmutableSet.copyOf(Iterables.concat(appProviders.keySet(),
        allApis.keySet()));

    /** Maximum number of retries to retrieve data from the blob store */
    private static final int MAX_RETRIES = 3;
    /** The jclouds blobstore binding */
    private final BlobStore mBlobStore;
    /** Context for the blobstore binding */
    private final BlobStoreContext mBlobStoreContext;
    /** Provider id */
    private final String mProvider;
    /** Id of the cloud */
    private final String mCloudId;
    /** Holder for the request in chronological order */
    private LinkedBlockingQueue<CloudRequest> mRequests;
    /** Holder for the replies in chronological order */
    private LinkedBlockingQueue<CloudReply> mReplies;
    /** CloudDataManager (Managing integrity, retrieval..) */
    private ICloudDataManager mCloudDataManager;
    /** IDepSkySProtocol that uses this cloud manager */
    private IDepSkySProtocol mDepskys;
    /** Determine whether this manager should terminate or not */
    private boolean mTerminate = false;

    public DepSkySCloudManager(Account account, ICloudDataManager pCloudDataManager,
        IDepSkySProtocol pDepskys) {

        // Getting the properties
        mProvider = account.getType();
        String identity = account.getAccessKey();
        String credential = account.getSecretKey();
        mCloudId = account.getId();
        // Checking if the provider is valid.
        checkArgument(contains(allKeys, mProvider), "provider %s not in supported list: %s", mProvider, allKeys);

        // Creating the context using the given properties
        if(mProvider.equals("filesystem")){
            Properties properties = new Properties();
            properties.setProperty(FilesystemConstants.PROPERTY_BASEDIR, new StringBuilder().append("local").append(File.separator).append("filesystem" + mCloudId).toString());
            mBlobStoreContext =
            ContextBuilder.newBuilder(mProvider).credentials(identity, credential).overrides(properties).buildView(
                BlobStoreContext.class);
        }
        else{
            mBlobStoreContext =
                ContextBuilder.newBuilder(mProvider).credentials(identity, credential).buildView(
                    BlobStoreContext.class);
        }
        
        // Create Container
        mBlobStore = mBlobStoreContext.getBlobStore();
        
        this.mRequests = new LinkedBlockingQueue<CloudRequest>();
        this.mReplies = new LinkedBlockingQueue<CloudReply>();
        this.mCloudDataManager = pCloudDataManager;
        this.mDepskys = pDepskys;
    }

    /**
     * {@inheritDoc}
     */
    public Void call() {
        while (true) {
                if (mTerminate && mReplies.isEmpty() && mRequests.isEmpty()) {
                    return null;
                } else {
                    // Each process method will block automatically if no elements are available.
                    processRequest(); // Process next request in queue
                    processReply(); // Process next reply in queue
                }
        }
    }

    /**
     * Do a request on this cloud provider.
     * @param request
     */
    public synchronized void doRequest(CloudRequest request) {
        if (!mTerminate) {
            mRequests.offer(request);
        }

    }

    /**
     * Add a reply by this cloud provider.
     * @param reply
     */
    public synchronized void addReply(CloudReply reply) {
        if (!mTerminate) {
            mReplies.offer(reply);
        }
    }

    /*
     * Process received requests
     */
    private void processRequest() {
        CloudRequest request = null;
        CloudReply r = null;
        long init = 0;
        try {
            
            Blob blob = null;
            Object response = null;
            request = mRequests.take();
            switch (request.getmOp()) {
            case DEL_CONT:

                // Delete all the files in this container (metadata and data files)
                
                for(String containerName : request.getmNamesToDelete()){
                    mBlobStore.deleteContainer(containerName);
                }
                break;
            case NEW_DATA:
                init = System.currentTimeMillis();
                // Writes new data in the cloud
                
                
                BlobMessage message = new BlobMessage(String.valueOf(request.getmOp()), request.getmW_data());
                message.addArg(request.getmContainerName());
                message.addArg(request.getmDataFileName());
                
                ByteArrayDataOutput out = ByteStreams.newDataOutput();
                message.serialize(out);
                
                if(!mBlobStore.containerExists(request.getmContainerName())){
                    mBlobStore.createContainerInLocation(null, request.getmContainerName());
                }
                
                blob = mBlobStore.blobBuilder(request.getmDataFileName()).payload(out.toByteArray()).build();
                putBlobMeta(blob, request);
                response = mBlobStore.putBlob(request.getmContainerName(), blob);
                
                r = new CloudReply(request.getmSeqNumber(), mCloudId, response, System.currentTimeMillis());
                r.setmOp(request.getmOp());
                r.setmDataUnit(request.getReg());
                r.setmIsMetadataFile(request.ismIsMetadataFile());
                r.setmValue(request.getmW_data());
                r.setmVersionNumber(request.getmVersionNumber());
                r.setmAllDataHash(request.getmAllDataHash());

                r.setmInitReceiveTime(init);
                if (request.getmDataFileName().contains("metadata") && request.ismIsMetadataFile()) {
                    r.setmReceiveTime(System.currentTimeMillis());
                    r.setmStartTime(request.getmStartTime());
                    r.setmContainerName(request.getmContainerName());
                }
                
                r.setmType(NEW_DATA);

                addReply(r);
                break;
            case GET_DATA:
                init = System.currentTimeMillis();
                // download a file from the cloud
                mBlobStore.createContainerInLocation(null, request.getmContainerName());
                
                blob = mBlobStore.getBlob(request.getmContainerName(), request.getmDataFileName());
                String versionNumber = "true";
                String versionHash = "true";
                if(blob != null){
                    r = new CloudReply(request.getmSeqNumber(), mCloudId, blob.getPayload().getRawContent(), System.currentTimeMillis());
                    versionNumber = blob.getMetadata().getUserMetadata().get("versionNumber");
                    versionHash = blob.getMetadata().getUserMetadata().get("versionNumber");
                }
                else{
                    r = new CloudReply(request.getmSeqNumber(), mCloudId, null, System.currentTimeMillis());
                }
                r.setmOp(request.getmOp());
                r.setmProtoOp(request.getmProtoOp());
                r.setmContainerName(request.getmContainerName());
                r.setmDataUnit(request.getReg());
                r.setmIsMetadataFile(request.ismIsMetadataFile());
                r.setmValue(request.getmW_data());
                r.setmVersionNumber(versionNumber);
                r.setmVHash(versionHash);
                r.setmAllDataHash(request.getmAllDataHash());
                r.setmHashMatching(request.getmHashMatching());

                r.setmInitReceiveTime(init);
                r.setmStartTime(request.getmStartTime());
                if (request.ismIsMetadataFile()) {
                    r.setmMetadataReceiveTime(System.currentTimeMillis());
                } else {
                    r.setmMetadataReceiveTime(request.getmMetadataReceiveTime());
                }
                
                r.setmType(GET_DATA);

                addReply(r);
                break;
            case DEL_DATA:

                // delete a file from the cloud
                mBlobStore.removeBlob(request.getmContainerName(), request.getmDataFileName());
                
                r = new CloudReply(request.getmSeqNumber(), mCloudId, true, System.currentTimeMillis());
                r.setmOp(request.getmOp());
                r.setmProtoOp(request.getmProtoOp());
                r.setmContainerName(request.getmContainerName());
                r.setmDataUnit(request.getReg());
                r.setmIsMetadataFile(request.ismIsMetadataFile());
                r.setmHashMatching(request.getmHashMatching());
                
                r.setmType(DEL_DATA);
                
                addReply(r);
                break;
            case LIST:

                // list all the files in the cloud that are in the given container
                PageSet<? extends StorageMetadata> list =mBlobStore.list(request.getmContainerName());
                LinkedList<String> names = new LinkedList<String>();
                for(StorageMetadata meta : list){
                    names.add(meta.getName());
                }
                
                r = new CloudReply(request.getmSeqNumber(), mCloudId, true, System.currentTimeMillis());

                r.setmOp(request.getmOp());
                r.setmProtoOp(request.getmProtoOp());
                r.setmContainerName(request.getmContainerName());
                r.setmDataUnit(request.getReg());
                r.setmIsMetadataFile(request.ismIsMetadataFile());
                r.setmValue(request.getmW_data());
                r.setmVersionNumber(request.getmVersionNumber());
                r.setmAllDataHash(request.getmAllDataHash());
                r.setmListNames(names);
                
                r.setmType(LIST);

                addReply(r);
                break;
            case SET_ACL:
                r = new CloudReply(request.getmSeqNumber(), mCloudId, false, System.currentTimeMillis());
                r.setmOp(request.getmOp());
                r.setmProtoOp(request.getmProtoOp());
                r.setmContainerName(request.getmContainerName());
                r.setmDataUnit(request.getReg());
                r.setmIsMetadataFile(request.ismIsMetadataFile());
                r.setmHashMatching(request.getmHashMatching());

                r.setmType(SET_ACL);

                addReply(r);
            default:
                // System.out.println("Operation does not exist");

                r = new CloudReply(request.getmSeqNumber(), mCloudId, null, System.currentTimeMillis());
                r.setmOp(request.getmOp());
                r.setmProtoOp(request.getmProtoOp());
                r.setmContainerName(request.getmContainerName());
                r.setmDataUnit(request.getReg());
                r.setmIsMetadataFile(request.ismIsMetadataFile());
                break;
            }
            // System.out.println("Request Processed: " + request);
        } catch (Exception ex) {
            // ex.printStackTrace();//testing purposes
            if (request.getmRetries() < MAX_RETRIES) {
                // retry (issue request to cloud again)
                request.incrementRetries();
                doRequest(request);
                // System.out.println("Retrying(#" + request.retries + ") request: " + request + " " +
                // driver.getDriverId());
                return;
            }
            // after MAX_REPLIES return null response

            r = new CloudReply(request.getmSeqNumber(), mCloudId, null, System.currentTimeMillis());
            r.setmOp(request.getmOp());
            r.setmProtoOp(request.getmProtoOp());
            r.setmContainerName(request.getmContainerName());
            r.setmDataUnit(request.getReg());
            r.setmIsMetadataFile(request.ismIsMetadataFile());
            r.setmVersionNumber(request.getmVersionNumber());
            r.setmVHash(request.getmVersionHash());
            r.setmReceiveTime(System.currentTimeMillis());
            r.setmInitReceiveTime(init);
            r.setmExceptionMessage(ex.getMessage());
            r.setmStartTime(request.getmStartTime());
            r.invalidateResponse();
            
        }
    }

    private void putBlobMeta(Blob blob, CloudRequest request) {
        blob.getMetadata().getUserMetadata().put("versionNumber", request.getmVersionNumber());
        blob.getMetadata().getUserMetadata().put("versionHash", request.getmVersionHash());
    }

    /*
     *  process received replies
     */
    private void processReply() {
        try {
            CloudReply reply = mReplies.take();// processing removed reply next
            if (reply == null) {
                // System.out.println("REPLY IS NULL!!");
                return;
            }
            // if error
            if (reply.getmResponse() == null) {
                mDepskys.dataReceived(reply);
                return;
            }
            // response may be processed
            if (reply.getmResponse() != null) {
                // process not null response
                if (reply.getmType() == SET_ACL) {
                    mDepskys.dataReceived(reply);
                } else if (reply.getmType() == GET_DATA && reply.ismIsMetadataFile()) {
                    /* metadata file */
                    mCloudDataManager.processMetadata(reply);
                } else if (reply.getmType() == GET_DATA) {

                    if (reply.getmVHash() == null)
                        mDepskys.dataReceived(reply); /* to read quorum operation (out of the protocols) */
                    else
                        mCloudDataManager.checkDataIntegrity(reply); /* valuedata file */
                } else if (reply.getmType() == GET_CONT_AND_DATA_ID) {
                    // send file request for metadata file ids received
                    String[] ids = (String[])reply.getmResponse();
                    // update container id in local register (cid is a constant value)
                    if (reply.getmDataUnit().getContainerId(reply.getmProviderId()) == null) {
                        reply.getmDataUnit().setContainerId(reply.getmProviderId(), ((String[])reply.getmResponse())[0]);
                    }
                    CloudRequest r = new CloudRequest(GET_DATA, ids[0], mCloudId, reply.getmStartTime());
                    r.setmDataFileName(ids[1]);
                    r.setmContainerName(reply.getmContainerName());
                    r.setReg(reply.getmDataUnit());
                    r.setmSeqNumber(reply.getmSequenceNumber());
                    r.setmIsMetadataFile(true);
                    r.setmHashMatching(reply.getmHashMatching());
                    
                    doRequest(r);
                } else if (reply.getmType() == NEW_DATA && reply.ismIsMetadataFile() && reply.getmValue() != null) {
                    // System.out.println("WRITING METADATA for this reply" + reply);
                    mCloudDataManager.writeNewMetadata(reply);
                } else if (reply.getmType() == NEW_DATA && reply.ismIsMetadataFile() && reply.getmValue() != null) {
                    mDepskys.dataReceived(reply);
                    return;
                } else {
                    mDepskys.dataReceived(reply);
                    return;
                }
            }
            // if after processing response was invalidated
            if (reply.getmResponse() == null) {
                // deliver reply if response was null
                mDepskys.dataReceived(reply);
                return;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public String getmProvider() {
        return mProvider;
    }

    public void terminate() {
        mTerminate = true;
    }

    public void resetRequests() {
        mTerminate = false;
        mReplies.clear();
        mRequests.clear();
    }

    public String getCloudId() {
        return mCloudId;
    }

}
