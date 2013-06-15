package depskys.clouds;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.contains;

import message.Message;

import org.jclouds.ContextBuilder;
import org.jclouds.apis.ApiMetadata;
import org.jclouds.apis.Apis;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.PageSet;
import org.jclouds.blobstore.domain.StorageMetadata;
import org.jclouds.providers.ProviderMetadata;
import org.jclouds.providers.Providers;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;

import depskys.core.IDepSkySProtocol;

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
    /** Properties for this CloudManager */
    private final Properties mProperties;
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

    public DepSkySCloudManager(Properties pProperties, ICloudDataManager pCloudDataManager,
        IDepSkySProtocol pDepskys) {
        mProperties = pProperties;

        // Getting the properties
        String provider = mProperties.getProperty("jclouds.provider");
        String identity = mProperties.getProperty("jclouds.identity");
        String credential = mProperties.getProperty("jclouds.credential");
        // Checking if the provider is valid.
        checkArgument(contains(allKeys, provider), "provider %s not in supported list: %s", provider, allKeys);

        // Creating the context using the given properties
        mBlobStoreContext =
            ContextBuilder.newBuilder(provider).credentials(identity, credential).buildView(
                BlobStoreContext.class);
        
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
                    processReply(); // Process next reply in queue
                    processRequest(); // Process next request in queue
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
                
                blob = mBlobStore.blobBuilder(request.getmDataFileName()).payload(out.toByteArray()).build();
                response = mBlobStore.putBlob(request.getmContainerName(), blob);
                
                r = new CloudReply(request.getmSeqNumber(), mProperties.getProperty("jclouds.provider"), response, System.currentTimeMillis());
                r.setmOp(request.getmOp());
                r.setmContainerName(request.getmContainerName());
                r.setmDataUnit(request.getReg());
                r.setmIsMetadataFile(request.ismIsMetadataFile());
                r.setmValue(request.getmW_data());
                r.setmVersionNumber(request.getmVersionNumber());
                r.setmAllDataHash(request.getmAllDataHash());

                r.setmInitReceiveTime(init);
                if (request.getmDataFileName().contains("metadata") && request.ismIsMetadataFile()) {
                    r.setmReceiveTime(System.currentTimeMillis());
                    r.setmStartTime(request.getmStartTime());
                }

                addReply(r);
                break;
            case GET_DATA:
                init = System.currentTimeMillis();
                // download a file from the cloud
                blob = mBlobStore.getBlob(request.getmContainerName(), request.getmDataFileName());

                r = new CloudReply(request.getmSeqNumber(), mProperties.getProperty("jclouds.provider"), blob.getPayload().getRawContent(), System.currentTimeMillis());
                r.setmOp(request.getmOp());
                r.setmProtoOp(request.getmProtoOp());
                r.setmContainerName(request.getmContainerName());
                r.setmDataUnit(request.getReg());
                r.setmIsMetadataFile(request.ismIsMetadataFile());
                r.setmValue(request.getmW_data());
                r.setmVersionNumber(request.getmVersionNumber());
                r.setmVHash(request.getmVersionHash());
                r.setmAllDataHash(request.getmAllDataHash());
                r.setmHashMatching(request.getmHashMatching());
                
                r.setmInitReceiveTime(init);
                r.setmStartTime(request.getmStartTime());
                if (request.ismIsMetadataFile()) {
                    r.setmMetadataReceiveTime(System.currentTimeMillis());
                } else {
                    r.setmMetadataReceiveTime(request.getmMetadataReceiveTime());
                }

                addReply(r);
                break;
            case DEL_DATA:

                // delete a file from the cloud
                mBlobStore.removeBlob(request.getmContainerName(), request.getmDataFileName());
                
                r = new CloudReply(request.getmSeqNumber(), mProperties.getProperty("jclouds.provider"), true, System.currentTimeMillis());
                r.setmOp(request.getmOp());
                r.setmProtoOp(request.getmProtoOp());
                r.setmContainerName(request.getmContainerName());
                r.setmDataUnit(request.getReg());
                r.setmIsMetadataFile(request.ismIsMetadataFile());
                r.setmHashMatching(request.getmHashMatching());
                break;
            case LIST:

                // list all the files in the cloud that are in the given container
                PageSet<? extends StorageMetadata> list =mBlobStore.list(request.getmContainerName());
                LinkedList<String> names = new LinkedList<String>();
                for(StorageMetadata meta : list){
                    names.add(meta.getName());
                }
                
                r = new CloudReply(request.getmSeqNumber(), mProperties.getProperty("jclouds.provider"), true, System.currentTimeMillis());

                r.setmOp(request.getmOp());
                r.setmProtoOp(request.getmProtoOp());
                r.setmContainerName(request.getmContainerName());
                r.setmDataUnit(request.getReg());
                r.setmIsMetadataFile(request.ismIsMetadataFile());
                r.setmValue(request.getmW_data());
                r.setmVersionNumber(request.getmVersionNumber());
                r.setmAllDataHash(request.getmAllDataHash());
                r.setmListNames(names);

                addReply(r);
                break;
            case SET_ACL:
                r = new CloudReply(request.getmSeqNumber(), mProperties.getProperty("jclouds.provider"), false, System.currentTimeMillis());
                r.setmOp(request.getmOp());
                r.setmProtoOp(request.getmProtoOp());
                r.setmContainerName(request.getmContainerName());
                r.setmDataUnit(request.getReg());
                r.setmIsMetadataFile(request.ismIsMetadataFile());
                r.setmHashMatching(request.getmHashMatching());
                addReply(r);
            default:
                // System.out.println("Operation does not exist");

                r = new CloudReply(request.getmSeqNumber(), mProperties.getProperty("jclouds.provider"), null, System.currentTimeMillis());
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

            r = new CloudReply(request.getmSeqNumber(), mProperties.getProperty("jclouds.provider"), null, System.currentTimeMillis());
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
            addReply(r);
        }
    }

    // process received replies
    private void processReply() {
        try {
            CloudReply reply = mReplies.remove(0);// processing removed reply next
            if (reply == null) {
                // System.out.println("REPLY IS NULL!!");
                return;
            }
            // if error
            if (reply.mResponse == null) {
                mDepskys.dataReceived(reply);
                return;
            }
            // response may be processed
            if (reply.mResponse != null) {
                // process not null response
                if (reply.mType == SET_ACL) {
                    mDepskys.dataReceived(reply);
                } else if (reply.mType == GET_DATA && reply.mIsMetadataFile) {
                    /* metadata file */
                    mCloudDataManager.processMetadata(reply);
                } else if (reply.mType == GET_DATA) {

                    if (reply.mVHash == null)
                        mDepskys.dataReceived(reply); /* to read quorum operation (out of the protocols) */
                    else
                        mCloudDataManager.checkDataIntegrity(reply); /* valuedata file */
                } else if (reply.mType == GET_CONT_AND_DATA_ID) {
                    // send file request for metadata file ids received
                    String[] ids = (String[])reply.mResponse;
                    // update container id in local register (cid is a constant value)
                    if (reply.mDataUnit.getContainerId(reply.mProviderId) == null) {
                        reply.mDataUnit.setContainerId(reply.mProviderId, ((String[])reply.mResponse)[0]);
                    }
                    CloudRequest r =
                        new CloudRequest(GET_DATA, reply.mSequenceNumber, mBlobStore.getSessionKey(), ids[0],
                            ids[1], null, null, reply.mDataUnit, reply.mProtoOp, true, reply.mHashMatching);
                    r.setStartTime(reply.mStartTime);
                    doRequest(r);
                } else if (reply.mType == NEW_DATA && !reply.mIsMetadataFile && reply.mValue != null) {
                    // System.out.println("WRITING METADATA for this reply" + reply);
                    mCloudDataManager.writeNewMetadata(reply);
                } else if (reply.mType == NEW_DATA && reply.mIsMetadataFile && reply.mValue != null) {
                    mDepskys.dataReceived(reply);
                    return;
                } else {
                    mDepskys.dataReceived(reply);
                    return;
                }
            }
            // if after processing response was invalidated
            if (reply.mResponse == null) {
                // deliver reply if response was null
                mDepskys.dataReceived(reply);
                return;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void terminate() {
        mTerminate = true;
    }

    public void resetRequests() {
        mTerminate = false;
        mReplies.clear();
        mRequests.clear();
    }

}
