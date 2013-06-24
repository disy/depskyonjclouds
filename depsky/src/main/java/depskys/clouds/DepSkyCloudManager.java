package depskys.clouds;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.contains;

import java.io.File;
import java.util.LinkedList;
import java.util.List;
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

import depskys.clouds.replys.DataCloudReply;
import depskys.clouds.replys.ICloudReply;
import depskys.clouds.replys.ListCloudReply;
import depskys.clouds.requests.DeleteCloudRequest;
import depskys.clouds.requests.GeneralCloudRequest;
import depskys.clouds.requests.ICloudRequest;
import depskys.core.IDepSkyClient;
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
public class DepSkyCloudManager implements Callable<Void> {

    public static final int INIT_SESS = 0;
    public static final int NEW_CONT = 1;
    public static final int DEL_CONT = 2;
    public static final int NEW_DATA = 3;
    public static final int GET_DATA = 4;
    public static final int DEL_DATA = 5;
    public static final int GET_META = 6;
    public static final int LIST = 7;

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
    private LinkedBlockingQueue<ICloudRequest> mRequests;
    /** Holder for the replies in chronological order */
    private LinkedBlockingQueue<ICloudReply> mReplies;
    /** CloudDataManager (Managing integrity, retrieval..) */
    private ICloudDataManager mCloudDataManager;
    /** IDepSkySProtocol that uses this cloud manager */
    private IDepSkyClient mDepskys;
    /** Determine whether this manager should terminate or not */
    private boolean mTerminate = false;

    public DepSkyCloudManager(Account account, ICloudDataManager pCloudDataManager, IDepSkyClient pDepskys) {

        // Getting the properties
        mProvider = account.getType();
        String identity = account.getAccessKey();
        String credential = account.getSecretKey();
        mCloudId = account.getId();
        // Checking if the provider is valid.
        checkArgument(contains(allKeys, mProvider), "provider %s not in supported list: %s", mProvider,
            allKeys);

        // Creating the context using the given properties
        if (mProvider.equals("filesystem")) {
            Properties properties = new Properties();
            properties.setProperty(FilesystemConstants.PROPERTY_BASEDIR, new StringBuilder().append("local")
                .append(File.separator).append("filesystem" + mCloudId).toString());
            mBlobStoreContext =
                ContextBuilder.newBuilder(mProvider).credentials(identity, credential).overrides(properties)
                    .buildView(BlobStoreContext.class);
        } else {
            mBlobStoreContext =
                ContextBuilder.newBuilder(mProvider).credentials(identity, credential).buildView(
                    BlobStoreContext.class);
        }

        // Create Container
        mBlobStore = mBlobStoreContext.getBlobStore();

        this.mRequests = new LinkedBlockingQueue<ICloudRequest>();
        this.mReplies = new LinkedBlockingQueue<ICloudReply>();
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
     * 
     * @param request
     */
    public synchronized void doRequest(ICloudRequest request) {
        if (!mTerminate) {
            mRequests.offer(request);
        }

    }

    /**
     * Add a reply by this cloud provider.
     * 
     * @param reply
     */
    public synchronized void addReply(ICloudReply reply) {
        if (!mTerminate) {
            mReplies.offer(reply);
        }
    }

    /*
     * Process received requests
     */
    private void processRequest() {
        ICloudRequest request = null;
        ICloudReply r = null;
        long init = 0;
        try {

            Blob blob = null;
            request = mRequests.take();
            byte[] response = null;
            switch (request.getOp()) {
            case DEL_CONT:

                // Delete all the files in this container (metadata and data files)

                for (String deleteBlob : ((DeleteCloudRequest)request).getNamesToDelete()) {
                    mBlobStore.removeBlob(request.getDataUnit().getContainerName(), deleteBlob);
                }
                break;
            case NEW_DATA:
                init = System.currentTimeMillis();
                // Writes new data in the cloud
                if (!mBlobStore.containerExists(request.getDataUnit().getContainerName())) {
                    mBlobStore.createContainerInLocation(null, request.getDataUnit().getContainerName());
                }

                blob =
                    mBlobStore.blobBuilder(request.getDataUnit().getValueDataFileNameLastKnownVersion())
                        .payload(request.getData()).build();
                putBlobMeta(blob, request);
                response = mBlobStore.putBlob(request.getDataUnit().getContainerName(), blob).getBytes();

                r = 
                    new DataCloudReply(request.getOp(), request.getSequenceNumber(), mCloudId, request.getDataUnit(), response, System.currentTimeMillis());

                ((DataCloudReply) r).setInitReceiveTime(init);

                addReply(r);
                break;
            case GET_DATA:
                init = System.currentTimeMillis();
                // download a file from the cloud
                if (!mBlobStore.containerExists(request.getDataUnit().getContainerName())) {
                    return;
                }
                
                blob = mBlobStore.getBlob(request.getDataUnit().getContainerName(), request.getDataUnit().getValueDataFileNameLastKnownVersion());
                response = (byte[])blob.getPayload().getRawContent();
                r =
                    new DataCloudReply(request.getOp(), request.getSequenceNumber(), mCloudId, request.getDataUnit(), response, System.currentTimeMillis());
                if(blob != null){
                    ((DataCloudReply) r).setVersionNumber(blob.getMetadata().getUserMetadata().get("versionNumber"));
                    ((DataCloudReply) r).setVHash(blob.getMetadata().getUserMetadata().get("versionHash"));
                    ((DataCloudReply) r).setAllDataHash(blob.getMetadata().getUserMetadata().get("allDataHash").getBytes());
                }
                

                ((DataCloudReply)r).setInitReceiveTime(init);
                ((DataCloudReply)r).setStartTime(request.getStartTime());

                addReply(r);
                break;
            case DEL_DATA:
                // delete a file from the cloud
                mBlobStore.removeBlob(request.getDataUnit().getContainerName(), request.getDataUnit().getValueDataFileNameLastKnownVersion());
                break;
            case LIST:

                // list all the files in the cloud that are in the given container
                PageSet<? extends StorageMetadata> list = mBlobStore.list(request.getDataUnit().getContainerName());
                List<String> names = new LinkedList<String>();
                for (StorageMetadata meta : list) {
                    names.add(meta.getName());
                }

                r = new ListCloudReply(request.getOp(), request.getSequenceNumber(), mCloudId, request.getDataUnit(), names, System.currentTimeMillis());

                addReply(r);
                break;
            default:
                break;
            }
            // System.out.println("Request Processed: " + request);
        } catch (Exception ex) {
            // ex.printStackTrace();//testing purposes
            if (request.getRetries() < MAX_RETRIES) {
                // retry (issue request to cloud again)
                request.incrementRetries();
                doRequest(request);
                return;
            }
        }
    }

    private void putBlobMeta(Blob blob, ICloudRequest request) {
        blob.getMetadata().getUserMetadata().put("lastKnownVersion", request.getDataUnit().getValueDataFileNameLastKnownVersion());
        blob.getMetadata().getUserMetadata().put("lastVersionNumber", String.valueOf(request.getDataUnit().getLastVersionNumber()));
        
        if (request instanceof GeneralCloudRequest) {
            blob.getMetadata().getUserMetadata().put("allDataHash", String.valueOf(((GeneralCloudRequest)request).getAllDataHash()));
            blob.getMetadata().getUserMetadata().put("versionNumber", ((GeneralCloudRequest)request).getVersionNumber());
            blob.getMetadata().getUserMetadata().put("versionHash", ((GeneralCloudRequest)request).getVersionHash());
        }
    }

    /*
     * process received replies
     */
    private void processReply() {
        try {
            ICloudReply reply = mReplies.take();// processing removed reply next
            if (reply == null) {
                // System.out.println("REPLY IS NULL!!");
                return;
            }
            // if error
            if (reply.getResponse() == null) {
                mDepskys.dataReceived(reply);
                return;
            }
            // response may be processed
            if (reply.getResponse() != null) {
                // process not null response
                if (reply.getType() == SET_ACL) {
                    mDepskys.dataReceived(reply);
                } else if (reply.getType() == GET_DATA && reply.ismIsMetadataFile()) {
                    /* metadata file */
                    mCloudDataManager.processMetadata(reply);
                } else if (reply.getType() == GET_DATA) {

                    if (reply.getVHash() == null)
                        mDepskys.dataReceived(reply); /* to read quorum operation (out of the protocols) */
                    else
                        mCloudDataManager.checkDataIntegrity(reply); /* valuedata file */
                } else if (reply.getType() == GET_CONT_AND_DATA_ID) {
                    // send file request for metadata file ids received
                    String[] ids = (String[])reply.getResponse();
                    // update container id in local register (cid is a constant value)
                    if (reply.getDataUnit().getContainerId(reply.getProviderId()) == null) {
                        reply.getDataUnit().setContainerId(reply.getProviderId(),
                            ((String[])reply.getResponse())[0]);
                    }
                    GeneralCloudRequest r =
                        new GeneralCloudRequest(GET_DATA, ids[0], mCloudId, reply.getStartTime());
                    r.setmDataFileName(ids[1]);
                    r.setmContainerName(reply.getContainerName());
                    r.setReg(reply.getDataUnit());
                    r.setmSeqNumber(reply.getSequenceNumber());
                    r.setmIsMetadataFile(true);
                    r.setmHashMatching(reply.getHashMatching());

                    doRequest(r);
                } else if (reply.getType() == NEW_DATA && reply.ismIsMetadataFile()
                    && reply.getValue() != null) {
                    // System.out.println("WRITING METADATA for this reply" + reply);
                    mCloudDataManager.writeNewMetadata(reply);
                } else if (reply.getType() == NEW_DATA && reply.ismIsMetadataFile()
                    && reply.getValue() != null) {
                    mDepskys.dataReceived(reply);
                    return;
                } else {
                    mDepskys.dataReceived(reply);
                    return;
                }
            }
            // if after processing response was invalidated
            if (reply.getResponse() == null) {
                // deliver reply if response was null
                mDepskys.dataReceived(reply);
                return;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public String getProvider() {
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
