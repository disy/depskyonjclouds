package depskys.clouds;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.contains;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
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
import depskys.clouds.replys.MetaCloudReply;
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
    private IDepSkyClient mDepSkyClient;
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
        this.mDepSkyClient = pDepskys;
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

                BlobMessage message = new BlobMessage(request.getData());
                message.setArgs(putBlobMeta(request));
                message.setDataHash(((GeneralCloudRequest) request).getAllDataHash());

                // Serializing data for blob
                ByteArrayDataOutput output = ByteStreams.newDataOutput();
                message.serialize(output);
                blob =
                    mBlobStore.blobBuilder(request.getDataUnit().getValueDataFileNameLastKnownVersion())
                        .payload(output.toByteArray()).calculateMD5().build();

                response = mBlobStore.putBlob(request.getDataUnit().getContainerName(), blob).getBytes();
                blob.getPayload().release();

                r =
                    new DataCloudReply(request.getOp(), request.getSequenceNumber(), mCloudId, request
                        .getDataUnit(), response, System.currentTimeMillis());
                
                ((DataCloudReply)r).setInitReceiveTime(init);

                addReply(r);
                break;
            case GET_META:
                init = System.currentTimeMillis();
                // download a file from the cloud
                if (mBlobStore.containerExists(request.getDataUnit().getContainerName())) {
                    blob =
                        mBlobStore.getBlob(request.getDataUnit().getContainerName(), request.getDataUnit()
                            .getValueDataFileNameLastKnownVersion());

                    r =
                        new MetaCloudReply(request.getOp(), request.getSequenceNumber(), mCloudId, request
                            .getDataUnit(), request.getStartTime());

                    if (blob != null) {

                        message = BlobMessage.deserializeOnlyMeta(blob.getPayload().getInput());
                        blob.getPayload().release();
                        if (message.getArg("lastVersionNumber") != null) {
                            r.getDataUnit().setLastVersionNumber(
                                Long.valueOf(message.getArg("lastVersionNumber")));
                        }
                        ((MetaCloudReply)r).setAllDataHash(message.getDataHash());
                    }

                    ((MetaCloudReply)r).setInitReceiveTime(init);

                    addReply(r);
                } else {
                    r =
                        new MetaCloudReply(request.getOp(), request.getSequenceNumber(), mCloudId, request
                            .getDataUnit(), request.getStartTime());
                    
                    addReply(r);
                }
                break;
            case GET_DATA:
                init = System.currentTimeMillis();
                // download a file from the cloud
                if (!mBlobStore.containerExists(request.getDataUnit().getContainerName())) {
                    return;
                }

                blob =
                    mBlobStore.getBlob(request.getDataUnit().getContainerName(), request.getDataUnit()
                        .getValueDataFileNameLastKnownVersion());

                if (blob != null) {

                    message = BlobMessage.deserialize(blob.getPayload().getInput());
                    response = message.getPayload();
                    blob.getPayload().release();
                    r =
                        new DataCloudReply(request.getOp(), request.getSequenceNumber(), mCloudId, request
                            .getDataUnit(), response, System.currentTimeMillis());
                    if (message.getArg("lastVersionNumber") != null) {
                        r.getDataUnit().setLastVersionNumber(
                            Long.valueOf(message.getArg("lastVersionNumber")));
                    }
                    ((DataCloudReply)r).setAllDataHash(message.getDataHash());
                } else {
                    r =
                        new DataCloudReply(request.getOp(), request.getSequenceNumber(), mCloudId, request
                            .getDataUnit(), response, System.currentTimeMillis());
                }

                ((DataCloudReply)r).setInitReceiveTime(init);
                ((DataCloudReply)r).setStartTime(request.getStartTime());

                addReply(r);
                break;
            case DEL_DATA:
                // delete a file from the cloud
                mBlobStore.removeBlob(request.getDataUnit().getContainerName(), request.getDataUnit()
                    .getValueDataFileNameLastKnownVersion());
                break;
            case LIST:

                // list all the files in the cloud that are in the given container
                PageSet<? extends StorageMetadata> list =
                    mBlobStore.list(request.getDataUnit().getContainerName());
                List<String> names = new LinkedList<String>();
                for (StorageMetadata meta : list) {
                    names.add(meta.getName());
                }

                r =
                    new ListCloudReply(request.getOp(), request.getSequenceNumber(), mCloudId, request
                        .getDataUnit(), names, System.currentTimeMillis());

                addReply(r);
                break;
            default:
                break;
            }
        } catch (Exception ex) {
            if (request.getRetries() < MAX_RETRIES) {
                // retry (issue request to cloud again)
                request.incrementRetries();
                doRequest(request);
                return;
            }
        }
    }

    private Map<String, String> putBlobMeta(ICloudRequest request) throws UnsupportedEncodingException {
        Map<String, String> userMeta = new HashMap<String, String>();
        userMeta.put("dataSize", String.valueOf(request.getData().length));
        userMeta.put("lastKnownVersion", request.getDataUnit().getValueDataFileNameLastKnownVersion());
        userMeta.put("lastVersionNumber", String.valueOf(request.getDataUnit().getLastVersionNumber()));

        return userMeta;
    }

    /*
     * process received replies
     */
    private void processReply() {
        try {
            ICloudReply reply = mReplies.take();// processing removed reply next

            // If datacloud reply has no data hash, the reply comes from a new data request
            if (reply instanceof DataCloudReply && ((DataCloudReply)reply).getAllDataHash() != null) {
                mCloudDataManager.checkDataIntegrity((DataCloudReply)reply);
            }

            mDepSkyClient.dataReceived(reply);
            return;
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
