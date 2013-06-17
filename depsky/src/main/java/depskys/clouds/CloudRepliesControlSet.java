package depskys.clouds;

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Semaphore;

import depskys.clouds.replys.ICloudReply;

/**
 *
 * @author koras
 */
public class CloudRepliesControlSet {

    /**
     * This semaphore has no permits, and its purpose is to lock a thread waiting for relies.
     * After, when n - f replies are received, the locked thread is unlocked.
     */
    private Semaphore mWaitReplies = new Semaphore(0);
    private CopyOnWriteArrayList<ICloudReply> mReplies;
    private long mSequence;

    public CloudRepliesControlSet(int n, long pSequence) {
        this.mSequence = pSequence;
        this.mReplies = new CopyOnWriteArrayList<ICloudReply>();
    }

    public Semaphore getWaitReplies() {
        return mWaitReplies;
    }

    public void setWaitReplies(Semaphore waitReplies) {
        this.mWaitReplies = waitReplies;
    }

    public CopyOnWriteArrayList<ICloudReply> getReplies() {
        return mReplies;
    }

    public void setReplies(CopyOnWriteArrayList<ICloudReply> replies) {
        this.mReplies = replies;
    }

    public long getSequence() {
        return mSequence;
    }
}
