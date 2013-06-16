package depskys.clouds;

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Semaphore;

/**
 *
 * @author koras
 */
public class CloudRepliesControlSet {

    /**
     * This semaphore has no permits, and its purpose is to lock a thread waiting for relies.
     * After, when n - f replies are received, the locked thread is unlocked.
     */
    private Semaphore waitReplies = new Semaphore(0);
    private CopyOnWriteArrayList<CloudReply> replies;
    private int sequence;

    public CloudRepliesControlSet(int n, int sequence) {
        this.sequence = sequence;
        this.replies = new CopyOnWriteArrayList<CloudReply>();
    }

    public Semaphore getWaitReplies() {
        return waitReplies;
    }

    public void setWaitReplies(Semaphore waitReplies) {
        this.waitReplies = waitReplies;
    }

    public CopyOnWriteArrayList<CloudReply> getReplies() {
        return replies;
    }

    public void setReplies(CopyOnWriteArrayList<CloudReply> replies) {
        this.replies = replies;
    }

    public int getSequence() {
        return sequence;
    }
}
