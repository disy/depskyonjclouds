package depskys.clouds.replys;

import depskys.core.DepSkyDataUnit;

/**
 *  @author Andreas Rain, University of Konstanz
 */
public class DataCloudReply implements ICloudReply{

    private final long mSequenceNumber;
    private int mOp, mProtoOp;
    private String mCloudId, mVersionNumber, mVersionHash;
    private final DepSkyDataUnit mDataUnit;
    private final byte[] mResponse;
    private byte[] mAllDataHash, mHashMatching;
    private long mReceiveTime, mInitReceiveTime, mStartTime;

    /**
     * 
     * @param pOp
     * @param mSequenceNumber
     * @param pProviderId
     * @param pUnit
     * @param pResponse
     * @param pStartTime
     */
    public DataCloudReply(int pOp, long mSequenceNumber, String pProviderId, DepSkyDataUnit pUnit, byte[] pResponse, long pStartTime) {
        super();
        this.mOp = pOp;
        this.mDataUnit = pUnit;
        this.mSequenceNumber = mSequenceNumber;
        this.mCloudId = pProviderId;
        this.mResponse = pResponse;
        this.mStartTime = pStartTime;
    }

    public void setOp(int pOp){
        mOp = pOp;
    }
    
    public int getOp(){
        return mOp;
    }
    
    public long getSequenceNumber() {
        return mSequenceNumber;
    }

    public int getProtoOp() {
        return mProtoOp;
    }

    public void setProtoOp(int mProtoOp) {
        this.mProtoOp = mProtoOp;
    }

    public String getProviderId() {
        return mCloudId;
    }

    public void setProviderId(String mProviderId) {
        this.mCloudId = mProviderId;
    }

    public String getVersionNumber() {
        return mVersionNumber;
    }

    public void setVersionNumber(String mVersionNumber) {
        this.mVersionNumber = mVersionNumber;
    }

    public String getVHash() {
        return mVersionHash;
    }

    public void setVHash(String mVHash) {
        this.mVersionHash = mVHash;
    }

    public DepSkyDataUnit getDataUnit() {
        return mDataUnit;
    }

    public byte[] getResponse() {
        return mResponse;
    }

    public byte[] getAllDataHash() {
        return mAllDataHash;
    }

    public void setAllDataHash(byte[] mAllDataHash) {
        this.mAllDataHash = mAllDataHash;
    }

    public byte[] getHashMatching() {
        return mHashMatching;
    }

    public void setHashMatching(byte[] mHashMatching) {
        this.mHashMatching = mHashMatching;
    }

    public long getReceiveTime() {
        return mReceiveTime;
    }

    public void setReceiveTime(long mReceiveTime) {
        this.mReceiveTime = mReceiveTime;
    }

    public long getInitReceiveTime() {
        return mInitReceiveTime;
    }

    public void setInitReceiveTime(long mInitReceiveTime) {
        this.mInitReceiveTime = mInitReceiveTime;
    }

    public long getStartTime() {
        return mStartTime;
    }

    public void setStartTime(long mStartTime) {
        this.mStartTime = mStartTime;
    }

    public void invalidateResponse() {
        this.mVersionNumber = null;
        this.mVersionHash = null;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof DataCloudReply) || mCloudId == null) {
            return false;
        }
        DataCloudReply r = (DataCloudReply)o;
        return mSequenceNumber == r.mSequenceNumber && mCloudId.equals(r.mCloudId);
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = (int) (29 * hash + this.mSequenceNumber);
        hash = 29 * hash + (this.mCloudId != null ? this.mCloudId.hashCode() : 0);
        return hash;
    }

    /**
     * {@inheritDoc}
     */
    public String toString() {
        return "sn:" + mSequenceNumber + "#cloud:" + mCloudId + "#regId:"
            + (mDataUnit != null ? mDataUnit.getContainerName() : "null") + "#op:" + mProtoOp + "#vn:" + mVersionNumber;
    }
}
