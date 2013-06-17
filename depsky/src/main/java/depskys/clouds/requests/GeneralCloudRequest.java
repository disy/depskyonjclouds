package depskys.clouds.requests;

import depskys.core.DepSkyDataUnit;

/**
 * Class that represents a request for each cloud
 * 
 * @author bruno quaresma
 * @author tiago oliveira
 * 
 *         Modified by @author Andreas Rain, University of Konstanz
 */
public class GeneralCloudRequest implements ICloudRequest{

    private long mSequenceNumber;
    private int mOp, mProtoOp, mRetries;
    private String mVersionNumber, mVersionHash;
    private DepSkyDataUnit mUnit;
    private byte[] mData, mAllDataHash, mHashMatching;
    private long mStartTime;

    public GeneralCloudRequest(int pOp, DepSkyDataUnit pUnit, long pSequenceNumber, long startTime) {
        this.mOp = pOp;
        this.mSequenceNumber = pSequenceNumber;
        this.mStartTime = startTime;
    }

    public int getOp() {
        return mOp;
    }

    public long getSequenceNumber() {
        return mSequenceNumber;
    }

    public void setSequenceNumber(long mSeqNumber) {
        this.mSequenceNumber = mSeqNumber;
    }

    public int getProtoOp() {
        return mProtoOp;
    }

    public void setProtoOp(int mProtoOp) {
        this.mProtoOp = mProtoOp;
    }

    public int getRetries() {
        return mRetries;
    }

    public String getVersionNumber() {
        return mVersionNumber;
    }

    public void setVersionNumber(String mVersionNumber) {
        this.mVersionNumber = mVersionNumber;
    }

    public String getVersionHash() {
        return mVersionHash;
    }

    public void setVersionHash(String mVersionHash) {
        this.mVersionHash = mVersionHash;
    }

    public DepSkyDataUnit getDataUnit() {
        return mUnit;
    }

    public void setDataUnit(DepSkyDataUnit pUnit) {
        this.mUnit = pUnit;
    }

    public byte[] getData() {
        return mData;
    }

    public void setData(byte[] pData) {
        this.mData = pData;
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
    
    public long getStartTime() {
        return mStartTime;
    }

    public void setStartTime(long mStartTime) {
        this.mStartTime = mStartTime;
    }

    public String toString() {
        return mOp + ":" + mSequenceNumber + ":" + mProtoOp + ":" + mUnit.getContainerName() + ":" + mUnit.getValueDataFileNameLastKnownVersion();
    }

    public void incrementRetries() {
        mRetries++;
    }

    public void resetRetries() {
        mRetries = 0;
    }

}
