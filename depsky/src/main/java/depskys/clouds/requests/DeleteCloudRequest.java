package depskys.clouds.requests;

import depskys.core.DepSkyDataUnit;

/**
 * 
 * @author Andreas Rain, University of Konstanz
 * 
 */
public class DeleteCloudRequest implements ICloudRequest {

    private long mSequenceNumber;
    private int mOp, mProtoOp, mRetries;
    private String mVersionNumber, mVersionHash;
    private DepSkyDataUnit mUnit;
    private String[] mNamesToDelete;
    private long mStartTime;

    public DeleteCloudRequest(int pOp, DepSkyDataUnit pUnit, long pSequenceNumber, long startTime) {
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

    public String[] getNamesToDelete() {
        return mNamesToDelete;
    }

    public void setNamesToDelete(String[] mNamesToDelete) {
        this.mNamesToDelete = mNamesToDelete;
    }

    public long getStartTime() {
        return mStartTime;
    }

    public void setStartTime(long mStartTime) {
        this.mStartTime = mStartTime;
    }

    public String toString() {
        return mOp + ":" + mSequenceNumber + ":" + mProtoOp + ":" + mUnit.getContainerName() + ":"
            + mUnit.getValueDataFileNameLastKnownVersion();
    }

    public void incrementRetries() {
        mRetries++;
    }

    public void resetRetries() {
        mRetries = 0;
    }

    @Override
    public byte[] getData() {
        // TODO Auto-generated method stub
        return null;
    }

}
