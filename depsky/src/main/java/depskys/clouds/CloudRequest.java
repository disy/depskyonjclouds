package depskys.clouds;

import java.util.Properties;

import depskys.core.DepSkySDataUnit;

/**
 * Class that represents a request for each cloud
 * 
 * @author bruno quaresma
 * @author tiago oliveira
 * 
 *         Modified by @author Andreas Rain, University of Konstanz
 */
public class CloudRequest {

    private int mOp, mSeqNumber, mProtoOp, mRetries;
    private String mContainerName, mDataFileName, mVersionNumber, mVersionHash, mPermission,
    mCanonicalId;
    private String[] mNamesToDelete;
    private DepSkySDataUnit reg;
    private byte[] mW_data, mAllDataHash, mHashMatching;
    private Properties mProps;
    private boolean mIsMetadataFile;
    private long mStartTime, mMetadataReceiveTime;

    public CloudRequest(int op, String cid, String did, long startTime) {
        super();
        this.mOp = op;
        this.mContainerName = cid;
        this.mDataFileName = did;
        this.mStartTime = startTime;
    }

    public int getmOp() {
        return mOp;
    }

    public int getmSeqNumber() {
        return mSeqNumber;
    }

    public void setmSeqNumber(int mSeqNumber) {
        this.mSeqNumber = mSeqNumber;
    }

    public int getmProtoOp() {
        return mProtoOp;
    }

    public void setmProtoOp(int mProtoOp) {
        this.mProtoOp = mProtoOp;
    }

    public int getmRetries() {
        return mRetries;
    }

    public void setmRetries(int mRetries) {
        this.mRetries = mRetries;
    }

    public String getmContainerName() {
        return mContainerName;
    }

    public void setmContainerName(String mContainerName) {
        this.mContainerName = mContainerName;
    }

    public String getmDataFileName() {
        return mDataFileName;
    }

    public void setmDataFileName(String mDataFileName) {
        this.mDataFileName = mDataFileName;
    }

    public String getmVersionNumber() {
        return mVersionNumber;
    }

    public void setmVersionNumber(String mVersionNumber) {
        this.mVersionNumber = mVersionNumber;
    }

    public String getmVersionHash() {
        return mVersionHash;
    }

    public void setmVersionHash(String mVersionHash) {
        this.mVersionHash = mVersionHash;
    }

    public String getmPermission() {
        return mPermission;
    }

    public void setmPermission(String mPermission) {
        this.mPermission = mPermission;
    }

    public String getmCanonicalId() {
        return mCanonicalId;
    }

    public void setmCanonicalId(String mCanonicalId) {
        this.mCanonicalId = mCanonicalId;
    }

    public String[] getmNamesToDelete() {
        return mNamesToDelete;
    }

    public void setmNamesToDelete(String[] mNamesToDelete) {
        this.mNamesToDelete = mNamesToDelete;
    }

    public DepSkySDataUnit getReg() {
        return reg;
    }

    public void setReg(DepSkySDataUnit reg) {
        this.reg = reg;
    }

    public byte[] getmW_data() {
        return mW_data;
    }

    public void setmW_data(byte[] mW_data) {
        this.mW_data = mW_data;
    }

    public byte[] getmAllDataHash() {
        return mAllDataHash;
    }

    public void setmAllDataHash(byte[] mAllDataHash) {
        this.mAllDataHash = mAllDataHash;
    }

    public byte[] getmHashMatching() {
        return mHashMatching;
    }

    public void setmHashMatching(byte[] mHashMatching) {
        this.mHashMatching = mHashMatching;
    }

    public Properties getmProps() {
        return mProps;
    }

    public void setmProps(Properties mProps) {
        this.mProps = mProps;
    }

    public boolean ismIsMetadataFile() {
        return mIsMetadataFile;
    }

    public void setmIsMetadataFile(boolean mIsMetadataFile) {
        this.mIsMetadataFile = mIsMetadataFile;
    }

    public long getmStartTime() {
        return mStartTime;
    }

    public void setmStartTime(long mStartTime) {
        this.mStartTime = mStartTime;
    }

    public long getmMetadataReceiveTime() {
        return mMetadataReceiveTime;
    }

    public void setmMetadataReceiveTime(long mMetadataReceiveTime) {
        this.mMetadataReceiveTime = mMetadataReceiveTime;
    }

    public String toString() {
        return mOp + ":" + mSeqNumber + ":" + mProtoOp + ":" + mContainerName + ":" + mDataFileName;
    }

    public void incrementRetries() {
        mRetries++;
    }

    public void resetRetries() {
        mRetries = 0;
    }

}
