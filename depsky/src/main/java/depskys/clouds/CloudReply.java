package depskys.clouds;

import java.util.LinkedList;

import depskys.core.DepSkySDataUnit;

/**
 * Class that contains the response for each cloud request
 * 
 * @author bruno quaresma
 * @author tiago oliveira
 * 
 *         Modified by @author Andreas Rain, University of Konstanz
 */
public class CloudReply {

    private int mOp, mSequenceNumber, mType, mProtoOp;
    private String mProviderId, mContainerName, mVersionNumber, mVHash, mExceptionMessage, mValueFileId;
    private DepSkySDataUnit mDataUnit;
    private Object mResponse;
    private boolean mIsMetadataFile;
    private byte[] mValue, mAllDataHash, mHashMatching;
    private long mReceiveTime, mInitReceiveTime, mStartTime, mMetadataReceiveTime;
    private LinkedList<String> mListNames;

    public CloudReply(int mSequenceNumber, String mProviderId, Object mResponse, long mStartTime) {
        super();
        this.mSequenceNumber = mSequenceNumber;
        this.mProviderId = mProviderId;
        this.mResponse = mResponse;
        this.mStartTime = mStartTime;
    }

    @Override
    public String toString() {
        return "sn:" + mSequenceNumber + "#cloud:" + mProviderId + "#type:" + mType + "#regId:"
            + (mDataUnit != null ? mDataUnit.regId : "null") + "#op:" + mProtoOp + "#vn:" + mVersionNumber
            + "#mdfile?" + mIsMetadataFile;
    }

    public void setmOp(int pOp){
        mOp = pOp;
    }
    
    public int getmOp(){
        return mOp;
    }
    
    public int getmSequenceNumber() {
        return mSequenceNumber;
    }

    public void setmSequenceNumber(int mSequenceNumber) {
        this.mSequenceNumber = mSequenceNumber;
    }

    public int getmType() {
        return mType;
    }

    public void setmType(int mType) {
        this.mType = mType;
    }

    public int getmProtoOp() {
        return mProtoOp;
    }

    public void setmProtoOp(int mProtoOp) {
        this.mProtoOp = mProtoOp;
    }

    public String getmProviderId() {
        return mProviderId;
    }

    public void setmProviderId(String mProviderId) {
        this.mProviderId = mProviderId;
    }

    public String getmContainerName() {
        return mContainerName;
    }

    public void setmContainerName(String mContainerName) {
        this.mContainerName = mContainerName;
    }

    public String getmVersionNumber() {
        return mVersionNumber;
    }

    public void setmVersionNumber(String mVersionNumber) {
        this.mVersionNumber = mVersionNumber;
    }

    public String getmVHash() {
        return mVHash;
    }

    public void setmVHash(String mVHash) {
        this.mVHash = mVHash;
    }

    public String getmExceptionMessage() {
        return mExceptionMessage;
    }

    public void setmExceptionMessage(String mExceptionMessage) {
        this.mExceptionMessage = mExceptionMessage;
    }

    public String getmValueFileId() {
        return mValueFileId;
    }

    public void setmValueFileId(String mValueFileId) {
        this.mValueFileId = mValueFileId;
    }

    public DepSkySDataUnit getmDataUnit() {
        return mDataUnit;
    }

    public void setmDataUnit(DepSkySDataUnit mDataUnit) {
        this.mDataUnit = mDataUnit;
    }

    public Object getmResponse() {
        return mResponse;
    }

    public void setmResponse(Object mResponse) {
        this.mResponse = mResponse;
    }

    public boolean ismIsMetadataFile() {
        return mIsMetadataFile;
    }

    public void setmIsMetadataFile(boolean mIsMetadataFile) {
        this.mIsMetadataFile = mIsMetadataFile;
    }

    public byte[] getmValue() {
        return mValue;
    }

    public void setmValue(byte[] mValue) {
        this.mValue = mValue;
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

    public long getmReceiveTime() {
        return mReceiveTime;
    }

    public void setmReceiveTime(long mReceiveTime) {
        this.mReceiveTime = mReceiveTime;
    }

    public long getmInitReceiveTime() {
        return mInitReceiveTime;
    }

    public void setmInitReceiveTime(long mInitReceiveTime) {
        this.mInitReceiveTime = mInitReceiveTime;
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

    public LinkedList<String> getmListNames() {
        return mListNames;
    }

    public void setmListNames(LinkedList<String> mListNames) {
        this.mListNames = mListNames;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof CloudReply) || mProviderId == null) {
            return false;
        }
        CloudReply r = (CloudReply)o;
        return mSequenceNumber == r.mSequenceNumber && mProviderId.equals(r.mProviderId);
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 29 * hash + this.mSequenceNumber;
        hash = 29 * hash + (this.mProviderId != null ? this.mProviderId.hashCode() : 0);
        return hash;
    }

    public void invalidateResponse() {
        this.mResponse = null;
        this.mVersionNumber = null;
        this.mVHash = null;
    }
}
