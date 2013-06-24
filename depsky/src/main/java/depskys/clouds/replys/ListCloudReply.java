package depskys.clouds.replys;

import java.util.List;

import depskys.core.DepSkyDataUnit;

/**
 *  @author Andreas Rain, University of Konstanz
 */
public class ListCloudReply implements ICloudReply{

    private final long mSequenceNumber;
    private int mOp, mProtoOp;
    private String mCloudId;
    private final DepSkyDataUnit mDataUnit;
    private List<String> mNames;
    private long mStartTime;

    /**
     * 
     * @param pOp
     * @param mSequenceNumber
     * @param pProviderId
     * @param pUnit
     * @param pResponse
     * @param pStartTime
     */
    public ListCloudReply(int pOp, long mSequenceNumber, String pProviderId, DepSkyDataUnit pUnit, List<String> pNames, long pStartTime) {
        super();
        this.mOp = pOp;
        this.mDataUnit = pUnit;
        this.mSequenceNumber = mSequenceNumber;
        this.mCloudId = pProviderId;
        this.mStartTime = pStartTime;
        this.mNames = pNames;
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

    public DepSkyDataUnit getDataUnit() {
        return mDataUnit;
    }

    public byte[] getResponse() {
        return null;
    }
    
    public List<String> getmNames() {
        return mNames;
    }

    public void setmNames(List<String> mNames) {
        this.mNames = mNames;
    }

    public long getStartTime() {
        return mStartTime;
    }

    public void setStartTime(long mStartTime) {
        this.mStartTime = mStartTime;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof DataCloudReply) || mCloudId == null) {
            return false;
        }
        ListCloudReply r = (ListCloudReply)o;
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
            + (mDataUnit != null ? mDataUnit.getContainerName() : "null") + "#op:" + mProtoOp;
    }
}
