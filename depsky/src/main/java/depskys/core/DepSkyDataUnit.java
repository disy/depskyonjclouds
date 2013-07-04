package depskys.core;


/**
 * Former authors:
 * @author tiago oliveira
 * @author bruno
 * ----------------------
 * 
 * This class holds some metadata (especially regarding the version of the data unit).
 * 
 *         Modified by @author Andreas Rain, University of Konstanz
 */
public class DepSkyDataUnit{
    
    
    /** Container and Unit name for this particular data unit. */
    private final String mContainerName, mUnitName;
    
    /** Version properties */
    private int mCloudVersionCount;
    private long mLastVersionNumber;
    private Long mHighestCloudVersion = null;
    private long mCurrentHighestVersion = -1;
    
    /** Cloud requirements */
    private int n = 4; 
    private int f = 1;

    /**
     * Create a new dep sky data unit.
     * @param pContainerName - Container name from the cloud storage to use
     * @param pUnitName - unit name, in case of jclouds "blob name"
     */
    public DepSkyDataUnit(String pContainerName, String pUnitName) {
        this.mContainerName = pContainerName.toLowerCase();
        this.mUnitName = pUnitName;
        this.mLastVersionNumber = -1;
        this.mCloudVersionCount = 0;
        this.mHighestCloudVersion = -1L;
        this.mCurrentHighestVersion = -1;
    }
    
    /**
     * Set how many clouds are involved and the lower border f.
     * @param n
     * @param f
     */
    public void setCloudRequirement(int n, int f){
        this.n = n;
        this.f = f;
    }

    public String getValueDataFileNameLastKnownVersion() {
        return mUnitName + "_" + mLastVersionNumber;
    }

    public String getGivenVersionValueDataFileName(String vn) {
        return mUnitName + "_" + vn;
    }

    public String getContainerName() {
        return mContainerName;
    }
    
    public long getLastVersionNumber() {
        return mLastVersionNumber;
    }

    public void setLastVersionNumber(long pLastVersionNumber) {
        this.mLastVersionNumber = pLastVersionNumber;
    }

    /**
     * 
     * @param pVersion - version that leads to the increment
     */
    public void incrementCloudVersionCount(long pVersion) {
        if (pVersion > mCurrentHighestVersion) {
            mCurrentHighestVersion = pVersion;
        }
        if (mCloudVersionCount >= n - f) {
            mHighestCloudVersion = mCurrentHighestVersion;
        }
    }

    /**
     * @return Long - last version for this data unit
     */
    public Long getMaxVersion() {
        if (mCloudVersionCount >= n - f) {
            return mHighestCloudVersion;
        }
        return mCurrentHighestVersion;
    }

    /**
     * {@inheritDoc}
     */
    public String toString() {
        return "DepSkySRegister: " + mContainerName + " # "
                + getContainerName() + "\n"
                + "lastVN = " + mLastVersionNumber;
    }
}
