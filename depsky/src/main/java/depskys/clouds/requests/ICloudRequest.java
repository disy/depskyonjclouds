package depskys.clouds.requests;

import depskys.core.DepSkyDataUnit;

/**
 * This interface defines general operations that can be made on cloud requests
 * 
 * @author Andreas Rain, University of Konstanz
 *
 */
public interface ICloudRequest {
    
    public int getOp();
    public void incrementRetries();
    public void resetRetries();
    public int getRetries();
    
    /**
     * Retrieve the data unit associated with this request
     * @return {@link DepSkyDataUnit}
     */
    public DepSkyDataUnit getDataUnit();
    
    /**
     * Determine when this procedure started originally
     * @return long - start time of this procedure
     */
    public long getStartTime();
    
    /**
     * Determine the sequence of this request
     * @return long
     */
    public long getSequenceNumber();
    
    /**
     * Get the data resulting from this request
     * @return byte[] consisting of the data
     */
    public byte[] getData();
    
    
}
