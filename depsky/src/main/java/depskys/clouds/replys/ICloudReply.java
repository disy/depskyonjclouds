package depskys.clouds.replys;

import depskys.core.DepSkyDataUnit;

public interface ICloudReply {
    
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

}
