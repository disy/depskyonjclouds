package depskys.core;

import java.io.File;

import depskys.clouds.replys.DataCloudReply;
import depskys.core.exceptions.CouldNotGetDataException;
import depskys.core.exceptions.DepSkyException;
import depskys.core.exceptions.IDepSkyWriteException;
import depskys.core.exceptions.NoDataAvailableException;

/**
 * Interface for depskys read/write protocol
 * @author bruno
 * 
 *         Modified by @author Andreas Rain, University of Konstanz
 *         
 *         Former: IDepSkySProtocol
 */
public interface IDepSkyClient {

    /**
     * Retrieve the clients id
     * @return int id
     */
    public int getClientId();
    
    /**
     * Read the last version written for the file associated with pDataUnit
     * 
     * @param pDataUnit
     *            - the DataUnit associated with the data
     * 
     */
    public byte[] read(DepSkyDataUnit pDataUnit) throws DepSkyException, InterruptedException;

    /**
     * Writes the value value in the corresponding dataUnit pDataUnit
     * 
     * @param pDataUnit
     *            - the DataUnit associated with the data
     * @param value
     *            - value to be written
     * @return the hash of the value written
     * 
     */
    public byte[] write(DepSkyDataUnit pDataUnit, byte[] value) throws IDepSkyWriteException;

    /**
     * Method that releases (when receive N-F replies (in most of the cases)) all the locks made by
     * broadcasts
     * 
     * @param reply
     *            - reply received by each broadcast containing the response of the clouds
     * 
     */
    public void dataReceived(DataCloudReply reply);

    public boolean sendingParallelRequests();
    
    /**
     * 
     * @author Andreas Rain, University of Konstanz
     *
     */
    public static class DepSkyClientFactory{
        
        private static String mConfigPath = new StringBuilder().append("src").append(File.separator).append("test")
        .append(File.separator).append("resources").append(File.separator).append(
        "account.props.yml").toString();

        
        /**
         * Create an instance of the default client using the standard configuration path (lies within the project).
         * @param clientId - give your client a unique id
         *
         * @return a new instance of {@link DefaultClient}
         */
        public static IDepSkyClient create(int clientId){
            return new DefaultClient(clientId, mConfigPath);
        }
        
        /**
         * Create an instance of the default client.
         * @param clientId - give your client a unique id
         * @param pConfigPath - where does the configuration file account.props.yml lie?
         *
         * @return a new instance of {@link DefaultClient}
         */
        public static IDepSkyClient create(int clientId, String pConfigPath){
            return new DefaultClient(clientId, pConfigPath);
        }
    }
}
