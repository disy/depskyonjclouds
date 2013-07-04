package depskys.core;

import java.io.File;
import java.util.List;

import depskys.clouds.replys.ICloudReply;
import depskys.core.exceptions.DepSkyException;

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
    public byte[] write(DepSkyDataUnit pDataUnit, byte[] value) throws DepSkyException;
    
    /**
     * 
     * @param pContainerName
     * @return the list of blobs contained by that container
     * @throws DepSkyException
     */
    public List<String> list(String pContainerName) throws DepSkyException;
    
    /**
     * 
     * @param pReply
     */
    public void dataReceived(ICloudReply pReply);

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
