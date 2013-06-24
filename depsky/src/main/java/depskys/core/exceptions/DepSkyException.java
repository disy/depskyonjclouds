package depskys.core.exceptions;

/**
 * General exception type thrown by depsky client.
 * @author Andreas Rain, University of Konstanz
 *
 */
public class DepSkyException extends Exception{
    
    public DepSkyException(){
        
    }
    
    public DepSkyException(String s) {
        super(s);
    }
}
