package depskys.clouds;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * This message is being used to put into blobs.
 * 
 * @author Andreas Rain, University of Konstanz
 */
public class BlobMessage{
	
	private String mOp;
	private List<String> mArgs;
	private byte[] mPayload;
	
	/**
	 * Create a new {@link BlobMessage}
	 * @param pOp - the operation type for this message
	 * @param pPayload - the data to be written
	 */
	public BlobMessage(String pOp, byte[] pPayload){
		this.mOp = pOp;
		this.mArgs = new LinkedList<String>();
		this.mPayload = pPayload;
	}
	
	public String getOp(){
		return this.mOp;
	}
	
	/**
	 * Add arguments to this message
	 * @param pArg
	 */
	public void addArg(String pArg){
	    this.mArgs.add(pArg);
	}
	
	public void setArgs(List<String> pArgs){
	    this.mArgs = pArgs;
	}
	
	public List<String> getArgs(){
		return this.mArgs;
	}
	
	public byte[] getPayload(){
		return this.mPayload;
	}
	
	/**
	 * Serialize this message
	 * @param pOutput - write the messages content into this {@link DataOutput}
	 * @throws IOException 
	 *            - if something goes wrong
	 */
	public void serialize(DataOutput pOutput) throws IOException{
	    pOutput.writeChars(mOp);
	    pOutput.write(mArgs.size());
	    for (String arg : mArgs) {
                pOutput.writeChars(arg);
            }
	    pOutput.writeInt(mPayload.length);
	    pOutput.write(mPayload);
	}
	
	/**
	 * Deserialize a given {@link DataInput} as a {@link BlobMessage} object
	 * @param pInput
	 * @return returns the {@link BlobMessage} for the given {@link DataInput}
	 * @throws IOException
	 */
	public static BlobMessage deserialize(DataInput pInput) throws IOException{
	    String op = pInput.readLine();
	    int argC = pInput.readInt();
	    LinkedList<String> args = new LinkedList<String>();
	    
	    for (int i = 0; i < argC; i++) {
                args.add(pInput.readLine());
            }
	    
	    int payloadSize = pInput.readInt();
	    byte[] payload = new byte[payloadSize];
	    
	    pInput.readFully(payload);
	    
            BlobMessage message = new BlobMessage(op, payload);
            message.setArgs(args);
            
            return message;
	}
	
	

}
