package depskys.clouds;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.google.common.io.ByteArrayDataInput;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;

/**
 * This message is being used to put into blobs.
 * 
 * @author Andreas Rain, University of Konstanz
 */
public class BlobMessage{
	
	private String mOp;
	private Map<String, String> mArgs;
	private byte[] mPayload;
    private byte[] mDataHash;
	
	/**
	 * Create a new {@link BlobMessage}
	 * @param pOp - the operation type for this message
	 * @param pPayload - the data to be written
	 */
	public BlobMessage(byte[] pPayload){
		this.mArgs = new HashMap<String, String>();
		this.mPayload = pPayload;
	}
	
	/**
	 * Add arguments to this message
	 * @param pKey
	 * @param pValue
	 */
	public void addArg(String pKey, String pValue){
	    this.mArgs.put(pKey, pValue);
	}
        
        /**
         * Add arguments to this message
         * @param pKey
         * @param pValue
         */
        public String getArg(String pKey){
            return this.mArgs.get(pKey);
        }
	
	public void setArgs(Map<String, String> pArgs){
	    this.mArgs = pArgs;
	}
	
	public Map<String, String> getArgs(){
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
	public void serialize(ByteArrayDataOutput pOutput) throws IOException{
	    pOutput.writeInt(mArgs.entrySet().size());
	    for (Entry<String, String> arg : mArgs.entrySet()) {
	        byte[] keyByte = arg.getKey().getBytes("UTF-8");
	        pOutput.writeInt(keyByte.length);
	        pOutput.write(keyByte);
                byte[] valueByte = arg.getValue().getBytes("UTF-8");
                pOutput.writeInt(valueByte.length);
                pOutput.write(valueByte);
            }

            pOutput.writeInt(mDataHash.length);
            pOutput.write(mDataHash);
	    pOutput.writeInt(mPayload.length);
	    pOutput.write(mPayload);
	}
	
	/**
	 * Deserialize a given {@link DataInput} as a {@link BlobMessage} object
	 * @param pInput
	 * @return returns the {@link BlobMessage} for the given {@link DataInput}
	 * @throws IOException
	 */
	public static BlobMessage deserialize(InputStream pInput) throws IOException{
	    
	    byte[] pBytes = new byte[pInput.available()];
	    pInput.read(pBytes);
	    
	    ByteArrayDataInput input = ByteStreams.newDataInput(pBytes);
	    
	    int argC = input.readInt();
	    Map<String, String> args = new HashMap<String, String>();
	    
	    for (int i = 0; i < argC; i++) {
                byte[] keyByte = new byte[input.readInt()];
                input.readFully(keyByte);
                String key = new String(keyByte, "UTF-8");
                byte[] valueByte = new byte[input.readInt()];
                input.readFully(valueByte);
                String value = new String(valueByte, "UTF-8");
                
                args.put(key,  value);
            }
            
            int dataHashSize = input.readInt();
            byte[] datahash = new byte[dataHashSize];
            input.readFully(datahash);
	    int payloadSize = input.readInt();
	    byte[] payload = new byte[payloadSize];
	    input.readFully(payload);
	    
            BlobMessage message = new BlobMessage(payload);
            message.setArgs(args);
            message.setDataHash(datahash);
            
            return message;
	}
        
        /**
         * Deserialize a given {@link DataInput} as a {@link BlobMessage} object
         * @param pInput
         * @return returns the {@link BlobMessage} for the given {@link DataInput}
         * @throws IOException
         */
        public static BlobMessage deserializeOnlyMeta(InputStream pInput) throws IOException{
            
            byte[] pBytes = new byte[pInput.available()];
            pInput.read(pBytes);
            
            ByteArrayDataInput input = ByteStreams.newDataInput(pBytes);
            
            int argC = input.readInt();
            Map<String, String> args = new HashMap<String, String>();
            
            for (int i = 0; i < argC; i++) {
                byte[] keyByte = new byte[input.readInt()];
                input.readFully(keyByte);
                String key = new String(keyByte, "UTF-8");
                byte[] valueByte = new byte[input.readInt()];
                input.readFully(valueByte);
                String value = new String(valueByte, "UTF-8");
                
                args.put(key,  value);
            }
            
            int dataHashSize = input.readInt();
            byte[] datahash = new byte[dataHashSize];
            input.readFully(datahash);
            
            BlobMessage message = new BlobMessage(null);
            message.setArgs(args);
            message.setDataHash(datahash);
            
            return message;
        }

        public byte[] getDataHash() {
            return mDataHash;
        }

        public void setDataHash(byte[] allDataHash) {
            this.mDataHash = allDataHash;
        }
	
	

}
