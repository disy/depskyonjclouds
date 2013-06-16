package depskys.core;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Class that stores the metadata information for each version
 * 
 * @author tiago oliveira
 *
 */
public class DepSkyMetadata {

	private byte[] allDataHash;
	private String versionFileId;
	private byte[] signature;	
	private String metadata;

	public DepSkyMetadata(){
	}
	
	public DepSkyMetadata(String medatada, byte[] signature, byte[] allDataHash, String versionFileId){
		this.metadata = medatada;
		this.signature = signature;
		this.allDataHash = allDataHash;
		this.versionFileId = versionFileId;
	}

	public String getVersionFileId(){
		return versionFileId;
	}

	public byte[] getAllDataHash(){
		return allDataHash;
	}

	public byte[] getsignature(){
		return signature;
	}
	
	public String getMetadata(){
		return metadata;
	}

	public void deserialize(DataInput in) throws IOException,
			ClassNotFoundException {
		allDataHash = new byte[in.readInt()];
		in.readFully(allDataHash);
		versionFileId = in.readUTF();
		signature = new byte[in.readInt()];
		in.readFully(signature);
		metadata = in.readUTF();
	}

	public void serialize(DataOutput out) throws IOException {		
		out.writeInt(allDataHash.length);
		out.write(allDataHash);
		out.writeUTF(versionFileId);
		out.writeInt(signature.length);
		out.write(signature);
		out.writeUTF(metadata);	
	}
}
