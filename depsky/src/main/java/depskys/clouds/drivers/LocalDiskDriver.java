package depskys.clouds.drivers;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.LinkedList;

import depskyDep.IDepSkySDriver;
import depskyDep.StorageCloudException;
import depskys.clouds.BlobMessage;

/**
 *
 * @author koras
 */
public class LocalDiskDriver implements IDepSkySDriver {

	private String mainPath;
	private String staticpath = "ds-local" + File.separator;
	private Socket socket;
	private ObjectOutputStream out;
	private ObjectInputStream in;
	private BlobMessage msg;
	private LinkedList<String> args;

	public LocalDiskDriver() {
	}

	public LocalDiskDriver(String driverpath) {
		try {
			socket = new Socket("127.0.0.1", 5555);
			out = new ObjectOutputStream(socket.getOutputStream());
			in = new ObjectInputStream(socket.getInputStream());

			args = new LinkedList<String>();
			args.add(driverpath);
			msg = new BlobMessage("init", args, null);
			out.writeObject(msg);
			out.reset();
			try {
				msg = (BlobMessage) in.readObject();
				if(msg.getOp().equals("ok")){
					//System.out.println("Cloud iniciada!");
				}
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		mainPath = driverpath;
	}

	private void init() {
		File f = new File(staticpath + File.separator + mainPath);
		if (!f.exists()) {
			f.mkdirs();
		} else {
			//System.out.println("path " + f.getPath() + " already exist... ");
		}
	}

	public String uploadData(String sid, String cid, byte[] data, String id) throws StorageCloudException {
		try {
			args.clear();
			args.add(sid);
			args.add(cid);
			args.add(id);
			BlobMessage msg = new BlobMessage("uploadData", args, data);
			out.writeObject(msg);
			out.reset();
			try {
				msg = (BlobMessage) in.readObject();
				if(msg.getOp().equals("ok")){
					//System.out.println("Upload executado com sucesso");
				}else{
					//System.out.println("Upload com problemas");
				}
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}

			return id;
		} catch (IOException iOException) {
			throw new StorageCloudException(iOException.getMessage());
		}
	}

	public byte[] downloadData(String sid, String cid, String id) throws StorageCloudException {
		try {

			args.clear();
			args.add(sid);
			args.add(cid);
			args.add(id);
			BlobMessage msg = new BlobMessage("downloadData", args, null);
			out.writeObject(msg);
			out.reset();
			try {
				msg = (BlobMessage) in.readObject();
				if(msg.getOp().equals("ok") && msg.getBytes() != null){
					//System.out.println("Download executado com sucesso");
					return msg.getBytes();
				}else{
					//System.out.println("download com problemas");
				}
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}

			return null;

		} catch (Exception e) {
			throw new StorageCloudException(e.getMessage());
		}
	}

	public boolean deleteData(String sid, String cid, String id) throws StorageCloudException {

		try{
			args.clear();
			args.add(sid);
			args.add(cid);
			args.add(id);
			BlobMessage msg = new BlobMessage("deleteData", args, null);
			out.writeObject(msg);
			out.reset();
			try {
				msg = (BlobMessage) in.readObject();
				if(msg.getOp().equals("ok")){
					//System.out.println("Download executado com sucesso");
					return true;
				}else{
					//System.out.println("download com problemas");
				}
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
			return false;
		} catch (Exception e) {
			throw new StorageCloudException(e.getMessage());
		}
	}

	public LinkedList<String> listNames(String prefix) throws StorageCloudException{

		try{
			args.clear();
			args.add("");
			args.add("");
			args.add("");
			BlobMessage msg = new BlobMessage("listNames", null, null);
			out.writeObject(msg);
			out.reset();
			try {
				msg = (BlobMessage) in.readObject();
				if(msg.getOp().equals("ok")){
					//System.out.println("Download executado com sucesso");
					LinkedList<String> allNames = msg.getArgs();
					LinkedList<String> find = new LinkedList<String>();
					for(String str : allNames){
						if(str.contains(prefix)){
							find.add(str);
						}

					}
					return find;
				}else{
					System.out.println("list com problemas");
				}
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
		} catch (Exception e) {
			throw new StorageCloudException(e.getMessage());
		}
		return null;
	}

	public String createContainer(String sid, String cid) throws StorageCloudException {
		try{
			args.clear();
			args.add(sid);
			args.add(cid);
			BlobMessage msg = new BlobMessage("createContainer", args, null);
			out.writeObject(msg);
			out.reset();
			try {
				msg = (BlobMessage) in.readObject();
				if(msg.getOp().equals("ok")){
					//System.out.println("Download executado com sucesso");
					return cid;
				}else{
					//System.out.println("download com problemas");
					throw new StorageCloudException("Container creation failed... maybe container already exist.");
				}
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
			return null;
		} catch (Exception e) {
			throw new StorageCloudException(e.getMessage());
		}
	}

	public boolean deleteContainer(String sid, String[] allNames) throws StorageCloudException {
		try{
			args.clear();
			//args.add(sid);
			//args.add(cid);
			for(String str : allNames){
				args.add(str);
			}
			BlobMessage msg = new BlobMessage("deleteContainer", args, null);
			out.writeObject(msg);
			out.reset();
			try {
				msg = (BlobMessage) in.readObject();
				if(msg.getOp().equals("ok")){
					//System.out.println("Delete executado com sucesso");
					return true;
				}else{
					//System.out.println("delete com problemas");
					throw new StorageCloudException("Container creation failed... maybe container already exist.");
				}
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
			return false;
		} catch (Exception e) {
			throw new StorageCloudException(e.getMessage());
		}
	}

	public String initSession() throws StorageCloudException {
		return mainPath;
	}

	public boolean endSession(String sid) throws StorageCloudException {
		return true;
	}

	public String getDriverId() {
		return mainPath;
	}

	public String getSessionKey() {
		return "sid";
	}

	public String getDataIdByName(String sid, String data_filename) throws StorageCloudException {
		return null;
	}

	public String[] getContainerAndDataIDsByName(String sid, String containername, String dataname) throws StorageCloudException {
		try{
			args.clear();
			args.add(sid);
			args.add(containername);
			args.add(dataname);
			BlobMessage msg = new BlobMessage("getContainerAndDataIDsByName", args, null);
			out.writeObject(msg);
			out.reset();
			try {
				msg = (BlobMessage) in.readObject();
				if(msg.getOp().equals("ok") && msg.getArgs() != null){
					//System.out.println("Delete executado com sucesso");
					LinkedList<String> l = msg.getArgs();
					String[] str = new String[l.size()];
					int i = 0;
					for(String a : l){
						str[i] = a;
						i++;
					}
					return str;
				}else{
					//System.out.println("delete com problemas");
					throw new StorageCloudException("Container creation failed... maybe container already exist.");
				}
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
			return null;
		} catch (IOException e) {
			throw new StorageCloudException(e.getMessage());
		}
	}

	public boolean setAcl(String arg0, String arg1, String arg2, String arg3)
			throws StorageCloudException {
		// TODO Auto-generated method stub
		return false;
	}
}