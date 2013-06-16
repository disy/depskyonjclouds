package depskys.core.configuration;

/**
 * 
 * @author Andreas Rain, University of Konstanz
 *
 */
public final class Account {
    private String type;
    private String id;
    private String accessKey;
    private String secretKey;
    
    public String getType() {
        return type;
    }
    public void setType(String type) {
        this.type = type;
    }
    public String getId() {
        return id;
    }
    public void setId(String id) {
        this.id = id;
    }
    public String getAccessKey() {
        return accessKey;
    }
    public void setAccessKey(String accessKey) {
        this.accessKey = accessKey;
    }
    public String getSecretKey() {
        return secretKey;
    }
    public void setSecretKey(String secretKey) {
        this.secretKey = secretKey;
    }
    
    @Override
    public String toString() {
        return "Account [type=" + type + ", id=" + id + ", accessKey=" + accessKey + ", secretKey="
            + secretKey + "]";
    }
    
    
}
