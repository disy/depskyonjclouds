package depskys.core.configuration;

import java.util.List;

public final class Configuration {
    
    private List<Account> clouds;

    public Configuration(){
        
    }
    
    public List<Account> getAccounts() {
        return clouds;
    }

    public void setAccounts(List<Account> clouds) {
        this.clouds = clouds;
    }

    @Override
    public String toString() {
        return "Configuration [accounts=" + clouds + "]";
    }
    
}
