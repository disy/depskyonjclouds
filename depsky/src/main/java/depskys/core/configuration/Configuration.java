package depskys.core.configuration;

import java.util.List;

public final class Configuration {
    
    private List<Account> clouds;
    
    public List<Account> getClouds() {
        return clouds;
    }

    public void setClouds(List<Account> clouds) {
        this.clouds = clouds;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        
        for (Account acc : clouds) {
            builder.append(acc.toString()).append("\n");
        }
        return builder.toString();
    }
    
}
