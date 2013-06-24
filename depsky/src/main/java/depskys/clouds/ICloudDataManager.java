package depskys.clouds;

import depskys.clouds.replys.DataCloudReply;
import depskys.clouds.replys.MetaCloudReply;

public interface ICloudDataManager {

    void processMetadata(MetaCloudReply metadataReply);

    void checkDataIntegrity(DataCloudReply valuedataReply);

    void writeNewMetadata(DataCloudReply reply);
}
