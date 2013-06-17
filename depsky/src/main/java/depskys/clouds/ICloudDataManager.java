package depskys.clouds;

import depskys.clouds.replys.DataCloudReply;

public interface ICloudDataManager {

    void processMetadata(DataCloudReply metadataReply);

    void checkDataIntegrity(DataCloudReply valuedataReply);

    void writeNewMetadata(DataCloudReply reply);
}
