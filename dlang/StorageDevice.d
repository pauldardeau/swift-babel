

class StorageDevice {

public:
    int dev_id;
    int region;
    int zone;
    string ip;
    string replication_ip;
    int port;

    this() {
        dev_id = -1;
        region = 1;
        zone = -1;
        ip = null;
        replication_ip = null;
        port = -1;
    }
}

