#ifndef STORAGEDEVICE_H
#define STORAGEDEVICE_H


#include <string>



class StorageDevice {

public:
    int dev_id;
    int region;
    int zone;
    std::string ip;
    std::string replication_ip;
    int port;

    StorageDevice() :
        dev_id(-1),
        region(1),
        zone(-1),
        port(-1) {
    }

    StorageDevice(const StorageDevice& copy) :
        dev_id(copy.dev_id),
        region(copy.region),
        zone(copy.zone),
        ip(copy.ip),
        replication_ip(copy.replication_ip),
        port(copy.port) {
    }

    StorageDevice& operator=(const StorageDevice& copy) {
        if (this == &copy) {
            return *this;
        }

        dev_id = copy.dev_id;
        region = copy.region;
        zone = copy.zone;
        ip = copy.ip;
        replication_ip = copy.replication_ip;
        port = copy.port;

        return *this;
    }
};

#endif

