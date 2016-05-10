#ifndef AUDITOROPTIONS_H
#define AUDITOROPTIONS_H

#include <string>


class AuditorOptions {

public:
    std::string mode;
    std::string devices;
    std::string device_dirs;
    std::string override_devices;
    bool zero_byte_fps;
    bool mount_check;


    AuditorOptions() :
        zero_byte_fps(false),
        mount_check(false) {
    }

    AuditorOptions(const AuditorOptions& copy) :
        mode(copy.mode),
        devices(copy.devices),
        device_dirs(copy.device_dirs),
        override_devices(copy.override_devices),
        zero_byte_fps(copy.zero_byte_fps),
        mount_check(copy.mount_check) {
    }

    AuditorOptions& operator=(const AuditorOptions& copy) {
        if (this == &copy) {
            return *this;
        }

        mode = copy.mode;
        devices = copy.devices;
        device_dirs = copy.device_dirs;
        override_devices = copy.override_devices;
        zero_byte_fps = copy.zero_byte_fps;
        mount_check = copy.mount_check;

        return *this;
    }
    
};

#endif

