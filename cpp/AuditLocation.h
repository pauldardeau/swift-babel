#ifndef AUDITLOCATION_H
#define AUDITLOCATION_H

#include <string>

#include "StrUtils.h"


class AuditLocation {

public:
    std::string path;
    std::string device;
    std::string partition;
    int policy;


    AuditLocation() :
        policy(-1) {
    }

    AuditLocation(const std::string& path_value,
                  const std::string& device_value,
                  const std::string& partition_value,
                  int policy_value) :
        path(path_value),
        device(device_value),
        partition(partition_value),
        policy(policy_value) {
    }

    AuditLocation(const AuditLocation& copy) :
        path(copy.path),
        device(copy.device),
        partition(copy.partition),
        policy(copy.policy) {
    }

    AuditLocation& operator=(const AuditLocation& copy) {
        if (this == &copy) {
            return *this;
        }

        path = copy.path;
        device = copy.device;
        partition = copy.partition;
        policy = copy.policy;

        return *this;
    }

    virtual std::string toString() const {
        return std::string("AuditLocation: path='") +
                           path +
                           "', device='" +
                           device +
                           ", partition='" +
                           partition +
                           ", policy=" +
                           StrUtils::toString(policy);
    }
};

#endif

