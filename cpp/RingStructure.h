#ifndef RINGSTRUCTURE_H
#define RINGSTRUCTURE_H


#include <string>
#include <vector>

#include "StorageDevice.h"


class RingStructure {

public:
    std::vector<StorageDevice*> devs;
    int part_shift;
    std::vector<std::vector<int> > replica2part2dev_id;


    static std::string toString(const std::vector<int>& part2dev_id);
    void serialize_v1(const std::string& filename);

    static RingStructure* deserialize_v1(const std::string& filename);
};

#endif

