#ifndef RINGDATA_H
#define RINGDATA_H

#include <stdio.h>
#include <string>

#include "RingStructure.h"


class RingData {

private:
    RingStructure _ring;
    //StorageDevice[] devs;
    //int[][] _replica2part2dev_id;
    //int _part_shift;


public:
    /*
    static RingStructure deserialize_v1(GZIPInputStream gz_file,
                                        bool metadata_only=false);
    */

    static RingData load(const std::string& filename,
                         bool metadata_only=false);

    RingData(RingStructure ring);
    void serialize_v1(FILE* file_obj);

    void save(const std::string& filename);

};

#endif

