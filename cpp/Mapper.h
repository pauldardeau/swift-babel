#ifndef MAPPER_H
#define MAPPER_H

#include <string>

#include "KeyValuePair.h"


class Mapper {
public:
    virtual ~Mapper() {}

    virtual KeyValuePair map(const std::string& filename) = 0;

};


#endif

