#ifndef MD5HASH_H
#define MD5HASH_H

#include <string>


class MD5Hash {

public:
    void update(const std::string& chunk);
    std::string hexdigest();
    int length();

};


#endif

