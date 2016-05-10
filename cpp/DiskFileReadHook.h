#ifndef DISKFILEREADHOOK_H
#define DISKFILEREADHOOK_H

#include <string>


class DiskFileReadHook {

public:
    virtual ~DiskFileReadHook() {}
    virtual void onFileRead(const std::string& chunk) = 0;
};

#endif

