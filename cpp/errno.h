#ifndef ERRNO_H
#define ERRNO_H

class errno {

public:
    enum {
        ENOENT,
        ENOSPC,
        EDQUOT,
        EEXIST,
        ENOTEMPTY,
        ENOTDIR,
        EAFNOSUPPORT,
        EWOULDBLOCK
    };
};

#endif

