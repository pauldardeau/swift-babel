#ifndef EXCEPTIONS_H
#define EXCEPTIONS_H

#include <string>
#include <exception>


class BaseException : public std::exception {
public:
    std::string _msg;

    BaseException() {}
    BaseException(const std::string& msg) :
        _msg(msg) {
    }

    BaseException(const BaseException& copy) :
        _msg(copy._msg) {
    }

    virtual ~BaseException() throw() {}

    BaseException& operator=(const BaseException& copy) {
        if (this == &copy) {
            return *this;
        }

        _msg = copy._msg;

        return *this;
    }

    virtual std::string toString() const {
        return _msg;
    }

    virtual const std::string& message() const {
        return _msg;
    }
};


class IOError : public BaseException {
public:
    int _errno;

    IOError() {}
    IOError(const std::string& msg) :
        BaseException(msg) {
    }

    IOError(const IOError& copy) :
        BaseException(copy) {
    }

    virtual ~IOError() throw() {}

    IOError& operator=(const IOError& copy) {
        if (this == &copy) {
            return *this;
        }

        BaseException::operator=(copy);

        return *this;
    }
};


class ValueError : public BaseException {
public:
    ValueError() {}
    ValueError(const std::string& msg) :
        BaseException(msg) {
    }

    ValueError(const ValueError& copy) :
        BaseException(copy) {
    }

    virtual ~ValueError() throw() {}

    ValueError& operator=(const ValueError& copy) {
        if (this == &copy) {
            return *this;
        }

        BaseException::operator=(copy);

        return *this;
    }
};


class DiskFileXattrNotSupported : public BaseException {
public:
    virtual ~DiskFileXattrNotSupported() throw() {}

};


class DiskFileDeleted : public BaseException {
public:
    virtual ~DiskFileDeleted() throw() {}

};


class DiskFileNotExist : public BaseException {
public:
    virtual ~DiskFileNotExist() throw() {}

};


class DiskFileNoSpace : public BaseException {
public:
    virtual ~DiskFileNoSpace() throw() {}

};


class DiskFileCollision : public BaseException {
public:
    virtual ~DiskFileCollision() throw() {}

};


class DiskFileNotOpen : public BaseException {
public:
    virtual ~DiskFileNotOpen() throw() {}

};


class DiskFileQuarantined : public BaseException {
public:
    DiskFileQuarantined(const std::string& msg) :
        BaseException(msg) {
    }

    DiskFileQuarantined(const DiskFileQuarantined& copy) :
        BaseException(copy) {
    }

    virtual ~DiskFileQuarantined() throw() {}


    DiskFileQuarantined& operator=(const DiskFileQuarantined& copy) {
        if (this == &copy) {
            return *this;
        }

        BaseException::operator=(copy);

        return *this;
    }

    std::string toString() const {
        return std::string("DiskFileQuarantined: ") + message();
    }
};


class Timeout : public BaseException {
public:
    virtual ~Timeout() throw() {}

};


class OSError : public BaseException {
public:
    int _errno;

    OSError(int errno_value) :
        _errno(errno_value) {
    }

    OSError(const OSError& copy) :
        BaseException(copy),
        _errno(copy._errno) {
    }

    virtual ~OSError() throw() {}

    OSError& operator=(const OSError& copy) {
        if (this == &copy) {
            return *this;
        }

        BaseException::operator=(copy);
        _errno = copy._errno;

        return *this;
    }

    std::string toString() const {
        //TODO: implement OSError toString
        return "";
    }
};


#endif

