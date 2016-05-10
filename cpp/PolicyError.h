#ifndef POLICYERROR_H
#define POLICYERROR_H

#include <string>

#include "StrUtils.h"


class PolicyError {

public:
    std::string msg;
    int index;


    PolicyError(const std::string& aMsg, int anIndex=-1) : 
        msg(aMsg),
        index(anIndex) {
    }

    PolicyError(const PolicyError& copy) :
        msg(copy.msg),
        index(copy.index) {
    }

    PolicyError& operator=(const PolicyError& copy) {
        if (this == &copy) {
            return *this;
        }

        msg = copy.msg;
        index = copy.index;

        return *this;
    }

    std::string toString() const {
        return std::string("PolicyError: index=") +
               StrUtils::toString(index) + 
               ", message=" +
               msg;
    }
};

#endif

