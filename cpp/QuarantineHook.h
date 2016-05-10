#ifndef QUARANTINEHOOK_H
#define QUARANTINEHOOK_H


#include <string>

class QuarantineHook {

public:
    virtual ~QuarantineHook() {}
    virtual void onQuarantine(const std::string& quarantine_reason) = 0;

};

#endif

