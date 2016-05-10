#ifndef OBJECTAUDITHOOK_H
#define OBJECTAUDITHOOK_H


class AuditLocation;


class ObjectAuditHook {

public:
    virtual ~ObjectAuditHook() {}
    virtual void auditObject(const AuditLocation& audit_location) = 0;

};

#endif

