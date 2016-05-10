#ifndef MTOBJECTAUDITOR_H
#define MTOBJECTAUDITOR_H


#include "ObjectAuditor.h"


class AuditorOptions;
class ConfigParser;


class MTObjectAuditor : ObjectAuditor 
{

private:
    // disallow copies
    MTObjectAuditor(const MTObjectAuditor&);
    MTObjectAuditor& operator=(const MTObjectAuditor&);
    MTObjectAuditor();


public:
    MTObjectAuditor(ConfigParser conf);

    int run_thread(AuditorOptions& options);

    void audit_loop(bool parent,
                    int zbo_fps,
                    AuditorOptions& options);

};

#endif

