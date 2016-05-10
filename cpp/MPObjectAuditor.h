#ifndef MPOBJECTAUDITOR_H
#define MPOBJECTAUDITOR_H


#include "ObjectAuditor.h"


class AuditorOptions;
class ConfigParser;


class MPObjectAuditor : ObjectAuditor 
{

private:
    // disallow copies
    MPObjectAuditor(const MPObjectAuditor&);
    MPObjectAuditor& operator=(const MPObjectAuditor&);
    MPObjectAuditor();

public:
    MPObjectAuditor(ConfigParser conf);

    int fork_child(AuditorOptions& options, bool zero_byte_fps=false);

    void audit_loop(bool parent,
                    int zbo_fps,
                    AuditorOptions& options);

};

#endif

