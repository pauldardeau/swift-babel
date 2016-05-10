#ifndef OBJECTAUDITOR_H
#define OBJECTAUDITOR_H

#include <string>
#include <vector>

#include "AuditorOptions.h"
#include "ConfigParser.h"
#include "Daemon.h"
#include "Logger.h"


class ObjectAuditor : Daemon
{

protected:
    ConfigParser conf;
    Logger* logger;
    std::string devices;
    int concurrency;
    int conf_zero_byte_fps;
    std::string recon_cache_path;
    std::string rcache;
    int interval;


    void _sleep();


public:
    ObjectAuditor(ConfigParser conf);
    virtual ~ObjectAuditor() {}

    void clear_recon_cache(const std::string& auditor_type);

    void run_audit(AuditorOptions& options);

    int fork_child(AuditorOptions& options);

    virtual void audit_loop(bool parent,
                            int zbo_fps,
                            AuditorOptions& options) = 0;

    void run_forever();
    void run_once();

    static void erase_id(std::vector<int>& id_list, int id_value);


};

#endif

