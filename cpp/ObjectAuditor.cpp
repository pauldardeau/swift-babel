#include <stdlib.h>
#include <algorithm>
#include <set>

#include "ObjectAuditor.h"
#include "AuditorWorker.h"
#include "Exceptions.h"
#include "OSUtils.h"
#include "SwiftUtils.h"
#include "Time.h"


using namespace std;


void ObjectAuditor::erase_id(vector<int>& list_ids, int id_value) {
    vector<int>::iterator it = list_ids.begin();
    const vector<int>::const_iterator itEnd = list_ids.end();

    for (; it != itEnd; ++it) {
        if (*it == id_value) {
            list_ids.erase(it);
            break;
        }
    }
}


ObjectAuditor::ObjectAuditor(ConfigParser conf) :
    Daemon(conf) {

    this->conf = conf;
    this->logger = Logger::get_logger("object-auditor");
    this->devices = conf.get("devices", "/srv/node");
    this->concurrency = atoi(conf.get("concurrency", "1").c_str());
    this->conf_zero_byte_fps = atoi(
        conf.get("zero_byte_files_per_second", "50").c_str());
    this->recon_cache_path = conf.get("recon_cache_path",
                                     "/var/cache/swift");
    this->rcache = OSUtils::path_join(this->recon_cache_path, "object.recon");
    this->interval = atoi(conf.get("interval", "30").c_str());
}

void ObjectAuditor::_sleep() {
    Time::sleep(this->interval);
}

/*
void ObjectAuditor::clear_recon_cache(const std::string& auditor_type) {
    dump_recon_cache({"object_auditor_stats_%s" % auditor_type: {}},
                     this->rcache, this->logger);
}
*/

void ObjectAuditor::run_audit(AuditorOptions& options) {
    AuditorWorker worker(this->conf,
                         this->logger,
                         this->rcache,
                         this->devices,
                         zero_byte_only_at_fps);
    worker.audit_all_objects(options);
}

void ObjectAuditor::run_forever(AuditorOptions& options) {
    // zero byte only command line option
    zbo_fps = options.zero_byte_fps;
    bool parent = false;
    if (zbo_fps) {
        // only start parent
        parent = true;
    }

    options.mode = "forever";

    while (true) {
        try {
            this->audit_loop(parent, zbo_fps, options);
        } catch (const exception& err) {
            this->logger->exception(string("ERROR auditing: ") + err.what());
        }
        this->_sleep();
    }
}

void ObjectAuditor::run_once(AuditorOptions& options) {
    // zero byte only command line option
    int zbo_fps = options.zero_byte_fps;
    vector<string> override_devices =
        SwiftUtils::list_from_csv(options.devices);
    // Remove bogus entries and duplicates from override_devices
    options.override_devices = list(
        set(SwiftUtils::listdir(this->devices)).intersection(set(override_devices)));
    bool parent = false;
    if (zbo_fps) {
        // only start parent
        parent = true;
    }

    options.mode = "once";

    try {
        this->audit_loop(parent, zbo_fps, options);
    } catch (const exception& err) {
        this->logger->exception(string("ERROR auditing: ") + err.what());
    }
}

