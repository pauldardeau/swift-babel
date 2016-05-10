#ifndef AUDITORWORKER_H
#define AUDITORWORKER_H

#include <string>
#include <map>
#include <vector>

#include "AuditLocation.h"
#include "AuditorOptions.h"
#include "Config.h"
#include "DiskFileRouter.h"
#include "Logger.h"
#include "ObjectAuditHook.h"
#include "QuarantineHook.h"
#include "StatBuckets.h"


class AuditorWorker : public QuarantineHook, public ObjectAuditHook
{

private:
    Config conf;
    Logger* logger;
    std::string devices;
    bool zero_byte_only_at_fps;
    float max_files_per_second;
    float max_bytes_per_second;
    std::string auditor_type;
    int bytes_running_time;
    int files_running_time;
    long last_logged;
    int log_time;
    int bytes_processed;
    int total_bytes_processed;
    int total_files_processed;
    int passes;
    int quarantines;
    int errors;
    std::vector<int> stats_sizes;
    StatBuckets stats_buckets;
    DiskFileRouter diskfile_router;
    std::string rcache;


public:
    AuditorWorker(Config conf,
                  Logger* logger,
                  const std::string& rcache,
                  const std::string& devices);

    AuditorWorker(Config conf,
                  Logger* logger,
                  const std::string& rcache,
                  const std::string& devices,
                  bool zero_byte_only_at_fps);

    void audit_all_objects(const AuditorOptions& options);

    void record_stats(int obj_size);

    // QuarantineHook
    void onQuarantine(const std::string& msg);

    // ObjectAuditHook
    void auditObject(const AuditLocation& audit_location);

    void failsafe_object_audit(const AuditLocation& location);
    void object_audit(const AuditLocation& location);
};

#endif



