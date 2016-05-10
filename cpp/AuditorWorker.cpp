#include <stdlib.h>

#include "AuditorWorker.h"
#include "DiskFile.h"
#include "DiskFileManager.h"
#include "DiskFileReader.h"
#include "Exceptions.h"
#include "SwiftUtils.h"
#include "Time.h"

using namespace std;


AuditorWorker::AuditorWorker(Config conf,
                             Logger* logger,
                             const string& rcache,
                             const string& devices,
                             bool zero_byte_only_at_fps) {

    this->conf = conf;
    this->logger = logger;
    this->devices = devices;
    this->diskfile_router = DiskFileRouter(conf, this->logger);
    this->max_files_per_second = atof(conf.get("files_per_second", "20"));
    this->max_bytes_per_second = atof(conf.get("bytes_per_second",
                                               "10000000"));
    this->auditor_type = "ALL";
    this->zero_byte_only_at_fps = zero_byte_only_at_fps;
    if (this->zero_byte_only_at_fps) {
        this->max_files_per_second = atof(this->zero_byte_only_at_fps);
        this->auditor_type = "ZBF";
    }
    this->log_time = atoi(conf.get("log_time", "3600"));
    this->last_logged = 0;
    this->files_running_time = 0;
    this->bytes_running_time = 0;
    this->bytes_processed = 0;
    this->total_bytes_processed = 0;
    this->total_files_processed = 0;
    this->passes = 0;
    this->quarantines = 0;
    this->errors = 0;
    this->rcache = rcache;
    vector<string> stat_sizes =
        SwiftUtils::list_from_csv(conf.get("object_size_stats"));
    this->stats_sizes = sorted(
        [int(s) for s in stat_sizes]);
}

/*
void AuditorWorker::create_recon_nested_dict(top_level_key,
                                             device_list,
                                             item) {
    if (device_list) {
        device_key = ''.join(sorted(device_list))
        return {top_level_key: {device_key: item}}
    } else {
        return {top_level_key: item}
    }
}
*/

void AuditorWorker::auditObject(const AuditLocation& audit_location) {
    double loop_time = Time::time();
    this->failsafe_object_audit(audit_location);
    this->logger->timing_since("timing", loop_time);
    this->files_running_time =
        SwiftUtils::ratelimit_sleep(this->files_running_time,
                                    this->max_files_per_second);
    this->total_files_processed += 1;
    double now = Time::time();
    if (now - this->last_logged >= this->log_time) {
        /*
        this->logger->info(_(
            'Object audit (%(type)s). '
            'Since %(start_time)s: Locally: %(passes)d passed, '
            '%(quars)d quarantined, %(errors)d errors '
            'files/sec: %(frate).2f , bytes/sec: %(brate).2f, '
            'Total time: %(total).2f, Auditing time: %(audit).2f, '
            'Rate: %(audit_rate).2f') % {
                'type': '%s%s' % (this->auditor_type, description),
                'start_time': time.ctime(reported),
                'passes': this->passes, 'quars': this->quarantines,
                'errors': this->errors,
                'frate': this->passes / (now - reported),
                'brate': this->bytes_processed / (now - reported),
                'total': (now - begin), 'audit': time_auditing,
                'audit_rate': time_auditing / (now - begin)})
        cache_entry = this->create_recon_nested_dict(
            'object_auditor_stats_%s' % (this->auditor_type),
            device_dirs,
            {'errors': this->errors, 'passes': this->passes,
             'quarantined': this->quarantines,
             'bytes_processed': this->bytes_processed,
             'start_time': reported, 'audit_time': time_auditing})
        dump_recon_cache(cache_entry, this->rcache, this->logger);
        */
        reported = now;
        total_quarantines += this->quarantines;
        total_errors += this->errors;
        this->passes = 0;
        this->quarantines = 0;
        this->errors = 0;
        this->bytes_processed = 0;
        this->last_logged = now;
    }
    time_auditing += (now - loop_time);
}

void AuditorWorker::audit_all_objects(const AuditorOptions& options) {

    string description = "";
    if (options.device_dirs.length() > 0) {
        string device_dir_str = ','.join(sorted(options.device_dirs));
        if (this->auditor_type == "ALL") {
            description = string(" - parallel, ") + device_dir_str;
        } else {
            description = string(" - ") + device_dir_str;
        }
    }
    this->logger->info(string("Begin object audit \"") +
                       options.mode +
                       "\" mode (" +
                       this->auditor_type +
                       description +
                       ")");
    double reported = Time::time();
    double begin = reported;
    this->total_bytes_processed = 0;
    this->total_files_processed = 0;
    int total_quarantines = 0;
    int total_errors = 0;
    double time_auditing = 0;
    // TODO: we should move audit-location generation to the storage policy,
    // as we may (conceivably) have a different filesystem layout for each.
    // We'd still need to generate the policies to audit from the actual
    // directories found on-disk, and have appropriate error reporting if we
    // find a directory that doesn't correspond to any known policy. This
    // will require a sizable refactor, but currently all diskfile managers
    // can find all diskfile locations regardless of policy -- so for now
    // just use Policy-0's manager.

    //all_locs = (this->diskfile_router[POLICIES[0]]
    //            .object_audit_location_generator(device_dirs=device_dirs));

    //TODO: hook up DiskFileManager
    DiskFileManager* disk_file_manager = NULL;
    disk_file_manager->object_audit_location_generator(options, this);


    // Avoid divide by zero during very short runs
    double elapsed = max(Time::time() - begin, 0.000001);

    /*
    this->logger->info(_(
        'Object audit (%(type)s) "%(mode)s" mode '
        'completed: %(elapsed).02fs. Total quarantined: %(quars)d, '
        'Total errors: %(errors)d, Total files/sec: %(frate).2f, '
        'Total bytes/sec: %(brate).2f, Auditing time: %(audit).2f, '
        'Rate: %(audit_rate).2f') % {
            'type': '%s%s' % (this->auditor_type, description),
            'mode': mode, 'elapsed': elapsed,
            'quars': total_quarantines + this->quarantines,
            'errors': total_errors + this->errors,
            'frate': this->total_files_processed / elapsed,
            'brate': this->total_bytes_processed / elapsed,
            'audit': time_auditing, 'audit_rate': time_auditing / elapsed})
    */
    if (this->stats_sizes.size() > 0) {
        this->logger->info(
            string("Object audit stats: ") + this->stats_buckets.toString());
    }
}

void AuditorWorker::record_stats(int obj_size) {
    bool bucket_found = false;
    for (int i = 0; i < this->stats_sizes.size(); ++i) {
        int stat_size = this->stats_sizes[i];
        if (obj_size <= stat_size) {
            this->stats_buckets.increment(stat_size);
            bucket_found = true;
            break;
        }
    }

    if (!bucket_found) {
        this->stats_buckets.increment_over();
    }
}

void AuditorWorker::failsafe_object_audit(const AuditLocation& location) {
    try {
        this->object_audit(location);
    } catch (const exception& e) {
        this->logger->increment("errors");
        this->errors += 1;
        this->logger->exception(string("ERROR Trying to audit ") + location.toString());
    }
}

void AuditorWorker::onQuarantine(const string& msg) {
    throw DiskFileQuarantined(msg);
}

void AuditorWorker::object_audit(const AuditLocation& location) {

    DiskFileManager* diskfile_mgr =
        this->diskfile_router[location.policy];
    DiskFile* df;
    DiskFileReader* reader = NULL;

    try {
        df = diskfile_mgr->get_diskfile_from_audit_location(location);
        {
            OpenedDiskFile odf(df->open());
            metadata = df->get_metadata();
            int obj_size = atoi(metadata["Content-Length"]);
            if (this->stats_sizes.size() > 0) {
                this->record_stats(obj_size);
            }
            if (this->zero_byte_only_at_fps && obj_size) {
                this->passes += 1;
                return;
            }
            reader = df->reader(this);
        }
        //TODO: change 'with' to RAII to close reader
        with closing(reader):
            for (chunk in reader) {
                int chunk_len = chunk.size();
                this->bytes_running_time =
                    SwiftUtils::ratelimit_sleep(
                        this->bytes_running_time,
                        this->max_bytes_per_second,
                        incr_by=chunk_len);
                this->bytes_processed += chunk_len;
                this->total_bytes_processed += chunk_len;
            }
    } catch (const DiskFileNotExist& dfne) {
        return;
    } catch (const DiskFileQuarantined& err) {
        this->quarantines += 1;
        this->logger->error(string("ERROR Object ") +
                            location.toString() +
                            " failed audit and was quarantined: " +
                            err.toString());
    }

    this->passes += 1;
}

