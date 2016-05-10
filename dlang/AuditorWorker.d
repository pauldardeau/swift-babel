module AuditorWorker;

import AuditLocation;
import Config;
import DiskFileManager;
import DiskFileRouter;
import Logger;
import QuarantineHook;
import StrUtils;
import SwiftUtils;


/**
 Walk through file system to audit objects
 */
class AuditorWorker : QuarantineHook
{

private:
    Config conf;
    Logger logger;
    DiskFileRouter diskfile_router;
    string devices;
    bool zero_byte_only_at_fps;
    float max_files_per_second;
    float max_bytes_per_second;
    string auditor_type;
    int bytes_running_time;
    int files_running_time;
    int last_logged;
    int bytes_processed;
    int total_bytes_processed;
    int total_files_processed;
    int passes;
    int quarantines;
    int errors;
    int[] stats_sizes;


public:
    this(Config conf,
         Logger logger,
         string rcache,
         string devices) {
        this(conf, logger, rcache, devices, false);
    }

    this(Config conf,
         Logger logger,
         string rcache,
         string devices,
         bool zero_byte_only_at_fps)
    {
        this.conf = conf;
        this.logger = logger;
        this.devices = devices;
        this.diskfile_router = new DiskFileRouter(conf, this.logger);
        this.max_files_per_second = to!float(conf.get("files_per_second", 20));
        this.max_bytes_per_second = to!float(conf.get("bytes_per_second",
                                                   10000000));
        this.auditor_type = "ALL";
        this.zero_byte_only_at_fps = zero_byte_only_at_fps;
        if (this.zero_byte_only_at_fps) {
            this.max_files_per_second = to!float(this.zero_byte_only_at_fps);
            this.auditor_type = "ZBF";
        }
        this.log_time = to!int(conf.get("log_time", 3600));
        this.last_logged = 0;
        this.files_running_time = 0;
        this.bytes_running_time = 0;
        this.bytes_processed = 0;
        this.total_bytes_processed = 0;
        this.total_files_processed = 0;
        this.passes = 0;
        this.quarantines = 0;
        this.errors = 0;
        this.rcache = rcache;
        /*
        this.stats_sizes = sorted(
            [int(s) for s in SwiftUtils.list_from_csv(conf.get("object_size_stats"))]);
        this.stats_buckets = dict(
            [(s, 0) for s in this.stats_sizes + ["OVER"]]);
        */
    }

    /*
    void create_recon_nested_dict(top_level_key,
                                  string[] device_list,
                                  item)
    {
        if (device_list !is null) {
            device_key = StrUtils.join("", sorted(device_list));
            return {top_level_key: {device_key: item}};
        } else {
            return {top_level_key: item};
        }
    }
    */

    void audit_all_objects(string mode="once", string[] device_dirs=null)
    {
        string description = "";
        if (device_dirs) {
            string device_dir_str = StrUtils.join(",", sorted(device_dirs));
            if (this.auditor_type == "ALL") {
                description = " - parallel, " ~ device_dir_str;
            } else {
                description = " - " ~ device_dir_str;
            }
        }
        this.logger.info("Begin object audit \"%s\" mode (%s%s)" %
                         (mode, this.auditor_type, description));
        begin = reported = time.time();
        this.total_bytes_processed = 0;
        this.total_files_processed = 0;
        int total_quarantines = 0;
        int total_errors = 0;
        int time_auditing = 0;
        // TODO: we should move audit-location generation to the storage policy,
        // as we may (conceivably) have a different filesystem layout for each.
        // We'd still need to generate the policies to audit from the actual
        // directories found on-disk, and have appropriate error reporting if we
        // find a directory that doesn't correspond to any known policy. This
        // will require a sizable refactor, but currently all diskfile managers
        // can find all diskfile locations regardless of policy -- so for now
        // just use Policy-0's manager.
        AuditLocation[] all_locs = (this.diskfile_router[POLICIES[0]]
                    .object_audit_location_generator(device_dirs));
        foreach (location; all_locs) {
            loop_time = time.time();
            this.failsafe_object_audit(location);
            this.logger.timing_since("timing", loop_time);
            this.files_running_time = SwiftUtils.ratelimit_sleep(
                this.files_running_time, this.max_files_per_second);
            ++this.total_files_processed;
            now = time.time();
            if (now - this.last_logged >= this.log_time) {
                /*
                this.logger.info(_(
                    "Object audit (%(type)s). "
                    "Since %(start_time)s: Locally: %(passes)d passed, " 
                    "%(quars)d quarantined, %(errors)d errors " 
                    "files/sec: %(frate).2f , bytes/sec: %(brate).2f, " 
                    "Total time: %(total).2f, Auditing time: %(audit).2f, " 
                    "Rate: %(audit_rate).2f") % {
                        "type": "%s%s" % (this.auditor_type, description),
                        "start_time": time.ctime(reported),
                        "passes": this.passes, "quars": this.quarantines,
                        "errors": this.errors,
                        "frate": this.passes / (now - reported),
                        "brate": this.bytes_processed / (now - reported),
                        "total": (now - begin), "audit": time_auditing,
                        "audit_rate": time_auditing / (now - begin)});
                cache_entry = this.create_recon_nested_dict(
                    "object_auditor_stats_%s" % (this.auditor_type),
                    device_dirs,
                    {"errors": this.errors, "passes": this.passes,
                     "quarantined": this.quarantines,
                     "bytes_processed": this.bytes_processed,
                     "start_time": reported, "audit_time": time_auditing})
                dump_recon_cache(cache_entry, this.rcache, this.logger);
                */
                reported = now;
                total_quarantines += this.quarantines;
                total_errors += this.errors;
                this.passes = 0;
                this.quarantines = 0;
                this.errors = 0;
                this.bytes_processed = 0;
                this.last_logged = now;
            }
            time_auditing += (now - loop_time);
        }
        // Avoid divide by zero during very short runs
        elapsed = time.time() - begin;
        if (elapsed < 0.000001) {
            elapsed = 0.000001;
        }
        /*
        this.logger.info(_(
            "Object audit (%(type)s) \"%(mode)s\" mode " 
            "completed: %(elapsed).02fs. Total quarantined: %(quars)d, " 
            "Total errors: %(errors)d, Total files/sec: %(frate).2f, " 
            "Total bytes/sec: %(brate).2f, Auditing time: %(audit).2f, " 
            "Rate: %(audit_rate).2f") % {
                "type": "%s%s" % (this.auditor_type, description),
                "mode": mode, "elapsed": elapsed,
                "quars": total_quarantines + this.quarantines,
                "errors": total_errors + this.errors,
                "frate": this.total_files_processed / elapsed,
                "brate": this.total_bytes_processed / elapsed,
                "audit": time_auditing, "audit_rate": time_auditing / elapsed})
        */
        if (this.stats_sizes) {
            this.logger.info(
                "Object audit stats: " ~ json.dumps(this.stats_buckets));
        }
    }

    /**
    Based on config's object_size_stats will keep track of how many objects
    fall into the specified ranges. For example with the following:

    object_size_stats = 10, 100, 1024

    and your system has 3 objects of sizes: 5, 20, and 10000 bytes the log
    will look like: {"10": 1, "100": 1, "1024": 0, "OVER": 1}
    */
    void record_stats(int obj_size)
    {
        bool recorded = false;
        foreach (stat_size; this.stats_sizes) {
            if (obj_size <= stat_size) {
                ++this.stats_buckets[stat_size];
                recorded = true;
                break;
            }
        }

        if (!recorded) {
            ++this.stats_buckets["OVER"];
        }
    }

    /**
    Entrypoint to object_audit, with a failsafe generic exception handler.
    */
    void failsafe_object_audit(AuditLocation location)
    {
        try {
            this.object_audit(location);
        } catch (Exception e) { //, Timeout) {
            this.logger.increment("errors");
            ++this.errors;
            this.logger.exception("ERROR Trying to audit " ~ location);
        }
    }

    void onQuaratine(string quarantine_reason) {
        throw new DiskFileQuarantine(quarantine_reason);
    }


    /**
    Audits the given object location.

    :param location: an audit location
                     (from diskfile.object_audit_location_generator)
    */
    void object_audit(AuditLocation location)
    {
        DiskFileManager diskfile_mgr = this.diskfile_router[location.policy];
        try {
            DiskFile df = diskfile_mgr.get_diskfile_from_audit_location(location);
            try {
                df.open();
                FileMetaData metadata = df.get_metadata();
                int obj_size = to!int(metadata["Content-Length"]);
                if (this.stats_sizes) {
                    this.record_stats(obj_size);
                }
                if (this.zero_byte_only_at_fps && obj_size > 0) {
                    ++this.passes;
                    return;
                }
                reader = df.reader(_quarantine_hook=this);
                foreach (chunk; reader) {
                    int chunk_len = chunk.length;
                    this.bytes_running_time = SwiftUtils.ratelimit_sleep(
                        this.bytes_running_time,
                        this.max_bytes_per_second,
                        incr_by=chunk_len);
                    this.bytes_processed += chunk_len;
                    this.total_bytes_processed += chunk_len;
                }
            } finally {
                //TODO: close reader
                //TODO: close df (if not closed by reader)
            }
        } catch (DiskFileNotExist dfne) {
            return;
        } catch (DiskFileQuarantined err) {
            ++this.quarantines;
            this.logger.error("ERROR Object " ~
                              location ~
                              " failed audit and was" ~
                              " quarantined: " ~
                              err);
        }
        ++this.passes;
    }
}


