import java.util.ArrayList;
import java.util.HashMap;


public class AuditorWorker implements QuarantineHook, DiskFileReadHook
{
    //Walk through file system to audit objects

    private Conf conf;
    private Logger logger;
    private String devices;
    private boolean zero_byte_only_at_fps;
    private float max_files_per_second;
    private float max_bytes_per_second;
    private String auditor_type;
    private int bytes_running_time;
    private int files_running_time;
    private long last_logged;
    private int log_time;
    private int bytes_processed;
    private int total_bytes_processed;
    private int total_files_processed;
    private int passes;
    private int quarantines;
    private int errors;
    private StatBuckets stats_buckets;
    private DiskFileRouter diskfile_router;
    private String rcache;


    public AuditorWorker(Config conf,
                         Logger logger,
                         String rcache,
                         String devices) {
        this(conf, logger, rcache, devices, false);
    }

    public AuditorWorker(Config conf,
                         Logger logger,
                         String rcache,
                         String devices,
                         boolean zero_byte_only_at_fps)
    {
        this.conf = conf;
        this.logger = logger;
        this.devices = devices;
        this.diskfile_router = new DiskFileRouter(conf, this.logger);
        this.max_files_per_second = Float.parseFloat(conf.get("files_per_second", "20"));
        this.max_bytes_per_second = Float.parseFloat(conf.get("bytes_per_second",
                                                   "10000000"));
        this.auditor_type = "ALL";
        this.zero_byte_only_at_fps = zero_byte_only_at_fps;
        if (this.zero_byte_only_at_fps) {
            this.max_files_per_second = Float.parseFloat(this.zero_byte_only_at_fps);
            this.auditor_type = "ZBF";
        }
        this.log_time = Integer.parseInt(conf.get("log_time", "3600"));
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
        this.stats_buckets = new StatBuckets();
    }

    /*
    public void create_recon_nested_dict(top_level_key, device_list, item)
    {
        if (device_list != null) {
            device_key = ''.join(sorted(device_list))
            return {top_level_key: {device_key: item}}
        } else {
            return {top_level_key: item}
        }
    }
    */

    //public void audit_all_objects(String mode="once", device_dirs=null)
    public void audit_all_objects(String mode, ArrayList<String> device_dirs) {
        String description = "";
        if (device_dirs != null) {
            String device_dir_str = ','.join(sorted(device_dirs));
            if (this.auditor_type.equals("ALL")) {
                description = " - parallel, " + device_dir_str;
            } else {
                description = " - " + device_dir_str;
            }
        }
        this.logger.info("Begin object audit \"" + mode +"\" mode (" +
                         this.auditor_type + description + ")");
        //begin = reported = Time.time();
        long begin = System.currentTimeMillis();
        long reported = begin;
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
        all_locs = (this.diskfile_router[POLICIES[0]]
                    .object_audit_location_generator(device_dirs));
        for (AuditLocation location : all_locs) {
            long loop_time = System.currentTimeMillis();
            this.failsafe_object_audit(location);
            this.logger.timing_since("timing", loop_time);
            this.files_running_time = SwiftUtils.ratelimit_sleep(
                this.files_running_time, this.max_files_per_second);
            this.total_files_processed += 1;
            long now = System.currentTimeMillis();
            if (now - this.last_logged >= this.log_time) {
                /*
                this.logger.info(_(
                    'Object audit (%(type)s). '
                    'Since %(start_time)s: Locally: %(passes)d passed, '
                    '%(quars)d quarantined, %(errors)d errors '
                    'files/sec: %(frate).2f , bytes/sec: %(brate).2f, '
                    'Total time: %(total).2f, Auditing time: %(audit).2f, '
                    'Rate: %(audit_rate).2f') % {
                        'type': '%s%s' % (this.auditor_type, description),
                        'start_time': time.ctime(reported),
                        'passes': this.passes, 'quars': this.quarantines,
                        'errors': this.errors,
                        'frate': this.passes / (now - reported),
                        'brate': this.bytes_processed / (now - reported),
                        'total': (now - begin), 'audit': time_auditing,
                        'audit_rate': time_auditing / (now - begin)})
                cache_entry = this.create_recon_nested_dict(
                    'object_auditor_stats_%s' % (this.auditor_type),
                    device_dirs,
                    {'errors': this.errors, 'passes': this.passes,
                     'quarantined': this.quarantines,
                     'bytes_processed': this.bytes_processed,
                     'start_time': reported, 'audit_time': time_auditing})
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
        long elapsed = System.currentTimeMillis() - begin;
        if (elapsed == 0) {
            elapsed = 1;
        }

        /*
        this.logger.info(_(
            'Object audit (%(type)s) "%(mode)s" mode '
            'completed: %(elapsed).02fs. Total quarantined: %(quars)d, '
            'Total errors: %(errors)d, Total files/sec: %(frate).2f, '
            'Total bytes/sec: %(brate).2f, Auditing time: %(audit).2f, '
            'Rate: %(audit_rate).2f') % {
                'type': '%s%s' % (this.auditor_type, description),
                'mode': mode, 'elapsed': elapsed,
                'quars': total_quarantines + this.quarantines,
                'errors': total_errors + this.errors,
                'frate': this.total_files_processed / elapsed,
                'brate': this.total_bytes_processed / elapsed,
                'audit': time_auditing, 'audit_rate': time_auditing / elapsed})
        */
        if (this.stats_sizes) {
            this.logger.info(
                "Object audit stats: " + this.stats_buckets.toString());
        }
    }

    /**
    Based on config's object_size_stats will keep track of how many objects
    fall into the specified ranges. For example with the following:

    object_size_stats = 10, 100, 1024

    and your system has 3 objects of sizes: 5, 20, and 10000 bytes the log
    will look like: {"10": 1, "100": 1, "1024": 0, "OVER": 1}
    */
    public void record_stats(int obj_size)
    {
        boolean stats_updated = false;
        for (int size : this.stats_sizes) {
            if (obj_size <= size) {
                this.stats_buckets.increment(size);
                stats_updated = true;
                break;
            }
        }

        if (!stats_updated) {
            this.stats_buckets.increment_over();
        }
    }

    /**
    Entrypoint to object_audit, with a failsafe generic exception handler.
    */
    public void failsafe_object_audit(AuditLocation location)
    {
        try {
            this.object_audit(location);
        } catch (Exception e) { //, Timeout):
            this.logger.increment("errors");
            this.errors += 1;
            this.logger.exception("ERROR Trying to audit " +
                                  location.toString());
        }
    }

    public void onQuarantine(String reason) {
        throw new DiskFileQuarantined(reason);
    }

    public void onFileRead(byte[] chunk) {
        int chunk_len = chunk.length;
        this.bytes_running_time =
            SwiftUtils.ratelimit_sleep(
                        this.bytes_running_time,
                        this.max_bytes_per_second,
                        incr_by=chunk_len);
        this.bytes_processed += chunk_len;
        this.total_bytes_processed += chunk_len;
    }

    /**
    Audits the given object location.

    @param location an audit location
                     (from diskfile.object_audit_location_generator)
    */
    public void object_audit(AuditLocation location) {
        DiskFileManager diskfile_mgr = this.diskfile_router[location.policy];
        try {
            DiskFile df = diskfile_mgr.get_diskfile_from_audit_location(location);
            DiskFileReader reader = null;
            try {
                df.open();
                metadata = df.get_metadata();
                obj_size = Integer.parseInt(metadata["Content-Length"]);
                if (this.stats_sizes) {
                    this.record_stats(obj_size);
                }
                if (this.zero_byte_only_at_fps && obj_size > 0) {
                    this.passes += 1;
                    return;
                }
                reader = df.reader(this);
            } finally {
                df.close();
            }

            try {
                reader.read(this);
            } finally {
                if (reader != null) {
                    reader.close();
                }
            }
        } catch (DiskFileNotExist dfne) {
            return;
        } catch (DiskFileQuarantined err) {
            this.quarantines += 1;
            this.logger.error("ERROR Object " + location.toString() +
                              " failed audit and was quarantined: " +
                              err.toString());
        }
        this.passes += 1;
    }
}


