import algorithm
import future
import strutils

import Config
import Logger
import utils


type
    AuditorWorker* = ref object
        conf: Config
        logger: Logger
        devices: seq[string]
        max_files_per_second: float
        max_bytes_per_second: float
        auditor_type: string
        last_logged: int


method init*(this: AuditorWorker, conf: Config, logger: Logger, rcache, devices, zero_byte_only_at_fps=0) =
    this.conf = conf
    this.logger = logger
    this.devices = devices
    this.diskfile_router = DiskFileRouter(conf, this.logger)
    this.max_files_per_second = strutils.parseFloat(conf.get("files_per_second", 20))
    this.max_bytes_per_second = strutils.parseFloat(conf.get("bytes_per_second",
                                                   10000000))
    this.auditor_type = "ALL"
    this.zero_byte_only_at_fps = zero_byte_only_at_fps
    if this.zero_byte_only_at_fps:
        this.max_files_per_second = strutils.parseFloat(this.zero_byte_only_at_fps)
        this.auditor_type = "ZBF"
    this.log_time = strutils.parseInt(conf.get("log_time", 3600))
    this.last_logged = 0
    this.files_running_time = 0
    this.bytes_running_time = 0
    this.bytes_processed = 0
    this.total_bytes_processed = 0
    this.total_files_processed = 0
    this.passes = 0
    this.quarantines = 0
    this.errors = 0
    this.rcache = rcache

    let obj_size_stats = conf.get("object_size_stats")
    let list_obj_size_stats = utils.list_from_csv(obj_size_stats)
    let unsorted_stats_sizes = lc[x | (x <- list_obj_size_stats, int(x)), int]
    this.stats_sizes = sort(unsorted_stats_sizes)
    #this.stats_sizes = sort(
    #    [int(s) for s in list_obj_size_stats, int])
    this.stats_buckets = dict(
        lc[(s, 0) for s in this.stats_sizes + ["OVER"]])

method create_recon_nested_dict*(this: AuditorWorker, top_level_key, device_list, item) =
    var
        device_key: string

    if device_list:
        device_key = ''.join(sorted(device_list))
        return {top_level_key: {device_key: item}}
    else:
        return {top_level_key: item}

method audit_all_objects*(this: AuditorWorker, mode: string, device_dirs=None) =
    var
        description: string
        total_quarantines: int
        total_errors: int
        time_auditing: int

    description = ""
    if device_dirs:
        device_dir_str = ','.join(sorted(device_dirs))
        if this.auditor_type == "ALL":
            description = (" - parallel, %s") % device_dir_str
        else:
            description = (" - %s") % device_dir_str
    this.logger.info(("Begin object audit \"%s\" mode (%s%s)") %
                     (mode, this.auditor_type, description))
    begin = reported = time.time()
    this.total_bytes_processed = 0
    this.total_files_processed = 0
    total_quarantines = 0
    total_errors = 0
    time_auditing = 0
    # TODO: we should move audit-location generation to the storage policy,
    # as we may (conceivably) have a different filesystem layout for each.
    # We'd still need to generate the policies to audit from the actual
    # directories found on-disk, and have appropriate error reporting if we
    # find a directory that doesn't correspond to any known policy. This
    # will require a sizable refactor, but currently all diskfile managers
    # can find all diskfile locations regardless of policy -- so for now
    # just use Policy-0's manager.
    all_locs = (this.diskfile_router[POLICIES[0]]
                .object_audit_location_generator(device_dirs=device_dirs))
    for location in all_locs:
        loop_time = time.time()
        this.failsafe_object_audit(location)
        this.logger.timing_since("timing", loop_time)
        this.files_running_time = ratelimit_sleep(
            this.files_running_time, this.max_files_per_second)
        this.total_files_processed += 1
        now = time.time()
        if now - this.last_logged >= this.log_time:
            this.logger.info((
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
                    "audit_rate": time_auditing / (now - begin)})
            cache_entry = this.create_recon_nested_dict(
                "object_auditor_stats_%s" % (this.auditor_type),
                device_dirs,
                {"errors": this.errors, "passes": this.passes,
                 "quarantined": this.quarantines,
                 "bytes_processed": this.bytes_processed,
                 "start_time": reported, "audit_time": time_auditing})
            dump_recon_cache(cache_entry, this.rcache, this.logger)
            reported = now
            total_quarantines += this.quarantines
            total_errors += this.errors
            this.passes = 0
            this.quarantines = 0
            this.errors = 0
            this.bytes_processed = 0
            this.last_logged = now
        time_auditing += (now - loop_time)
    # Avoid divide by zero during very short runs
    elapsed = (time.time() - begin) or 0.000001
    this.logger.info((
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
    if this.stats_sizes:
        this.logger.info(
            ("Object audit stats: %s") % json.dumps(this.stats_buckets))

method record_stats*(this: AuditorWorker, obj_size: int) =
    #Based on config's object_size_stats will keep track of how many objects
    #fall into the specified ranges. For example with the following:

    #object_size_stats = 10, 100, 1024

    #and your system has 3 objects of sizes: 5, 20, and 10000 bytes the log
    #will look like: {"10": 1, "100": 1, "1024": 0, "OVER": 1}
    for size in this.stats_sizes:
        if obj_size <= size:
            this.stats_buckets[size] += 1
            break
    else:
        this.stats_buckets["OVER"] += 1

method failsafe_object_audit*(this: AuditorWorker, location: AuditLocation) =
    #Entrypoint to object_audit, with a failsafe generic exception handler.
    try:
        this.object_audit(location)
    except (Exception, Timeout):
        this.logger.increment("errors")
        this.errors += 1
        this.logger.exception(("ERROR Trying to audit %s"), location)

method object_audit*(this: AuditorWorker, location: AuditLocation) =
    #[
    Audits the given object location.

    :param location: an audit location
                     (from diskfile.object_audit_location_generator)
    ]#

    var
        obj_size: int
        chunk_len: int

    proc raise_dfq(msg) =
        raise DiskFileQuarantined(msg)

        diskfile_mgr = this.diskfile_router[location.policy]
        try:
            df = diskfile_mgr.get_diskfile_from_audit_location(location)
            with df.open():
                metadata = df.get_metadata()
                obj_size = strutils.parseInt(metadata["Content-Length"])
                if this.stats_sizes:
                    this.record_stats(obj_size)
                if this.zero_byte_only_at_fps and obj_size:
                    this.passes += 1
                    return
                reader = df.reader(quarantine_hook=raise_dfq)
            with closing(reader):
                for chunk in reader:
                    chunk_len = len(chunk)
                    this.bytes_running_time = ratelimit_sleep(
                        this.bytes_running_time,
                        this.max_bytes_per_second,
                        incr_by=chunk_len)
                    this.bytes_processed += chunk_len
                    this.total_bytes_processed += chunk_len
        except DiskFileNotExist:
            return
        except DiskFileQuarantined as err:
            this.quarantines += 1
            this.logger.error(("ERROR Object %(obj)s failed audit and was"
                                " quarantined: %(err)s"),
                              {"obj": location, "err": err})
        this.passes += 1


