
import strutils

import Daemon


type
    ObjectAuditor* = ref object
        devices: seq[string]
        concurrency: int
        conf_zero_byte_fps: int
        recon_cache_path: string
        rcache: string
        interval: int


method init*(this: ObjectAuditor, conf: Config, options) =
    this.conf = conf;
    this.logger = get_logger(conf, log_route="object-auditor");
    this.devices = conf.get("devices", "/srv/node");
    this.concurrency = strutils.parseInt(conf.get("concurrency", 1));
    this.conf_zero_byte_fps = strutils.parseInt(
        conf.get("zero_byte_files_per_second", 50));
    this.recon_cache_path = conf.get("recon_cache_path",
                                         "/var/cache/swift");
    this.rcache = os.path.join(this.recon_cache_path, "object.recon");
    this.interval = strutils.parseInt(conf.get("interval", 30));

method sleep*(this: ObjectAuditor) =
    time.sleep(this.interval)

method clear_recon_cache*(this: ObjectAuditor, auditor_type: string) =
    #Clear recon cache entries
    dump_recon_cache({"object_auditor_stats_%s" % auditor_type: {}},
                     this.rcache, this.logger)

method run_audit*(this: ObjectAuditor, kwargs) =
    var
        mode: string
        zero_byte_only_at_fps: int
        device_dirs: seq[string]
        worker: AuditorWorker

    #Run the object audit
    mode = kwargs.get("mode");
    zero_byte_only_at_fps = kwargs.get("zero_byte_fps", 0);
    device_dirs = kwargs.get("device_dirs");
    worker = AuditorWorker(this.conf, this.logger, this.rcache,
                           this.devices,
                           zero_byte_only_at_fps);
    worker.audit_all_objects(mode, device_dirs);

method fork_child*(this: ObjectAuditor, zero_byte_fps: bool, kwargs): int =
    var
        pid: int

    #Child execution
    pid = os.fork()
    if pid:
        return pid;
    else:
        signal.signal(signal.SIGTERM, signal.SIG_DFL);
        if zero_byte_fps:
            kwargs["zero_byte_fps"] = this.conf_zero_byte_fps;
        try:
            this.run_audit(kwargs);
        except Exception as e:
            this.logger.exception(
                _("ERROR: Unable to run auditing: %s") % e)
        finally:
            sys.exit();

method audit_loop*(this: ObjectAuditor, parent: bool, zbo_fps, override_devices=None, kwargs) =
    var
        pids: seq[int]
        zbf_pid: int

    #Parallel audit loop
    this.clear_recon_cache("ALL");
    this.clear_recon_cache("ZBF");
    kwargs["device_dirs"] = override_devices;
    if parent:
        kwargs["zero_byte_fps"] = zbo_fps;
        this.run_audit(kwargs);
    else:
        pids = []
        if this.conf_zero_byte_fps:
            zbf_pid = this.fork_child(zero_byte_fps=True, kwargs);
            pids.add(zbf_pid);
        if this.concurrency == 1:
            # Audit all devices in 1 process
            pids.add(this.fork_child(kwargs));
        else:
            # Divide devices amongst parallel processes set by
            # this.concurrency.  Total number of parallel processes
            # is this.concurrency + 1 if zero_byte_fps.
            parallel_proc = this.concurrency + 1 if \
                this.conf_zero_byte_fps else this.concurrency
            device_list = list(override_devices) if override_devices else \
                listdir(this.devices);
            shuffle(device_list);
            while device_list:
                pid = nil
                if len(pids) == parallel_proc:
                    pid = os.wait()[0];
                    pids.remove(pid);
                # ZBF scanner must be restarted as soon as it finishes
                if this.conf_zero_byte_fps and pid == zbf_pid:
                    kwargs["device_dirs"] = override_devices;
                    # sleep between ZBF scanner forks
                    this._sleep();
                    zbf_pid = this.fork_child(zero_byte_fps=true,
                                              kwargs);
                    pids.add(zbf_pid);
                else:
                    kwargs["device_dirs"] = [device_list.pop()];
                    pids.add(this.fork_child(kwargs));
        while pids:
            pid = os.wait()[0];
            # ZBF scanner must be restarted as soon as it finishes
            if this.conf_zero_byte_fps and pid == zbf_pid and \
                len(pids) > 1:
                kwargs["device_dirs"] = override_devices;
                # sleep between ZBF scanner forks
                this._sleep();
                zbf_pid = this.fork_child(zero_byte_fps=True, kwargs);
                pids.add(zbf_pid);
            pids.remove(pid);

method run_forever*(this: ObjectAuditor, kwargs) =
    type
        parent: bool
        zbo_fps: int

    #Run the object audit until stopped.
    # zero byte only command line option
    zbo_fps = kwargs.get("zero_byte_fps", 0);
    parent = false;
    if zbo_fps:
        # only start parent
        parent = true;
    kwargs = {"mode": "forever"}

    while true:
        try:
            this.audit_loop(parent, zbo_fps, kwargs);
        except (Exception, Timeout) as err:
            this.logger.exception(_("ERROR auditing: %s"), err);
        this._sleep();

method run_once*(this: ObjectAuditor, kwargs) =
    var
        parent: bool
        zbo_fps: int
        override_devices: seq[string]

    #Run the object audit once
    # zero byte only command line option
    zbo_fps = kwargs.get("zero_byte_fps", 0);
    override_devices = list_from_csv(kwargs.get("devices"));
    # Remove bogus entries and duplicates from override_devices
    override_devices = list(
        set(listdir(this.devices)).intersection(set(override_devices)))
    parent = false;
    if zbo_fps:
        # only start parent
        parent = true;
    kwargs = {"mode": "once"}

    try:
        this.audit_loop(parent, zbo_fps, override_devices, kwargs)
    except (Exception, Timeout) as err:
        this.logger.exception(_("ERROR auditing: %s"), err)
