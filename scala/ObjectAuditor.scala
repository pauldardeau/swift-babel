import java.util.ArrayList;
import java.util.HashMap;


class ObjectAuditor extends Daemon
{
    //Audit objects.
    private Config conf;
    private Logger logger;
    private String devices;
    private Int concurrency;
    private Int conf_zero_byte_fps;
    private String recon_cache_path;
    private String rcache;
    private Int interval;


    def ObjectAuditor(conf: Config)
    {
        this.conf = conf;
        this.logger = get_logger(conf, log_route="object-auditor");
        this.devices = conf.get("devices", "/srv/node");
        this.concurrency = Integer.parseInt(conf.get("concurrency", 1));
        this.conf_zero_byte_fps = Integer.parseInt(
            conf.get("zero_byte_files_per_second", 50));
        this.recon_cache_path = conf.get("recon_cache_path",
                                         "/var/cache/swift");
        this.rcache = os.path.join(this.recon_cache_path, "object.recon");
        this.interval = Integer.parseInt(conf.get("interval", 30));
    }

    def _sleep() {
        time.sleep(this.interval);
    }

    /*
    def clear_recon_cache(auditor_type: String)
    {
        //Clear recon cache entries
        dump_recon_cache({"object_auditor_stats_%s" % auditor_type: {}},
                         this.rcache, this.logger);
    }
    */

    def run_audit(HashMap kwargs)
    {
        //Run the object audit
        String mode = kwargs.get("mode");
        zero_byte_only_at_fps = kwargs.get("zero_byte_fps", 0);
        String device_dirs = kwargs.get("device_dirs");
        AuditorWorker worker;
        worker = new AuditorWorker(this.conf,
                                   this.logger,
                                   this.rcache,
                                   this.devices,
                                   zero_byte_only_at_fps);
        worker.audit_all_objects(mode, device_dirs);
    }

    def fork_child(zero_byte_fps: Boolean) : Int = {
        //Child execution
        int pid = os.fork();
        if (pid > 0) {
            return pid;
        } else {
            signal.signal(signal.SIGTERM, signal.SIG_DFL);
            HashMap kwargs = new HashMap();
            if (zero_byte_fps) {
                kwargs.put("zero_byte_fps", this.conf_zero_byte_fps);
            }

            try {
                this.run_audit(kwargs);
            } catch (Exception e) {
                this.logger.exception(
                    "ERROR: Unable to run auditing: " + e);
            } finally {
                System.exit();
            }
        }
    }

    def audit_loop(parent: Boolean,
                   zbo_fps: Int,
                           ArrayList<String> override_devices, //=null,
                           HashMap kwargs)
    {
        //Parallel audit loop
        this.clear_recon_cache("ALL");
        this.clear_recon_cache("ZBF");
        kwargs.put("device_dirs", override_devices);
        if (parent) {
            kwargs.put("zero_byte_fps", zbo_fps);
            this.run_audit(kwargs);
        } else {
            ArrayList<Integer> pids = new ArrayList<>();
            if (this.conf_zero_byte_fps) {
                int zbf_pid = this.fork_child(zero_byte_fps=true, kwargs);
                pids.add(zbf_pid);
            }
            if (this.concurrency == 1) {
                // Audit all devices in 1 process
                pids.add(this.fork_child(kwargs));
            } else {
                // Divide devices amongst parallel processes set by
                // self.concurrency.  Total number of parallel processes
                // is self.concurrency + 1 if zero_byte_fps.
                int parallel_proc = this.concurrency;
                if (this.conf_zero_byte_fps) {
                    ++parallel_proc;
                }
                /*
                device_list = list(override_devices) if override_devices else
                    OSUtils.listdir(this.devices);
                    */
                shuffle(device_list);
                while (!device_list.isEmpty()) {
                    String pid = null;
                    if (pids.size() == parallel_proc) {
                        pid = os.wait()[0];
                        pids.remove(pid);
                    }
                    // ZBF scanner must be restarted as soon as it finishes
                    if (this.conf_zero_byte_fps && pid == zbf_pid) {
                        kwargs.put("device_dirs", override_devices);
                        // sleep between ZBF scanner forks
                        this._sleep();
                        zbf_pid = this.fork_child(zero_byte_fps=true,
                                                  kwargs);
                        pids.add(zbf_pid);
                    } else {
                        /*
                        kwargs.put("device_dirs", [device_list.pop()]);
                        */
                        pids.add(this.fork_child(kwargs));
                    }
                }
            }

            while (!pids.isEmpty()) {
                int pid = os.wait()[0];
                // ZBF scanner must be restarted as soon as it finishes
                if (this.conf_zero_byte_fps && pid == zbf_pid &&
                   pids.size() > 1) {
                    kwargs.put("device_dirs", override_devices);
                    // sleep between ZBF scanner forks
                    this._sleep();
                    int zbf_pid = this.fork_child(zero_byte_fps=true, kwargs);
                    pids.add(zbf_pid);
                }
                pids.remove(pid);
            }
        }
    }

    def run_forever(kwargs: HashMap)
    {
        //Run the object audit until stopped.
        // zero byte only command line option
        int zbo_fps = kwargs.get("zero_byte_fps", 0);
        Boolean parent = false;
        if (zbo_fps > 0) {
            // only start parent
            parent = true;
        }

        kwargs = new HashMap();
        kwargs.put("mode", "forever");

        while (true) {
            try {
                this.audit_loop(parent, zbo_fps, kwargs);
            } catch (Timeout err) {
                this.logger.exception("ERROR auditing: " + err);
            }
            this._sleep();
        }
    }

    def run_once(kwargs: HashMap)
    {
        //Run the object audit once
        // zero byte only command line option
        int zbo_fps = kwargs.get("zero_byte_fps", 0);
        ArrayList<String> override_devices = list_from_csv(kwargs.get("devices"));
        // Remove bogus entries and duplicates from override_devices
        override_devices = list(
            set(OSUtils.listdir(this.devices)).intersection(set(override_devices)));
        Boolean parent = false;
        if (zbo_fps > 0) {
            // only start parent
            parent = true;
        }

        kwargs = new HashMap();
        kwargs.put("mode", "once");

        try {
            this.audit_loop(parent, zbo_fps, override_devices);
        } catch (Exception err) { //, Timeout) as err:
            this.logger.exception("ERROR auditing: " + err);
        }
    }

    def static void main(String[] args) {
    }
}

