module ObjectAuditor;

import std.process;

import AuditorWorker;
import Config;
import Daemon;
import Logger;
import OSUtils;
import SwiftUtils;
import Utils;


/**
 Audit objects
 */
class ObjectAuditor : Daemon
{

private:
    string devices;
    int concurrency;
    int conf_zero_byte_fps;
    string recon_cache_path;
    string rcache;
    int interval;
    Logger logger;
    Config conf;


protected:
    void _sleep() {
        time.sleep(this.interval);
    }


public:
    this(Config conf)
    {
        this.conf = conf;
        this.logger = Utils.get_logger(conf, log_route="object-auditor");
        this.devices = conf.get("devices", "/srv/node");
        this.concurrency = to!int(conf.get("concurrency", 1));
        this.conf_zero_byte_fps = to!int(
            conf.get("zero_byte_files_per_second", 50));
        this.recon_cache_path = conf.get("recon_cache_path",
                                         "/var/cache/swift");
        this.rcache = OSUtils.path_join(this.recon_cache_path, "object.recon");
        this.interval = to!int("interval", 30);
    }

    void clear_recon_cache(string auditor_type)
    {
        //Clear recon cache entries
        /*
        dump_recon_cache({"object_auditor_stats_%s" % auditor_type: {}},
                         this.rcache, this.logger);
        */
    }

    void run_audit(string mode,
                   string[] device_dirs,
                   int zero_byte_only_at_fps)
    {
        //Run the object audit
        //zero_byte_only_at_fps = kwargs.get("zero_byte_fps", 0);
        //device_dirs = kwargs.get("device_dirs");
        worker = new AuditorWorker(this.conf,
                                   this.logger,
                                   this.rcache,
                                   this.devices,
                                   zero_byte_only_at_fps);
        worker.audit_all_objects(mode, device_dirs);
    }

    int fork_child(bool zero_byte_fps=false)
    {
        //Child execution
        pid = OSUtils.fork();
        if (pid) {
            return pid;
        } else {
            string mode = ""; //TODO: PJD assign correct value
            string[] device_dirs; //TODO: PJD assign correct value
            int zero_byte_fps = 0;

            if (zero_byte_fps) {
                zero_byte_fps = this.conf_zero_byte_fps;
            }

            signal.signal(signal.SIGTERM, signal.SIG_DFL);

            try {
                this.run_audit(mode, device_dirs, zero_byte_fps);
            } catch(Exception e) {
                this.logger.exception(
                    "ERROR: Unable to run auditing: " ~ e);
            } finally {
                System.exit();
            }
        }
    }

    void audit_loop(bool parent, int zbo_fps, string[] override_devices=null)
    {
        //Parallel audit loop
        this.clear_recon_cache("ALL");
        this.clear_recon_cache("ZBF");
        kwargs["device_dirs"] = override_devices;
        if (parent) {
            kwargs["zero_byte_fps"] = zbo_fps;
            this.run_audit(kwargs);
        } else {
            int[] pids;
            if (this.conf_zero_byte_fps) {
                zbf_pid = this.fork_child(zero_byte_fps=true, kwargs);
                pids ~= zbf_pid;
            }
            if (this.concurrency == 1) {
                // Audit all devices in 1 process
                pids ~= this.fork_child(kwargs);
            } else {
                // Divide devices amongst parallel processes set by
                // this.concurrency.  Total number of parallel processes
                // is this.concurrency + 1 if zero_byte_fps.
                int parallel_proc = this.concurrency;
                if (this.conf_zero_byte_fps) {
                    ++parallel_proc;
                }

                string[] device_list;

                if (override_devices is null) {
                    device_list = listdir(this.devices);
                } else {
                    device_list = override_devices;
                }

/*
                device_list = list(override_devices) if override_devices else \
                    listdir(this.devices)
                    */

                shuffle(device_list);
                while (device_list) {
                    pid = None;
                    if (pids.length == parallel_proc) {
                        pid = OSUtils.wait();
                        pids.remove(pid);
                    }
                    // ZBF scanner must be restarted as soon as it finishes
                    if (this.conf_zero_byte_fps && pid == zbf_pid) {
                        kwargs["device_dirs"] = override_devices;
                        // sleep between ZBF scanner forks
                        this._sleep();
                        zbf_pid = this.fork_child(zero_byte_fps=true,
                                                  kwargs);
                        pids ~= zbf_pid;
                    } else {
                        kwargs["device_dirs"] = [device_list.pop()];
                        pids ~= this.fork_child(kwargs);
                    }
                }
            }
            while (pids.length > 0) {
                pid = OSUtils.wait();
                // ZBF scanner must be restarted as soon as it finishes
                if (this.conf_zero_byte_fps && pid == zbf_pid &&
                   pids.length > 1) {
                    kwargs["device_dirs"] = override_devices;
                    // sleep between ZBF scanner forks
                    this._sleep();
                    zbf_pid = this.fork_child(zero_byte_fps=true, kwargs);
                    pids ~= zbf_pid;
                }
                pids.remove(pid);
            }
        }
    }

    void run_forever()
    {
        //Run the object audit until stopped.
        // zero byte only command line option
        int zbo_fps = kwargs.get("zero_byte_fps", 0);
        bool parent = false;
        if (zbo_fps > 0) {
            // only start parent
            parent = true;
        }
        /*
        kwargs = {"mode": "forever"}
        */

        while (true) {
            try {
                this.audit_loop(parent, zbo_fps);
            } catch (Exception e) { //, Timeout) as err {
                this.logger.exception("ERROR auditing: " ~ e.toString());
            }
            this._sleep();
        }
    }

    void run_once()
    {
        //Run the object audit once
        // zero byte only command line option
        int zbo_fps = kwargs.get("zero_byte_fps", 0);
        override_devices = SwiftUtils.list_from_csv(kwargs.get("devices"));
        // Remove bogus entries and duplicates from override_devices
        override_devices = list(
            set(listdir(this.devices)).intersection(set(override_devices)));
        bool parent = false;
        if (zbo_fps) {
            // only start parent
            parent = true;
        }
        /*
        kwargs = {"mode": "once"}
        */

        try {
            this.audit_loop(parent,
                            zbo_fps,
                            override_devices);
        } catch (Exception e) { //, Timeout) as err) {
            this.logger.exception("ERROR auditing: " ~ err.toString());
        }
    }
}

