module Daemon;


import core.sys.posix.signal;
import std.conv;

import Config;
import ConfigParser;
import Logger;
import Utils;


interface Daemon {
    void load_config(ConfigParser conf);
    void run_once();
    void run_forever();
}


abstract class AbstractDaemon : Daemon {

private:
    Config conf;
    Logger logger;

public:
    this(Config conf) {
        this.conf = conf;
        this.logger = Utils.Utils.get_logger(conf, "daemon");
    }

    static void kill_children() {
        //signal.signal(signal.SIGTERM, signal.SIG_IGN);                  
        //os.killpg(0, signal.SIGTERM);                                   
        //sys.exit();     
    }

    void run(bool once) {
        //Run the daemon
        Utils.Utils.validate_configuration();
        Utils.Utils.drop_privileges(this.conf.get("user", "swift"));
        Utils.Utils.capture_stdio(this.logger);

        signal.signal(signal.SIGTERM, kill_children);

        if (once) {
            this.run_once();
        } else {
            this.run_forever();
        }
    }
}


static void run_daemon(string klass_name,
                       string conf_file,
                       string section_name="",
                       bool once=false) {
    /*
    Loads settings from conf, then instantiates daemon "klass" and runs the
    daemon with the specified once kwarg.  The section_name will be derived
    from the daemon "klass" if not provided (e.g. ObjectReplicator =>
    object-replicator).

    :param klass: Class to instantiate, subclass of common.daemon.Daemon
    :param conf_file: Path to configuration file
    :param section_name: Section name from conf file to load config from
    :param once: Passed to daemon run method
    */
    // very often the config section_name is based on the class name
    // the None singleton will be passed through to readconf as is
    if (section_name is null || section_name == "") {
        section_name = sub("([a-z])([A-Z])", "\1-\2",
                           class_name).lower();
    }

    ConfigParser conf =
        Utils.Utils.readconf(conf_file,
                             section_name,
                             log_name=kwargs.get("log_name"));

    // once on command line (i.e. daemonize=false) will over-ride config
    if (!once) {
        bool daemonize = Utils.Utils.config_true_value(conf.get("daemonize", "true"));
        if (!daemonize) {
            once = true;
        }
    }

    // pre-configure logger
    Logger logger;

    if ("logger" in kwargs) {
        logger = kwargs.pop("logger");
    } else {
        logger =
            Utils.Utils.get_logger(conf,
                                   conf.get("log_name",
                                            section_name),
                                   log_to_console=kwargs.pop("verbose", false),
                                   log_route=section_name);
    }

    // disable fallocate if desired
    if (Utils.Utils.config_true_value(conf.get("disable_fallocate", "no"))) {
        Utils.Utils.disable_fallocate();
    }

    // set utils.FALLOCATE_RESERVE if desired
    int reserve = to!int(conf.get("fallocate_reserve", "0"));
    if (reserve > 0) {
        Utils.FALLOCATE_RESERVE = reserve;
    }

    // By default, disable eventlet printing stacktraces
    //eventlet_debug = utils.config_true_value(conf.get("eventlet_debug", "no"));
    //eventlet.debug.hub_exceptions(eventlet_debug)

    // Ensure TZ environment variable exists to avoid stat('/etc/localtime') on
    // some platforms. This locks in reported times to the timezone in which
    // the server first starts running in locations that periodically change
    // timezones.
    os.environ["TZ"] = time.strftime("%z", time.gmtime());

    Daemon daemon = cast(Daemon)Object.factory(klass_name);

    try {
        daemon.load_config(conf);
        daemon.run(once);
    } catch (KeyboardInterrupt e) {
        logger.info("User quit");
    }

    logger.info("Exited");
}

