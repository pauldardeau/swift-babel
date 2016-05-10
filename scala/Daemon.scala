

class Daemon {
    /*Daemon base class*/

    private static Logger logger;

    private static ConfigParser conf;
    //private Logger logger;


    def Daemon(ConfigParser conf) {
        this.conf = conf;
        String log_route = "daemon";
        logger = Utils.get_logger(conf, log_route);
    }

    /**
     * Run the script once
     */
    def abstract run_once();

    /**
     * Run forever
     */
    def abstract run_forever();

    def run() {
        run(false);
    }

    def run(once: Boolean) {
        //Run the daemon
        Utils.validate_configuration();
        Utils.drop_privileges(this.conf.get("user", "swift"));
        Utils.capture_stdio(this.logger);

        /*
        def kill_children(args):
            signal.signal(signal.SIGTERM, signal.SIG_IGN)
            os.killpg(0, signal.SIGTERM)
            System.exit();

        signal.signal(signal.SIGTERM, kill_children);
        */

        if (once) {
            this.run_once();
        } else {
            this.run_forever();
        }
    }

    def static run_daemon(Class klass,
                                  conf_file: String,
                                  section_name: String,
                                  once: Boolean) {
        /**
        Loads settings from conf, then instantiates daemon "klass" and runs the
        daemon with the specified once kwarg.  The section_name will be derived
        from the daemon "klass" if not provided (e.g. ObjectReplicator =>
        object-replicator).

        @param klass Class to instantiate, subclass of common.daemon.Daemon
        @param conf_file Path to configuration file
        @param section_name Section name from conf file to load config from
        @param once Passed to daemon run method
        */

        // very often the config section_name is based on the class name
        // the null singleton will be passed through to readconf as is
        if (section_name == null || section_name.length() == 0) {
            section_name = section_name.toLowerCase();
            /*
            section_name = sub(r'([a-z])([A-Z])', r'\1-\2',
                               klass.__name__).lower()
                               */
        }

        String log_name = "";  //kwargs.get("log_name");

        conf = Utils.readconf(conf_file,
                              section_name,
                              log_name);

        // once on command line (i.e. daemonize=false) will over-ride config
        Boolean daemonize = Utils.config_true_value(conf.get("daemonize", "true"));
        if (!once) {
            once = !daemonize;
        }

        // pre-configure logger
        /*
        if ("logger" in kwargs) {
            logger = kwargs.pop("logger");
        } else {
            */
            Boolean log_to_console = false; //kwargs.pop("verbose", false);
            String log_route = section_name;
            logger = Utils.get_logger(conf,
                                      conf.get("log_name", section_name),
                                      log_to_console,
                                      log_route);
        //}

        // disable fallocate if desired
        if (Utils.config_true_value(conf.get("disable_fallocate", "no"))) {
            Utils.disable_fallocate();
        }
        // set utils.FALLOCATE_RESERVE if desired
        int reserve = Integer.parseInt(conf.get("fallocate_reserve", "0"));
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
        //os.environ["TZ"] = time.strftime("%z", time.gmtime())

        try {
            Daemon daemon = klass.newInstance(conf);
            daemon.run(once);
        } catch (Exception e) { //KeyboardInterrupt) {
            logger.info("User quit");
        } finally {
            logger.info("Exited");
        }
    }
}

