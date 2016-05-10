import posix;
import strutils;

import utils;
import Config;
import Logger;


type
    Daemon* = ref object
        conf: Config
        logger: Logger


proc Daemon_init*(daemon: Daemon, conf: Config) =
    daemon.conf = conf;
    daemon.logger = utils.get_logger(conf, log_route="daemon");

method run_once*(this: Daemon) {.base.} =
    #Override this to run the script once
    #raise NotImplementedError("run_once not implemented")
    raise newException(OSError, "run_once not implemented");

method run_forever*(this: Daemon) {.base.} =
    #Override this to run forever
    #raise NotImplementedError("run_forever not implemented")
    raise newException(OSError, "run_forever not implemented");

proc kill_children(sig: cint) {.noconv.} =
    var
        ignore: cint

    ignore = posix.sigignore(SIGTERM);
    ignore = posix.killpg(0, SIGTERM);
    quit(QuitSuccess);

proc run(daemon: Daemon, once: bool) =
    #Run the daemon
    utils.validate_configuration();
    utils.drop_privileges(daemon.conf.get("user", "swift"));
    utils.capture_stdio(daemon.logger);

    posix.signal(SIGTERM, kill_children)
    if once:
        daemon.run_once();
    else:
        daemon.run_forever();


proc run_daemon(klass: int, conf_file: string, section_name: string, log_name: string, once: bool) =
    var
        conf: Config
        run_once: bool

    #Loads settings from conf, then instantiates daemon "klass" and runs the
    #daemon with the specified once kwarg.  The section_name will be derived
    #from the daemon "klass" if not provided (e.g. ObjectReplicator =>
    #object-replicator).

    #:param klass: Class to instantiate, subclass of common.daemon.Daemon
    #:param conf_file: Path to configuration file
    #:param section_name: Section name from conf file to load config from
    #:param once: Passed to daemon run method

    # very often the config section_name is based on the class name
    # the None singleton will be passed through to readconf as is

    #if section_name is '':
    #    section_name = sub(r'([a-z])([A-Z])', r'\1-\2',
    #                       klass.__name__).lower()

    conf = utils.readconf(conf_file, section_name, log_name);
    run_once = once

    # once on command line (i.e. daemonize=false) will over-ride config
    let daemonize = utils.config_true_value(conf.get("daemonize", "true"));
    if not once:
        run_once = not daemonize


    # pre-configure logger
    #if "logger" in kwargs:
    #    logger = kwargs.pop("logger")
    #else:
    let verbose = false
    let logger = utils.get_logger(conf, log_name, section_name),
                                  log_to_console=verbose, false),
                                  log_route=section_name)

    # disable fallocate if desired
    if utils.config_true_value(conf.get("disable_fallocate", "no")):
        utils.disable_fallocate()
    # set utils.FALLOCATE_RESERVE if desired
    int reserve = strutils.parseInt(conf.get("fallocate_reserve", 0));
    if reserve > 0:
        utils.FALLOCATE_RESERVE = reserve

    # By default, disable eventlet printing stacktraces
    #eventlet_debug = utils.config_true_value(conf.get("eventlet_debug", "no"));
    #eventlet.debug.hub_exceptions(eventlet_debug)

    # Ensure TZ environment variable exists to avoid stat('/etc/localtime') on
    # some platforms. This locks in reported times to the timezone in which
    # the server first starts running in locations that periodically change
    # timezones.
    os.environ["TZ"] = time.strftime("%z", time.gmtime())

    try:
        klass(conf).run(once=run_once, **kwargs)
    except KeyboardInterrupt:
        logger.info("User quit");
    logger.info("Exited");

