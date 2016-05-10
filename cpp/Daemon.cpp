#include "Daemon.h"

using namespace std;


Daemon::Daemon(ConfigParser* conf) :
    _conf(conf) {
    //TODO: implement Daemon(ConfigParser*)
}

void Daemon::run(bool once) {
    //TODO: implement run(bool)
}

void Daemon::run_daemon(Daemon* daemon,
                        const string& conf_file,
                        const string& section_name,
                        bool once) {
    //TODO: implement run_daemon
}

