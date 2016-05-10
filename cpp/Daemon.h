#ifndef DAEMON_H
#define DAEMON_H


#include <string>

#include "ConfigParser.h"
#include "Logger.h"



class Daemon {

private:
    Logger* _logger;
    ConfigParser* _conf;


public:
    Daemon(ConfigParser* conf);

    virtual void run_once() = 0;
    virtual void run_forever() = 0;

    virtual void run() {
        run(false);
    }

    virtual void run(bool once);

    static void run_daemon(Daemon* daemon,
                           const std::string& conf_file,
                           const std::string& section_name,
                           bool once);
};

#endif

