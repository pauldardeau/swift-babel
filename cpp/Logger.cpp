#include "Logger.h"

using namespace std;


map<string, Logger*> Logger::loggers;


void Logger::set_logger(const string& name, Logger* logger) {
    loggers[name] = logger;
}

Logger* Logger::get_logger(const string& name) {
    map<string, Logger*>::iterator it = loggers.find(name);
    if (it != loggers.end()) {
        return (*it).second;
    } else {
        return NULL;
    }
}

