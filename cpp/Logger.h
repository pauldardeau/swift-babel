#ifndef LOGGER_H
#define LOGGER_H

#include <string>
#include <map>


class Logger {

private:
    static std::map<std::string, Logger*> loggers;

public:
    virtual ~Logger() {}

    static void set_logger(const std::string& name,
                           Logger* logger);
    static Logger* get_logger(const std::string& name);

    virtual void debug(const std::string& msg) = 0;
    virtual void info(const std::string& msg) = 0;
    virtual void warning(const std::string& msg) = 0;
    virtual void error(const std::string& msg) = 0;
    virtual void exception(const std::string& msg) = 0;

    virtual void increment(const std::string& counter) = 0;
    virtual long counter_value(const std::string& counter) = 0;

};

#endif

