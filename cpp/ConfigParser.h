#ifndef CONFIGPARSER_H
#define CONFIGPARSER_H

#include <string>
#include <map>


class ConfigParser {

private:
    std::map<std::string, std::string> values;

public:
    const std::string& get(const std::string& key,
                           const std::string& default_value) const;
};


#endif


