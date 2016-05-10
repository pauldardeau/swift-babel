module Utils;

import Config;
import ConfigParser;
import Logger;


class Utils {

public:

    static Logger get_logger(Config conf, string log_route) {
        return null;
    }

    static void validate_configuration() {
    }

    static void drop_privileges(string user) {
    }

    static void capture_stdio(Logger logger) {
    }

    static ConfigParser readconf(string conf_file,
                                 string section_name,
                                 string log_name) {
        return null;
    }

    static bool config_true_value(string config_value) {
        return false;
    }

}

