import Config;
import Logger;


proc list_from_csv*(s: string): seq[string] =
    return @[]

proc get_logger*(conf: Config, log_route: string): Logger =
    return Logger(x: 5);

proc validate_configuration*() =
    var
        x: int

    x = 5;

proc drop_privileges*(user: string) =
    var
        x: int

    x = 5

proc capture_stdio*(logger: Logger) =
    var
        x: int

    x = 5

proc readconf*(conf_file: string, section_name: string, log_name: string): Config =
    var
        x: int

    x = 5
    return nil

proc config_true_value*(config_value: string): bool =
    return false;

