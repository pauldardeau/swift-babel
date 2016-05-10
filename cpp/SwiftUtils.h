#ifndef SWIFTUTILS_H
#define SWIFTUTILS_H

#include <string>
#include <vector>


class SwiftUtils {

public:
    static std::vector<std::string> list_from_csv(const std::string& comma_separated_str);
    static bool ismount(const std::string& path);
    static bool ismount_raw(const std::string& path);
    static double ratelimit_sleep(double running_time,
                                  double max_rate,
                                  int incr_by=1,
                                  int rate_buffer=5);

    static std::vector<std::string> listdir(const std::string& dir);
    static void drop_buffer_cache(int fd,
                                  unsigned long offset,
                                  unsigned long length);
    static int get_md5_socket();
    static void mkdirs(const std::string& dirPath);
    static std::string hash_path(const std::string& account,
                                 const std::string& container,
                                 const std::string& object,
                                 bool raw_digest);
    static std::string md5_digest(const std::string& s);
    static std::string md5_hexdigest(const std::string& s);

};


#endif

