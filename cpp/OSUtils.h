#ifndef OSUTILS_H
#define OSUTILS_H

#include <string>
#include <vector>


class OSUtils {
public:
    static std::vector<std::string> listdir(const std::string& dir);
    static std::string path_join(const std::string& dir,
                                 const std::string& filename);
    static int fork();
    static int wait();
    static bool ismount(const std::string& path);
    static std::string path_basename(const std::string& path);
};

#endif

