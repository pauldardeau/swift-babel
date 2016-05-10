#ifndef STRUTILS_H
#define STRUTILS_H

#include <string>


class StrUtils {
public:
    static bool startswith(const std::string& haystack,
                           const std::string& needle);
    static std::string toString(int i);
    static std::string toString(long l);
    static std::string toString(unsigned int ui);
    static std::string toString(unsigned long ul);
    static std::string toString(float f);
    static std::string toString(double d);
    static std::string toString(bool b);
    static std::string upper(const std::string& s);
    static bool contains_only_chars_in(const std::string& s_test,
                                       const std::string& valid_chars);
    static std::string lstrip(const std::string& s,
                              const std::string& leading);
};

#endif

