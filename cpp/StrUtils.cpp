#include <stdio.h>
#include <algorithm>

#include "StrUtils.h"

#define CHAR_BUFF_SIZE 40


using namespace std;


static const std::string true_string_value = "T";
static const std::string false_string_value = "F";


bool StrUtils::startswith(const std::string& haystack,
                          const std::string& needle) {

    if (haystack.empty() || needle.empty()) {
        return false;
    }

    // int compare_char_count = min(needle.length(), haystack.length());
    int compare_char_count = needle.length();
    if (haystack.length() < compare_char_count) {
        compare_char_count = haystack.length();
    }

    bool haystack_starts_with_needle = true;

    for (int i = 0; i < compare_char_count; ++i) {
        if (haystack[i] != needle[i]) {
            haystack_starts_with_needle = false;
            break;
        }
    }
    
    return haystack_starts_with_needle;
}

std::string StrUtils::toString(int i) {
    char i_string[CHAR_BUFF_SIZE];
    snprintf(i_string, CHAR_BUFF_SIZE, "%d", i);
    return std::string(i_string);
}

std::string StrUtils::toString(long l) {
    char l_string[CHAR_BUFF_SIZE];
    snprintf(l_string, CHAR_BUFF_SIZE, "%ld", l);
    return std::string(l_string);
}

std::string StrUtils::toString(unsigned int ui) {
    char ui_string[CHAR_BUFF_SIZE];
    snprintf(ui_string, CHAR_BUFF_SIZE, "%u", ui);
    return std::string(ui_string);
}

std::string StrUtils::toString(unsigned long ul) {
    char ui_string[CHAR_BUFF_SIZE];
    snprintf(ui_string, CHAR_BUFF_SIZE, "%lu", ul);
    return std::string(ui_string);
}

std::string StrUtils::toString(float f) {
    return StrUtils::toString((double)f);
}

std::string StrUtils::toString(double d) {
    char d_string[40];
    snprintf(d_string, 40, "%f", d);
    return std::string(d_string);
}

std::string StrUtils::toString(bool b) {
    if (b) {
        return true_string_value;
    } else {
        return false_string_value;
    }
}

std::string StrUtils::upper(const std::string& s) {
    string s_upper(s);
    std::transform(s_upper.begin(),
                   s_upper.end(),
                   s_upper.begin(),
    ::toupper);
    return s_upper;
}

bool StrUtils::contains_only_chars_in(const std::string& s_test,
                                      const std::string& valid_chars) {
    return string::npos == s_test.find_first_not_of(valid_chars);
}

std::string StrUtils::lstrip(const std::string& s,
                             const std::string& leading) {
    return "";
}

