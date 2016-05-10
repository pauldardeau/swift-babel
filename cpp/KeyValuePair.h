#ifndef KEYVALUEPAIR_H
#define KEYVALUEPAIR_H


#include <string>


class KeyValuePair {

public:
    std::string key;
    std::string value;

    KeyValuePair() {}
    KeyValuePair(const std::string& aKey, const std::string& aValue) :
        key(aKey),
        value(aValue) {
    }

    KeyValuePair(const KeyValuePair& copy) :
        key(copy.key),
        value(copy.value) {
    }

    KeyValuePair& operator=(const KeyValuePair& copy) {
        if (this == &copy) {
            return *this;
        }

        key = copy.key;
        value = copy.value;

        return *this;
    }
};


#endif

