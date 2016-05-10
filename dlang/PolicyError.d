module PolicyError;

import std.conv;


class PolicyError : Exception {

public:
    this(string msg, int index=-1) {
        if (index > -1) {
            msg ~= (", for index " ~ to!string(index));
        }
        super(msg);
    }
}

