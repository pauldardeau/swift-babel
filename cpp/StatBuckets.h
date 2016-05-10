#ifndef STATBUCKETS_H
#define STATBUCKETS_H

#include <string>
#include <map>


class StatBuckets {

private:
    std::map<int, long> stats_buckets;
    long over_counter;


public:
    StatBuckets() :
        over_counter(0) {
    }

    StatBuckets(const StatBuckets& copy) :
        stats_buckets(copy.stats_buckets),
        over_counter(copy.over_counter) {
    }

    StatBuckets& operator=(const StatBuckets& copy) {
        if (this == &copy) {
            return *this;
        }

        stats_buckets = copy.stats_buckets;
        over_counter = copy.over_counter;

        return *this;
    }

    void increment(int counter_size) {
        std::map<int, long>::iterator it = stats_buckets.find(counter_size);
        if (it != stats_buckets.end()) {
            stats_buckets[counter_size] = (*it).second + 1;
        } else {
            stats_buckets[counter_size] = 1;
        }
    }

    void increment_over() {
        ++over_counter;
    }

    std::string toString() const {
        //TODO: implement StatBuckets::toString
        return "";
    }
};

#endif

