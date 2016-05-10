#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "SwiftUtils.h"
#include "OSUtils.h"
#include "Time.h"
#include "errno.h"
#include "Exceptions.h"


using namespace std;

static const string FORWARD_SLASH = "/";

static string HASH_PATH_SUFFIX = "swift_hash_path_suffix";
static string HASH_PATH_PREFIX = "swift_hash_path_prefix";


/**
Splits the str given and returns a properly stripped list of the comma
separated values.
*/
vector<string> SwiftUtils::list_from_csv(const string& comma_separated_str) {
    vector<string> l;

    if (comma_separated_str.length() > 0) {
        //TODO: convert this
        //return [v.strip() for v in comma_separated_str.split(',') if v.strip()]
    }

    return l;
}

/**
Test whether a path is a mount point. This will catch any
exceptions and translate them into a False return value
Use ismount_raw to have the exceptions raised instead.
*/
bool SwiftUtils::ismount(const string& path) {
    return SwiftUtils::ismount_raw(path);
}

/**
Test whether a path is a mount point. Whereas ismount will catch
any exceptions and just return False, this raw version will not
catch exceptions.
This is code hijacked from C Python 2.6.8, adapted to remove the extra
lstat() system call.
*/
bool SwiftUtils::ismount_raw(const string& path) {
    struct stat s1;
    int rc;

    rc = ::lstat(path.c_str(), &s1);
    if (rc != 0) {
        return false;
    }

    if (S_ISLNK(s1.st_mode)) {
        // A symlink can never be a mount point
        return false;
    }

    struct stat s2;
    rc = ::lstat(OSUtils::path_join(path, "..").c_str(), &s2);
    if (rc == 0) {
        if (s1.st_dev != s2.st_dev) {
            // path/.. on a different device as path
            return true;
        }

        if (s1.st_ino == s2.st_ino) {
            // path/.. is the same i-node as path
            return true;
        }
    }

    return false;
}

/**
Will eventlet.sleep() for the appropriate time so that the max_rate    
is never exceeded.  If max_rate is 0, will not ratelimit.  The         
maximum recommended rate should not exceed (1000 * incr_by) a second   
as eventlet.sleep() does involve some overhead.  Returns running_time  
that should be used for subsequent calls.                              
                                                                   
@param running_time the running time in milliseconds of the next      
                     allowable request. Best to start at zero.         
@param max_rate The maximum rate per second allowed for the process.  
@param incr_by How much to increment the counter.  Useful if you want 
                to ratelimit 1024 bytes/sec and have differing sizes   
                of requests. Must be > 0 to engage rate-limiting       
                behavior.                                              
@param rate_buffer Number of seconds the rate counter can drop and be 
                    allowed to catch up (at a faster than listed rate).
                    A larger number will result in larger spikes in rat
                    but better average accuracy. Must be > 0 to engage 
                    rate-limiting behavior.                            
*/
double SwiftUtils::ratelimit_sleep(double running_time,
                                   double max_rate,
                                   int incr_by,
                                   int rate_buffer) {
    if ((max_rate <= 0) || (incr_by <= 0)) {
        return running_time;
    }
 
    // 1,000 milliseconds = 1 second
    double clock_accuracy = 1000.0;
 
    // Convert seconds to milliseconds
    double now = Time::time() * clock_accuracy;

    // Calculate time per request in milliseconds
    double time_per_request = clock_accuracy * (incr_by / max_rate);
 
    // Convert rate_buffer to milliseconds and compare
    if (now - running_time > rate_buffer * clock_accuracy) {
        running_time = now;
    } else if (running_time - now > time_per_request) {
        // Convert diff back to a floating point number of seconds and sleep
        Time::sleep((running_time - now) / clock_accuracy);
    }
                                                                           
    // Return the absolute time for the next interval in milliseconds; note 
    // that time could have passed well beyond that point, but the next call
    // will catch that and skip the sleep.
    return running_time + time_per_request;
}

void SwiftUtils::mkdirs(const std::string& dirPath) {
    //TODO: implement SwiftUtils::mkdirs
}

string SwiftUtils::md5_digest(const std::string& s) {
    //TODO: implement SwiftUtils::md5_digest
    return "";
}

string SwiftUtils::md5_hexdigest(const std::string& s) {
    //TODO: implement SwiftUtils::md5_hexdigest
    return "";
}

/**
Get the canonical hash for an account/container/object
@param account Account
@param container Container
@param object Object
@param raw_digest If true, return the raw version rather than a hex digest
@return hash string
*/
string SwiftUtils::hash_path(const string& account,
                             const string& container,
                             const string& object,
                             bool raw_digest) {
    if (!object.empty() && container.empty()) {
        throw ValueError("container is required if object is provided");
    }

    string path = HASH_PATH_PREFIX;
    path += FORWARD_SLASH;
    path += account;

    if (!container.empty()) {
        path += FORWARD_SLASH;
        path += container;
    }

    if (!object.empty()) {
        path += FORWARD_SLASH;
        path += object;
    }

    path += FORWARD_SLASH;
    path += HASH_PATH_SUFFIX;

    if (raw_digest) {
        return md5_digest(path);
    } else {
        return md5_hexdigest(path);
    }
}

