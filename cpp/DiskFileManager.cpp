#include <set>
#include <algorithm>

#include "DiskFileManager.h"
#include "ObjectAuditHook.h"
#include "OSUtils.h"
#include "PolicyError.h"
#include "StrUtils.h"
#include "SwiftUtils.h"
#include "errno.h"
#include "Exceptions.h"


using namespace std;

static const std::string DATADIR_BASE = "objects";


/**
    Given a devices path (e.g. "/srv/node"), yield an AuditLocation for all
    objects stored under that directory if device_dirs isn't set.  If
    device_dirs is set, only yield AuditLocation for the objects under the
    entries in device_dirs. The AuditLocation only knows the path to the hash
    directory, not to the .data file therein (if any). This is to avoid a
    double listdir(hash_dir); the DiskFile object will always do one, so
    we don't.
    :param devices: parent directory of the devices to be audited
    :param mount_check: flag to check if a mount check should be performed
                        on devices
    :param logger: a logger object
    :device_dirs: a list of directories under devices to traverse
*/
void DiskFileManager::object_audit_location_generator(const AuditorOptions& options,
                                                      Logger* logger,
                                                      ObjectAuditHook* object_audit_hook) {

    vector<string> audit_device_dirs;

    if (options.device_dirs.size() == 0) {
        audit_device_dirs = OSUtils::listdir(options.devices);
    } else {
        // remove bogus devices and duplicates from device_dirs
        std::vector<std::string> v1 = OSUtils::listdir(options.devices);
        std::set<std::string> s1(v1.begin(), v1.end());
        std::set<std::string> s2(options.device_dirs.begin(),
                                 options.device_dirs.end());
        set<string> intersection;
        set_intersection(s1.begin(), s1.end(), s2.begin(), s2.end(),
                         std::inserter(intersection, intersection.begin()));
        std::copy(intersection.begin(), intersection.end(),
                  std::back_inserter(audit_device_dirs));
        //audit_device_dirs = list(
        //    set(OSUtils::listdir(devices)).intersection(set(device_dirs)));
    }

    // randomize devices in case of process restart before sweep completed
    std::random_shuffle(audit_device_dirs.begin(), audit_device_dirs.end());

    const vector<string>::const_iterator itDevicesEnd =
        audit_device_dirs.end();
    vector<string>::iterator itDevices = audit_device_dirs.begin();

    for (; itDevices != itDevicesEnd; ++itDevices) {
        const string& device = *itDevices;
        if (mount_check &&
            !OSUtils::ismount(OSUtils::path_join(devices, device))) {
            if (logger != NULL) {
                logger->debug(
                    string("Skipping ") + device + " as it is not mounted");
            }
            continue;
        }

        // loop through object dirs for all policies
        vector<string> obj_dirs =
            OSUtils::listdir(OSUtils::path_join(devices, device));
        const vector<string>::const_iterator itObjDirEnd = obj_dirs.end();
        vector<string>::iterator itObjDir = obj_dirs.begin();

        for (; itObjDir != itObjDirEnd; ++itObjDir) {
            const string& dir_ = *itObjDir;
            if (!StrUtils::startswith(dir_, DATADIR_BASE)) {
                continue;
            }

            int policy;

            try {
                policy = StoragePolicy::extract_policy(dir_);
            } catch (const PolicyError& e) {
                if (logger != NULL) {
                    logger->warning(string("Directory ") + dir_ +
                                    " does not map to a valid policy (" +
                                    e.toString() + ")");
                }
                continue;
            }

            string datadir_path = OSUtils::path_join(devices, device, dir_);
            vector<string> partitions = SwiftUtils::listdir(datadir_path);
            const vector<string>::const_iterator itPartEnd = partitions.end();
            vector<string>::iterator itPart = partitions.begin();

            for (; itPart != itPartEnd; ++itPart) {
                const string& partition = *itPart;
                string part_path = OSUtils::path_join(datadir_path, partition);
                vector<string> suffixes;
                try {
                    suffixes = SwiftUtils::listdir(part_path);
                } catch (const OSError& e) {
                    if (e._errno != ENOTDIR) {
                        throw e;
                    }
                    continue;
                }

                const vector<string>::const_iterator itSuffEnd = suffixes.end();
                vector<string>::iterator itSuff = suffixes.begin();

                for (; itSuff != itSuffEnd; ++itSuff) {
                    const string& asuffix = *itSuff;
                    string suff_path = OSUtils::path_join(part_path, asuffix);
                    vector<string> hashes;
                    try {
                        hashes = OSUtils::listdir(suff_path);
                    } catch (const OSError& e) {
                        if (e._errno != ENOTDIR) {
                            throw e;
                        }
                        continue;
                    }

                    const vector<string>::const_iterator itHashEnd = hashes.end();
                    vector<string>::iterator itHash = hashes.begin();

                    for (; itHash != itHashEnd; ++itHash) {
                        const string& hsh = *itHash;
                        string hsh_path = OSUtils::path_join(suff_path, hsh);
                        AuditLocation location(hsh_path, device, partition, policy);

                        // In python this is implemented as a generator (yield).
                        // For c++ use object audit hook
                        object_audit_hook->auditObject(location);
                        //yield AuditLocation(hsh_path, device, partition,
                        //                    policy);

                    }  // for each hash
                }  // for each suffix
            }  // for each partition
        }  // loop through object dirs for all policies
    }  // for each device
}

