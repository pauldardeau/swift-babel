module DiskFileManager;

import AuditLocation;
import AuditorOptions;
import Logger;
import ObjectAuditHook;
import OSError;
import OSUtils;
import PolicyError;
import StoragePolicy;
import StrUtils;
import SwiftUtils;
import errno;


class DiskFileManager {

public:
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
void object_audit_location_generator(AuditorOptions options,
                                     Logger logger,
                                     ObjectAuditHook object_audit_hook) {

    string[] audit_device_dirs;

    if (options.device_dirs.size() == 0) {
        audit_device_dirs = OSUtils.listdir(options.devices);
    } else {
        // remove bogus devices and duplicates from device_dirs
        string[] v1 = OSUtils.listdir(options.devices);
        std::set<std::string> s1(v1.begin(), v1.end());
        std::set<std::string> s2(options.device_dirs.begin(),
                                 options.device_dirs.end());
        set<string> intersection;
        set_intersection(s1.begin(), s1.end(), s2.begin(), s2.end(),
                         std::inserter(intersection, intersection.begin()));
        std::copy(intersection.begin(), intersection.end(),
                  std::back_inserter(audit_device_dirs));
        //audit_device_dirs = list(
        //    set(OSUtils.listdir(devices)).intersection(set(device_dirs)));
    }

    // randomize devices in case of process restart before sweep completed
    std::random_shuffle(audit_device_dirs.begin(), audit_device_dirs.end());

    for (device; audit_device_dirs) {
        if (options.mount_check &&
            !OSUtils.ismount(OSUtils.path_join(devices, device))) {
            if (logger != null) {
                logger.debug(
                    "Skipping " ~ device ~ " as it is not mounted");
            }
            continue;
        }

        // loop through object dirs for all policies
        string[] obj_dirs =
            OSUtils.listdir(OSUtils.path_join(devices, device));

        for (dir_; obj_dirs) {
            if (!StrUtils.startswith(dir_, DATADIR_BASE)) {
                continue;
            }

            int policy;

            try {
                policy = StoragePolicy.extract_policy(dir_);
            } catch (PolicyError e) {
                if (logger != null) {
                    logger.warning("Directory " ~ dir_ ~
                                    " does not map to a valid policy (" ~
                                    e.toString() ~ ")");
                }
                continue;
            }

            string datadir_path = OSUtils.path_join(devices, device, dir_);

            for (partition; SwiftUtils.listdir(datadir_path)) {
                string part_path = OSUtils.path_join(datadir_path, partition);
                string[] suffixes;
                try {
                    suffixes = SwiftUtils.listdir(part_path);
                } catch (OSError e) {
                    if (e.errno != errno.ENOTDIR) {
                        throw e;
                    }
                    continue;
                }

                for (asuffix; suffixes) {
                    string suff_path = OSUtils.path_join(part_path, asuffix);
                    string[] hashes;
                    try {
                        hashes = OSUtils.listdir(suff_path);
                    } catch (OSError e) {
                        if (e.errno != errno.ENOTDIR) {
                            throw e;
                        }
                        continue;
                    }

                    for (hsh; hashes) {
                        string hsh_path = OSUtils.path_join(suff_path, hsh);
                        AuditLocation location =
                            new AuditLocation(hsh_path, device, partition, policy);

                        // In python this is implemented as a generator (yield).
                        // For d use object audit hook
                        object_audit_hook.auditObject(location);
                        //yield AuditLocation(hsh_path, device, partition,
                        //                    policy);

                    }  // for each hash
                }  // for each suffix
            }  // for each partition
        }  // loop through object dirs for all policies
    }  // for each device
}
}

