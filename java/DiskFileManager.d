import java.util.List;


public class DiskFileManager {


/**
    Given a devices path (e.g. "/srv/node"), yield an AuditLocation for all
    objects stored under that directory if device_dirs isn't set.  If
    device_dirs is set, only yield AuditLocation for the objects under the
    entries in device_dirs. The AuditLocation only knows the path to the hash
    directory, not to the .data file therein (if any). This is to avoid a
    double listdir(hash_dir); the DiskFile object will always do one, so
    we don't.
    @param devices: parent directory of the devices to be audited
    @param mount_check: flag to check if a mount check should be performed
                        on devices
    @param logger: a logger object

    @device_dirs: a list of directories under devices to traverse
*/
void DiskFileManager::object_audit_location_generator(AuditorOptions options,
                                                      Logger logger,
                                                      ObjectAuditHook object_audit_hook) {

    List<String> audit_device_dirs;

    if (options.device_dirs.size() == 0) {
        audit_device_dirs = OSUtils.listdir(options.devices);
    } else {
        // remove bogus devices and duplicates from device_dirs
        List<String> v1 = OSUtils.listdir(options.devices);
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

    for (String device : audit_device_dirs) {
        if (options.mount_check &&
            !OSUtils.ismount(OSUtils.path_join(devices, device))) {
            if (logger != NULL) {
                logger->debug(
                    string("Skipping ") + device + " as it is not mounted");
            }
            continue;
        }

        // loop through object dirs for all policies
        List<String> obj_dirs =
            OSUtils.listdir(OSUtils.path_join(devices, device));

        for (String dir_ : obj_dirs) {
            if (!dir_.startswith(DATADIR_BASE)) {
                continue;
            }

            int policy;

            try {
                policy = StoragePolicy.extract_policy(dir_);
            } catch (PolicyError e) {
                if (logger != null) {
                    logger.warning(string("Directory ") + dir_ +
                                    " does not map to a valid policy (" +
                                    e.toString() + ")");
                }
                continue;
            }

            String datadir_path = OSUtils.path_join(devices, device, dir_);
            List<String> partitions = SwiftUtils.listdir(datadir_path);

            for (String partition : partitions) {
                String part_path = OSUtils.path_join(datadir_path, partition);
                List<String> suffixes;
                try {
                    suffixes = SwiftUtils.listdir(part_path);
                } catch (OSError e) {
                    if (e.errno != errno.ENOTDIR) {
                        throw e;
                    }
                    continue;
                }

                for (String asuffix : suffixes) {
                    String suff_path = OSUtils.path_join(part_path, asuffix);
                    List<String> hashes;
                    try {
                        hashes = OSUtils.listdir(suff_path);
                    } catch (OSError e) {
                        if (e.errno != errno.ENOTDIR) {
                            throw e;
                        }
                        continue;
                    }

                    for (String hsh : hashes) {
                        String hsh_path = OSUtils.path_join(suff_path, hsh);
                        AuditLocation location =
                            new AuditLocation(hsh_path, device, partition, policy);

                        // In python this is implemented as a generator (yield).
                        // For c++ use object audit hook
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

