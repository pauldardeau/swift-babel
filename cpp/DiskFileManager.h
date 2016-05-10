#ifndef DISKFILEMANAGER_H
#define DISKFILEMANAGER_H


#include <string>
#include <vector>
#include <map>

#include "AuditLocation.h"
#include "AuditorOptions.h"
#include "Config.h"
#include "DiskFile.h"
#include "FileInfo.h"
#include "Logger.h"
#include "Mapper.h"
#include "StoragePolicy.h"
#include "TimeConstants.h"
#include "Timestamp.h"


class DiskFile;
class ObjectAuditHook;


class DiskFileManager {


private:
    Logger* logger;
    std::string devices;
    int disk_chunk_size;
    int keep_cache_size;
    int bytes_per_sync;
    bool mount_check;
    int reclaim_age;
    bool replication_one_per_device;
    int replication_lock_timeout;
    int threads_per_disk;
    bool use_splice;


public:
    DiskFileManager(Config conf, Logger* logger);
    virtual ~DiskFileManager() {}

    virtual void parse_on_disk_filename(const std::string& filename) = 0;
    virtual void _process_ondisk_files(const std::map<std::string, std::vector<FileInfo> >& exts, const std::map<std::string, std::vector<FileInfo> >& results) = 0;
    virtual void _hash_suffix(const std::string&path,
                              int reclaim_age) = 0;


    bool _verify_ondisk_files(const std::map<std::string, std::vector<FileInfo> >& results);

    /*
    void _split_list(original_list, condition);

    void _split_gt_timestamp(file_info_list, Timestamp timestamp);

    void _split_gte_timestamp(file_info_list, Timestamp timestamp);
    */

    void get_ondisk_files(std::vector<std::string>& files,
                          const std::string& datadir,
                          bool verify=true);

    std::map<std::string, std::vector<std::string> > cleanup_ondisk_files(const std::string& hsh_path,
                                 int reclaim_age=TimeConstants::ONE_WEEK);

    void hash_cleanup_listdir(const std::string& hsh_path,
                              int reclaim_age=TimeConstants::ONE_WEEK);

    void _hash_suffix_dir(const std::string& path,
                          Mapper* mapper,
                          int reclaim_age);

    void _get_hashes(const std::string& partition_path,
                     std::vector<std::string>& recalculate,
                     bool do_listdir=false,
                     int reclaim_age=-1);

    std::string construct_dev_path(const std::string& device);

    std::string get_dev_path(const std::string& device);
    std::string get_dev_path(const std::string& device,
                             bool mount_check);

    void replication_lock(const std::string& device);

    //PJD: based on name, is this Python only?
    /*
    void pickle_async_update(const std::string& device,
                             const std::string& account,
                             const std::string& container,
                             const std::string& obj,
                             byte[] data,
                             Date timestamp,
                             StoragePolicy* policy);
    */

    DiskFile* get_diskfile(const std::string& device,
                      int partition,
                      const std::string& account,
                      const std::string& container,
                      const std::string& obj,
                      StoragePolicy* policy);

    void object_audit_location_generator(const AuditorOptions& options,
                                         Logger* logger,
                                         ObjectAuditHook* object_audit_hook);

    DiskFile* get_diskfile_from_audit_location(const AuditLocation& audit_location);

    DiskFile* get_diskfile_from_hash(const std::string& device,
                                     int partition,
                                     const std::string& object_hash,
                                     StoragePolicy* policy);

    void get_hashes(const std::string& device,
                    int partition,
                    const std::vector<std::string>& suffixes,
                    StoragePolicy* policy);

    std::vector<std::string> _listdir(const std::string& path);

    void yield_suffixes(const std::string& device,
                        int partition,
                        StoragePolicy* policy);

    void yield_hashes(const std::string& device,
                      int partition,
                      StoragePolicy* policy,
                      const std::vector<std::string>& suffixes);

};


#endif


