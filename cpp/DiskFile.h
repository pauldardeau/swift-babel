#ifndef DISKFILE_H
#define DISKFILE_H

#include <stdio.h>
#include <map>
#include <vector>
#include <exception>

#include "Date.h"
#include "Logger.h"
#include "QuarantineHook.h"
#include "StoragePolicy.h"
#include "ThreadPool.h"


class DiskFileManager;
class DiskFileReader;


class DiskFile {

/*
    public Set<String> DATAFILE_SYSTEM_META = null;

    static {
        DATAFILE_SYSTEM_META = new TreeSet();
        DATAFILE_SYSTEM_META.add("content-length");
        DATAFILE_SYSTEM_META.add("content-type");
        DATAFILE_SYSTEM_META.add("deleted");
        DATAFILE_SYSTEM_META.add("etag");
    }
*/

private:
    std::string _name;
    std::string _account;
    std::string _container;
    std::string _obj;
    std::string _datadir;
    std::string _data_file;
    std::string _tmpdir;
    bool _use_splice;
    bool _started_at_0;
    bool _read_to_eof;
    int _pipe_size;
    StoragePolicy* policy;
    Logger* _logger;
    DiskFileManager* _manager;
    std::string _device_path;
    int _disk_chunk_size;
    int _bytes_per_sync;
    std::map<std::string, std::string> _metadata;
    std::map<std::string, std::string> _datafile_metadata;
    std::map<std::string, std::string> _metafile_metadata;
    FILE* _fp;


public:
    DiskFile(DiskFileManager* mgr,
             const std::string& device_path,
             ThreadPool threadpool,
             int partition,
             const std::string& account, //=null
             const std::string& container, //=null
             const std::string& obj, //=null
             const std::string& _datadir, //=null
             StoragePolicy* policy, //=null
             bool use_splice=false,
             int pipe_size=-1);

    DiskFileManager* manager();

    const std::string& account() const;
    const std::string& container() const;
    const std::string& obj() const;

    void content_length() const;

    Date timestamp();
    Date data_timestamp();
    Date durable_timestamp();

    void fragments();

    /*
    static void from_hash_dir(Class cls,
                              DiskFileManager* mgr,
                              const std::string& hash_dir_path,
                              const std::string& device_path,
                              int partition,
                              StoragePolicy policy);
    */

    DiskFile* open();
    void close();

    void __enter__();
    void __exit__();

    bool exists(const std::string& path) const;

    std::exception* _quarantine(const std::string& data_file,
                                const std::string& msg);

    virtual void _get_ondisk_files(std::vector<std::string>& files) = 0;

    std::exception _construct_exception_from_ts_file(const std::string& ts_file);

    void _verify_name_matches_hash(const std::string& data_file);
    void _verify_data_file(const std::string& data_file,
                           FILE* fp);


    std::map<std::string, std::string> _failsafe_read_metadata(const std::string& source,
                                 const std::string& quarantine_filename);

    void _construct_from_data_file(const std::string& data_file,
                                   const std::string& meta_file);

    std::map<std::string, std::string>& get_metafile_metadata();
    std::map<std::string, std::string>& get_datafile_metadata();
    std::map<std::string, std::string>& get_metadata();
    std::map<std::string, std::string>& read_metadata();

    DiskFileReader* reader(bool keep_cache=false,
                          QuarantineHook* quarantine_hook=NULL);

    void create(int size);

    void write_metadata(const std::map<std::string, std::string>& metadata);

    void delete_object(const std::string& timestamp);

};

// for auto_ptr/unique_ptr use
class DiskFileCloser {
private:
    // disallow copies
    DiskFileCloser(const DiskFileCloser&);
    DiskFileCloser& operator=(const DiskFileCloser&);
    DiskFileCloser();

public:
    DiskFile* _df;

    DiskFileCloser(DiskFile* df) :
        _df(df) {
    }

    ~DiskFileCloser() {
        if (_df != NULL) {
            _df->close();
        }
    }
};

#endif


