#ifndef DISKFILEREADER_H
#define DISKFILEREADER_H


#include <stdio.h>
#include <string>

#include "MD5Hash.h"
#include "ThreadPool.h"

class DiskFile;
class DiskFileManager;
class DiskFileReadHook;
class Logger;
class QuarantineHook;


class DiskFileReader {

private:
    FILE* _fp;
    std::string _data_file;
    int _obj_size;
    std::string _etag;
    ThreadPool _threadpool;
    DiskFile* _diskfile;
    int _disk_chunk_size;
    std::string _device_path;
    Logger* _logger;
    QuarantineHook* _quarantine_hook;
    bool _use_splice;
    int _pipe_size;
    bool _keep_cache;
    MD5Hash _iter_etag;
    int _bytes_read;
    bool _started_at_0;
    bool _read_to_eof;
    std::string _md5_of_sent_bytes;
    bool _suppress_file_closing;
    std::string _quarantined_dir;


public:
    DiskFileReader(FILE* fp,
                   const std::string& data_file,
                   int obj_size,
                   const std::string& etag,
                   ThreadPool threadpool,
                   int disk_chunk_size,
                   bool keep_cache_size,
                   const std::string& device_path,
                   Logger* logger,
                   QuarantineHook* quarantine_hook,
                   bool use_splice,
                   int pipe_size,
                   DiskFile* diskfile,
                   bool keep_cache);

    DiskFileManager* manager();
    void __iter__(DiskFileReadHook* dfr_hook);

    bool can_zero_copy_send() const;
    void zero_copy_send(int wsockfd);
    void app_iter_range(DiskFileReadHook* dfr_hook, int start, int stop);
    //void app_iter_ranges(DiskFileReadHook* dfr_hook,
    //                       ranges,
    //                       const std::string& content_type,
    //                       const std::string& boundary,
    //                       size);
    void _drop_cache(int fd, unsigned long offset, unsigned long length);
    void _quarantine(const std::string& msg);
    void _handle_close_quarantine();
    void close();
    void _close(bool check_for_suppressed); // used by closer
};


// for auto_ptr/unique_ptr use
class OpenedDiskFileReader {
private:
    DiskFileReader* _dfr;

    // disallow copies
    OpenedDiskFileReader(const OpenedDiskFileReader&);
    OpenedDiskFileReader& operator=(const OpenedDiskFileReader&);
    OpenedDiskFileReader();

public:
    OpenedDiskFileReader(DiskFileReader* dfr) :
        _dfr(dfr) {
    }

    ~OpenedDiskFileReader() {
        _dfr->close();
    }
};

class DiskFileReaderCloser {
private:
    DiskFileReader* _dfr;
    bool _check_for_suppressed;

    DiskFileReaderCloser();
    DiskFileReaderCloser(const DiskFileReaderCloser&);
    DiskFileReaderCloser& operator=(const DiskFileReaderCloser&);

public:
    DiskFileReaderCloser(DiskFileReader* dfr, bool check_for_suppressed) :
        _dfr(dfr),
        _check_for_suppressed(check_for_suppressed) {
    }

    ~DiskFileReaderCloser() {
        _dfr->_close(_check_for_suppressed);
    }
};

#endif

