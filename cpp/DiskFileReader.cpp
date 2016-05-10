#include <unistd.h>
#include <fcntl.h>

#include "DiskFileReader.h"
#include "DiskFileManager.h"
#include "DiskFileReadHook.h"
#include "DiskFile.h"
#include "Exceptions.h"
#include "Logger.h"
#include "StrUtils.h"
#include "SwiftUtils.h"
#include "errno.h"

using namespace std;

static const int DROP_CACHE_WINDOW = 1024 * 1024;
static const string MD5_OF_EMPTY_STRING = "d41d8cd98f00b204e9800998ecf8427e";

DiskFileReader::DiskFileReader(FILE* fp,
                               const string& data_file,
                               int obj_size,
                               const string& etag,
                               ThreadPool threadpool,
                               int disk_chunk_size,
                               bool keep_cache_size,
                               const std::string& device_path,
                               Logger* logger,
                               QuarantineHook* quarantine_hook,
                               bool use_splice,
                               int pipe_size,
                               DiskFile* diskfile,
                               bool keep_cache) { //=false
    // Parameter tracking
    this->_fp = fp;
    this->_data_file = data_file;
    this->_obj_size = obj_size;
    this->_etag = etag;
    this->_threadpool = threadpool;
    this->_diskfile = diskfile;
    this->_disk_chunk_size = disk_chunk_size;
    this->_device_path = device_path;
    this->_logger = logger;
    this->_quarantine_hook = quarantine_hook;
    this->_use_splice = use_splice;
    this->_pipe_size = pipe_size;
    if (keep_cache) {
        // Caller suggests we keep this in cache, only do it if the
        // object's size is less than the maximum.
        this->_keep_cache = (obj_size < keep_cache_size);
    } else {
        this->_keep_cache = false;
    }

    // Internal Attributes
    this->_bytes_read = 0;
    this->_started_at_0 = false;
    this->_read_to_eof = false;
    //this->_md5_of_sent_bytes = null;
    this->_suppress_file_closing = false;
    //this->_quarantined_dir = null;
}

DiskFileManager* DiskFileReader::manager() {
    return this->_diskfile->manager();
}

void DiskFileReader::__iter__(DiskFileReadHook* dfr_hook) {
    DiskFileReaderCloser dfrc(this, true); // check for suppression
    int dropped_cache = 0;
    this->_bytes_read = 0;
    this->_started_at_0 = false;
    this->_read_to_eof = false;
    if (ftell(this->_fp) == 0) {
        this->_started_at_0 = true;
        //this->_iter_etag = hashlib.md5();
    }

    while (true) {
        string chunk = this->_threadpool.run_in_thread(
            this->_fp.read, this->_disk_chunk_size);
        if (chunk.length() > 0) {
            //if (this->_iter_etag) {
                this->_iter_etag.update(chunk);
            //}
            this->_bytes_read += chunk.length();
            if (this->_bytes_read - dropped_cache > DROP_CACHE_WINDOW) {
                this->_drop_cache(fileno(this->_fp),
                                  dropped_cache,
                                  this->_bytes_read - dropped_cache);
                dropped_cache = this->_bytes_read;
            }
            dfr_hook->onFileRead(chunk);
        } else {
            this->_read_to_eof = true;
            this->_drop_cache(fileno(this->_fp),
                              dropped_cache,
                              this->_bytes_read - dropped_cache);
            break;
        }
    }
}

bool DiskFileReader::can_zero_copy_send() const {
    return this->_use_splice;
}

/*
void DiskFileReader::zero_copy_send(int wsockfd) {
    // Note: if we ever add support for zero-copy ranged GET responses,
    // we'll have to make this conditional.
    this->_started_at_0 = true;

    int rfd = fileno(this->_fp);
    int pipe_fds[2];
    pipe(pipe_fds);
    int client_rpipe = pipe_fds[0];
    int client_wpipe = pipe_fds[1];
    pipe(pipe_fds);
    int hash_rpipe = pipe_fds[0];
    int hash_wpipe = pipe_fds[1];
    int md5_sockfd = SwiftUtils::get_md5_socket();

    // The actual amount allocated to the pipe may be rounded up to the
    // nearest multiple of the page size. If we have the memory allocated,
    // we may as well use it.
    //
    // Note: this will raise IOError on failure, so we don't bother
    // checking the return value.
    int pipe_size = fcntl(client_rpipe, F_SETPIPE_SZ, this->_pipe_size);
    fcntl(hash_rpipe, F_SETPIPE_SZ, pipe_size);

    int dropped_cache = 0;
    this->_bytes_read = 0;
    int bytes_copied;

    try {
        while (true) {
            // Read data from disk to pipe
            (bytes_in_pipe, _1, _2) = this->_threadpool.run_in_thread(
                splice, rfd, null, client_wpipe, null, pipe_size, 0);
            if (bytes_in_pipe == 0) {
                this->_read_to_eof = true;
                this->_drop_cache(rfd,
                                  dropped_cache,
                                  this->_bytes_read - dropped_cache);
                break;
            }
            this->_bytes_read += bytes_in_pipe;

            // "Copy" data from pipe A to pipe B (really just some pointer
            // manipulation in the kernel, not actual copying).
            bytes_copied = tee(client_rpipe, hash_wpipe, bytes_in_pipe, 0);
            if (bytes_copied != bytes_in_pipe) {
                // We teed data between two pipes of equal size, and the
                // destination pipe was empty. If, somehow, the destination
                // pipe was full before all the data was teed, we should
                // fail here. If we don't raise an exception, then we will
                // have the incorrect MD5 hash once the object has been
                // sent out, causing a false-positive quarantine.
                throw std::exception(string("tee() failed: tried to move ") +
                                    StrUtils::toString(bytes_in_pipe) +
                                    " bytes, but only moved " +
                                    StrUtils::toString(bytes_copied));
            }
            // Take the data and feed it into an in-kernel MD5 socket. The
            // MD5 socket hashes data that is written to it. Reading from
            // it yields the MD5 checksum of the written data.
            //
            // Note that we don't have to worry about splice() returning
            // null here (which happens on EWOULDBLOCK); we're splicing
            // $bytes_in_pipe bytes from a pipe with exactly that many
            // bytes in it, so read won't block, and we're splicing it into
            // an MD5 socket, which synchronously hashes any data sent to
            // it, so writing won't block either.
            (hashed, _1, _2) = splice(hash_rpipe, null, md5_sockfd, null,
                                      bytes_in_pipe, splice.SPLICE_F_MORE);
            if (hashed != bytes_in_pipe) {
                throw exception(string("md5 socket didn't take all the data? ") +
                                "(tried to write " +
                                StrUtils::toString(bytes_in_pipe) +
                                ", but wrote " + hashed + ")");
            }

            while (bytes_in_pipe > 0) {
                try {
                    res = splice(client_rpipe, null, wsockfd, null,
                                 bytes_in_pipe, 0);
                    bytes_in_pipe -= res[0];
                } catch (const IOError& exc) {
                    if (exc._errno == EWOULDBLOCK) {
                        trampoline(wsockfd, true); //write
                    } else {
                        throw exc;
                    }
                }
            }

            if (this->_bytes_read - dropped_cache > DROP_CACHE_WINDOW) {
                this->_drop_cache(rfd,
                                  dropped_cache,
                                  this->_bytes_read - dropped_cache);
                dropped_cache = this->_bytes_read;
            }
        }
    } finally {
        // Linux MD5 sockets return '00000000000000000000000000000000' for
        // the checksum if you didn't write any bytes to them, instead of
        // returning the correct value.
        string hex_checksum;
        if (this->_bytes_read > 0) {
            bin_checksum = os.read(md5_sockfd, 16);
            hex_checksum = "".join("%02x" % ord(c) for c in bin_checksum);
        } else {
            hex_checksum = MD5_OF_EMPTY_STRING;
        }
        this->_md5_of_sent_bytes = hex_checksum;

        ::close(client_rpipe);
        ::close(client_wpipe);
        ::close(hash_rpipe);
        ::close(hash_wpipe);
        ::close(md5_sockfd);
        this->close();
    }
}
*/

void DiskFileReader::app_iter_range(DiskFileReadHook* dfr_hook,
                                    int start,
                                    int stop) {
    if (start > -1) {
        fseek(this->_fp, start, SEEK_SET);
    }

    long length;

    if (stop > 0) {
        length = stop - start;
    } else {
        length = -1;
    }

    DiskFileReaderCloser dfrc(this, true); // check for suppress
    for (string chunk : this) {
        if (length > -1) {
            length -= chunk.length();
            if (length < 0) {
                // Chop off the extra:
                dfr_hook->onFileRead(chunk.substr(0,length));
                break;
            }
        }
        dfr_hook->onFileRead(chunk);
    }
}

/*
void DiskFileReader::app_iter_ranges(DiskFileReadHook* dfr_hook,
                                     ranges,
                                     content_type,
                                     boundary,
                                     size) {
    if (!ranges) {
        dfr_hook->onFileRead(string(""));
    } else {
        try {
            this->_suppress_file_closing = true;
            for (string chunk : multi_range_iterator(
                    ranges, content_type, boundary, size,
                    this->app_iter_range)) {
                dfr_hook->onFileRead(chunk);
            }
        } finally {
            this->_suppress_file_closing = false;
            this->close();
        }
    }
}
*/

void DiskFileReader::_drop_cache(int fd,
                                 unsigned long offset,
                                 unsigned long length) {
    if (!this->_keep_cache) {
        SwiftUtils::drop_buffer_cache(fd, offset, length);
    }
}

void DiskFileReader::_quarantine(const string& msg) {
    this->_quarantined_dir = this->_threadpool.run_in_thread(
        this->manager()->quarantine_renamer, this->_device_path,
        this->_data_file);
    this->_logger->warning(string("Quarantined object ") +
                         this->_data_file + ": " + msg);
    this->_logger->increment("quarantines");
    if (this->_quarantine_hook != NULL) {
        this->_quarantine_hook->onQuarantine(msg);
    }
}

void DiskFileReader::_handle_close_quarantine() {
    if (this->_iter_etag.length() > 0 &&
        this->_md5_of_sent_bytes.length() == 0) {
        this->_md5_of_sent_bytes = this->_iter_etag.hexdigest();
    }

    if (this->_bytes_read != this->_obj_size) {
        this->_quarantine(
            string("Bytes read: ") +
            StrUtils::toString(this->_bytes_read) +
            ", does not match metadata: " +
            StrUtils::toString(this->_obj_size));
    } else if (this->_md5_of_sent_bytes.length() > 0 &&
               this->_etag != this->_md5_of_sent_bytes) {
        this->_quarantine(
            string("ETag ") +
            this->_etag +
            " and file's md5 " +
            this->_md5_of_sent_bytes +
            " do not match");
    }
}

void DiskFileReader::close() {
    if (NULL != this->_fp) {
        DiskFileReaderCloser closer(this, false); // don't check for suppress
        try {
            if (this->_started_at_0 && this->_read_to_eof) {
                this->_handle_close_quarantine();
            }
        } catch (const DiskFileQuarantined& dfq) {
            throw dfq;
        } catch (const exception& e) { //, Timeout) as e:
            this->_logger->error(
                string("ERROR DiskFile ") +
                this->_data_file +
                " close failure: " +
                e.what());
                // unfortunately, no easy way to get call stack
                // in c++
                /*
                " : " +
                "".join(traceback.format_stack()));
                */
        }
    }
}

void DiskFileReader::_close(bool check_for_suppressed) {
    if (check_for_suppressed) {
        if (!this->_suppress_file_closing) {
            this->close();
        }
    } else {
        this->close();
    }
}

