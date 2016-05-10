#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>

#include "DiskFile.h"
#include "DiskFileWriter.h"
#include "OSUtils.h"
#include "SwiftUtils.h"
#include "Timestamp.h"
#include "Exceptions.h"
#include "errno.h"


using namespace std;


DiskFile::DiskFile(DiskFileManager* mgr,
             const string& device_path,
             ThreadPool& threadpool,
             int partition,
             const string& account,
             const string& container,
             const string& obj,
             const string& _datadir,
             StoragePolicy* policy,
             bool use_splice,
             int pipe_size) {
}

DiskFileManager* DiskFile::manager() {
}

const string& DiskFile::account() const {
    return this->_account;
}

const string& DiskFile::container() const {
    return this->_container;
}

const string& DiskFile::obj() const {
    return this->_obj;
}

void DiskFile::content_length() const {
}

Date DiskFile::timestamp() {
}

Date DiskFile::data_timestamp() {
}

Date DiskFile::durable_timestamp() {
}

void DiskFile::fragments() {
}

DiskFile* DiskFile::open() {
    vector<string> files;
    // First figure out if the data directory exists
    try {
        files = OSUtils::listdir(this->_datadir);
    } catch (const OSError& err) {
        if (err._errno == ENOTDIR) {
            // If there's a file here instead of a directory, quarantine
            // it; something's gone wrong somewhere.
            throw this->_quarantine(
                // hack: quarantine_renamer actually renames the directory
                // enclosing the filename you give it, but here we just
                // want this one file and not its parent.
                OSUtils::path_join(this->_datadir, "made-up-filename"),
                string("Expected directory, found file at ") + this->_datadir);
        } else if (err._errno != ENOENT) {
            throw DiskFileError(
                string("Error listing directory ") +
                    this->_datadir + ": " +
                    err.toString());
        }
        // The data directory does not exist, so the object cannot exist.
        file.clear();
    }

    // gather info about the valid files to use to open the DiskFile
    file_info = this->_get_ondisk_files(files);

    this->_data_file = file_info.get("data_file");
    if (this->_data_file.empty()) {
        throw this->_construct_exception_from_ts_file(file_info);
    }
    this->_fp = this->_construct_from_data_file(file_info);
    // This method must populate the internal _metadata attribute.
    return this;
}

void DiskFile::close() {
    if (NULL != this->_fp) {
        try {
            if (this->_started_at_0 && this->_read_to_eof) {
                this->_handle_close_quarantine();
            }
        } catch (const DiskFileQuarantined& dfq) {
            throw dfq;
        } catch (const std::exception& e) {
            this->_logger->error(
                string("ERROR DiskFile ") +
                this->_data_file +
                " close failure: " +
                e.what() +
                " : NA");
        }

        finally:
            ::fclose(this->_fp);
            this->_fp = NULL;
    }
}

void DiskFile::__enter__() {
}

void DiskFile::__exit__() {
}

std::exception* DiskFile::_quarantine(const string& data_file,
                                      const string& msg) {
    this->_quarantined_dir = this->_threadpool.run_in_thread(
        this->manager.quarantine_renamer,
        this->_device_path,
        this->_data_file);
    this->_logger->warning(string("Quarantined object ") +
                           this->_data_file +
                           ": " +
                           msg);
    this->_logger->increment("quarantines");
    this->_quarantine_hook(msg);
}

std::exception DiskFile::_construct_exception_from_ts_file(const string& ts_file) {
    if (ts_file.empty()) {
        return DiskFileNotExist();
    } else {
        try {
            map<string,string> metadata =
                this->_failsafe_read_metadata(ts_file, ts_file);
            // All well and good that we have found a tombstone file, but
            // we don't have a data file so we are just going to raise an
            // exception that we could not find the object, providing the
            // tombstone's timestamp.
            return DiskFileDeleted(metadata);
        } catch (const DiskFileQuarantined& dfq) {
            // If the tombstone's corrupted, quarantine it and pretend it
            // wasn't there
            return DiskFileNotExist();
        }
    }
}

void DiskFile::_verify_name_matches_hash(const string& data_file) {
    const string hash_from_fs = OSUtils::path_basename(this->_datadir);
    const string hash_from_name =
        SwiftUtils::hash_path(StrUtils::lstrip(this->_name, "/"),
                              "",
                              "",
                              false);
    if (hash_from_fs != hash_from_name) {
        throw this->_quarantine(
            data_file,
            "Hash of name in metadata does not match directory name");
    }
}

int DiskFile::_verify_data_file(const string& data_file,
                                FILE* fp) {

    map<string,string>::it = this->_metadata.find("name");
    if (it == this->_metadata.end()) {
        throw this->_quarantine(data_file, "missing name metadata");
    }

    const string& mname = *it;

    if (mname != this->_name) {
        this->_logger->error(
            string("Client path ") + this->_name + " does not match " +
             "path stored in object metadata " + mname);
        throw DiskFileCollision("Client path does not match path "
                                "stored in object metadata");
    }

    it = this->_metadata.find("X-Delete-At");
    if (it != this->_metadata.end()) {
        const string& delete_at_str = (*it).second;
        try {
            int x_delete_at = Integer::parseInt(delete_at_str);
            if (x_delete_at <= time.time()) {
                throw DiskFileExpired(this->_metadata);
            }
        } catch (const IllegalValueError& ive) {
            // Quarantine, the x-delete-at key is present but not an
            // integer.
            throw this->_quarantine(
                data_file, "bad metadata x-delete-at value %s" % (
                    this->_metadata["X-Delete-At"]))
        }
    }

    it = this->_metadata.find("Content-Length");
    if (it == this->_metadata.end()) {
        throw this->_quarantine(data_file,
                                "missing content-length in metadata");
    }

    int metadata_size = 0;

    try {
        metadata_size = Integer::parseInt((*it).second);
    } catch (const IllegalValueError& ive) {
        // Quarantine, the content-length key is present but not an
        // integer.
        throw this->_quarantine(
            data_file, string("bad metadata content-length value ") +
                       (*it).second);
    }

    int fd = ::fileno(fp);
    struct stat statbuf;

    int rc = ::fstat(fd, &statbuf);
    if (rc != 0) {
        // Quarantine, we can't successfully stat the file.
        throw this->_quarantine(data_file,
                                string("not stat-able: ") +
                                err.toString());
    }

    const int obj_size = statbuf.st_size;

    if (obj_size != metadata_size) {
        throw this->_quarantine(
            data_file, string("metadata content-length " +
                              metadata_size +
                              "  does not match actual object size " +
                              statbuf.st_size));
    }

    this->_content_length = obj_size;
    return obj_size;
}

map<string,string> DiskFile::_failsafe_read_metadata(const string& source,
                                       const string& quarantine_filename) {
    try {
        return read_metadata(source);
    } catch (const DiskFileXattrNotSupported& dfxns) {
        throw dfxns;
    } catch (const DiskFileNotExist& dfne) {
        throw dfne;
    } catch (const exception& err) {
        throw this->_quarantine(
            quarantine_filename,
            string("Exception reading metadata: ") + err.what());
    }
}

FILE* DiskFile::_construct_from_data_file(const string& data_file,
                                          const string& meta_file) {
    FILE* fp = ::fopen(data_file, "rb");
    this->_datafile_metadata = this->_failsafe_read_metadata(fp, data_file);
    this->_metadata.clear();
    if (meta_file.length() > 0) {
        this->_metafile_metadata = this->_failsafe_read_metadata(
            meta_file, meta_file);
        map<string,string> sys_metadata;
        map<string,string>::const_iterator it =
            this->_datafile_metadata.begin();
        const map<string,string>::const_iterator itEnd =
            this->_datafile_metadata.end();
        for (; it != itEnd; ++it) {
            const string& key = (*it).first;
            bool add_pair = false;

            // if key.lower in DATAFILE_SYSTEM_META
            const string lower_key = StrUtils::lower(key);
            if (DATAFILE_SYSTEM_META.find(lower_key) !=
                DATAFILE_SYSTEM_META.end()) {
                add_pair = true;
            } else {
                if (is_sys_meta("object", key)) {
                    add_pair = true;
                }
            }

            if (add_pair) {
                sys_metadata.push_back(key, (*it).second);
            }
        }

        this->_metadata.update(this->_metafile_metadata);
        this->_metadata.update(sys_metadata);
        // diskfile writer added 'name' to metafile, so remove it here
        this->_metafile_metadata.pop("name", None);
    } else {
        this->_metadata.update(this->_datafile_metadata);
    }

    if (this->_name.empty()) {
        // If we don't know our name, we were just given a hash dir at
        // instantiation, so we'd better validate that the name hashes back
        // to us
        this->_name = this->_metadata["name"];
        this->_verify_name_matches_hash(data_file);
    }

    this->_verify_data_file(data_file, fp);
    return fp;
}

map<string,string>& DiskFile::get_metafile_metadata() {
    if (this->_metadata.empty()) {
        throw DiskFileNotOpen();
    }
    return this->_metafile_metadata;
}

map<string,string>& DiskFile::get_datafile_metadata() {
    if (this->_datafile_metadata.empty()) {
        throw DiskFileNotOpen();
    }
    return this->_datafile_metadata;
}

map<string,string>& DiskFile::get_metadata() {
    if (this->_metadata.empty()) {
        throw DiskFileNotOpen();
    }
    return this->_metadata;
}

map<string,string>& DiskFile::read_metadata() {
    DiskFileCloser closer(this->open());
    return this->get_metadata();
}

DiskFileReader* DiskFile::reader(bool keep_cache,
                          QuarantineHook* quarantine_hook) {
    //TODO: implement DiskFile::reader
    return NULL;
}

bool DiskFile::exists(const string& path) const {
    //TODO: implement DiskFile::exists
    return false;
}

void DiskFile::create(int size) {
    if (!exists(this->_tmpdir)) {
        SwiftUtils::mkdirs(this->_tmpdir);
    }

    int fd;
    string tmppath;

    try {
        fd, tmppath = mkstemp(this->_tmpdir);
    } catch (const OSError& err) {
        if (err._errno == ENOSPC ||
            err._errno == EDQUOT) {
            // No more inodes in filesystem
            throw DiskFileNoSpace();
        }
        throw err;
    }

    DiskFileWriter* dfw = NULL;

    try {
        if (size > 0) {
            int rc = ::posix_fallocate(fd, 0, size);
            if (rc != 0) {
                if (errno == ENOSPC ||
                    errno == EDQUOT) {
                    throw DiskFileNoSpace();
                }
                throw DiskFileNoAllocate();
            }
        }
        dfw = this->writer_cls(this->_name, this->_datadir, fd, tmppath,
                              this->_bytes_per_sync,
                              this->_threadpool,
                              this);
        yield dfw;
    }

    finally {
     
        ::close(fd);

        if ((dfw == NULL) || (!dfw->put_succeeded)) {
            // Try removing the temp file only if put did NOT succeed.
            //
            // dfw.put_succeeded is set to True after renamer() succeeds in
            // DiskFileWriter._finalize_put()
            if (0 != ::unlink(tmppath.c_str())) {
                this->_logger->exception(string("Error removing tempfile: ") +
                                         tmppath);
            }
        }
    }
}

void DiskFile::write_metadata(const map<string,string>& metadata) {
    DiskFileCloser closer(this->create());
    closer._df->_extension = ".meta";
    closer._df->put(metadata);
}

void DiskFile::delete_object(const string& timestamp) {
    // this is dumb, only tests send in strings
    timestamp = Timestamp(timestamp);
    DiskFileCloser closer(this->create());
    closer->_df->_extension = ".ts";
    closer->_df->put({"X-Timestamp": timestamp.internal});
}


