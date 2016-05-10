import Logger;
import ThreadPool;
import DiskFile;


type
    BaseDiskFileReader* = ref object
        keep_cache: bool
        use_splice: bool
        started_at_0: bool
        read_to_eof: bool
        fp: File
        data_file: string
        obj_size: int
        pipe_size: int
        etag: string
        thread_pool: ThreadPool
        diskfile: DiskFile
        disk_chunk_size: int
        device_path: string
        logger: Logger
        #quarantine_hook: QuarantineHook
        bytes_read: int
        suppress_file_closing: bool
        quarantined_dir: string


    #[
    Encapsulation of the WSGI read context for servicing GET REST API
    requests. Serves as the context manager object for the
    :class:`swift.obj.diskfile.DiskFile` class's
    :func:`swift.obj.diskfile.DiskFile.reader` method.

    .. note::

        The quarantining behavior of this method is considered implementation
        specific, and is not required of the API.

    .. note::

        The arguments to the constructor are considered implementation
        specific. The API does not define the constructor arguments.

    :param fp: open file object pointer reference
    :param data_file: on-disk data file name for the object
    :param obj_size: verified on-disk size of the object
    :param etag: expected metadata etag value for entire file
    :param threadpool: thread pool to use for read operations
    :param disk_chunk_size: size of reads from disk in bytes
    :param keep_cache_size: maximum object size that will be kept in cache
    :param device_path: on-disk device path, used when quarantining an obj
    :param logger: logger caller wants this object to use
    :param quarantine_hook: 1-arg callable called w/reason when quarantined
    :param use_splice: if true, use zero-copy splice() to send data
    :param pipe_size: size of pipe buffer used in zero-copy operations
    :param diskfile: the diskfile creating this DiskFileReader instance
    :param keep_cache: should resulting reads be kept in the buffer cache
    ]#
method init(self: BaseDiskFileReader, fp: File, data_file: string, obj_size: int, etag: string, threadpool: ThreadPool,
                 disk_chunk_size: int, keep_cache_size: int, device_path: string, logger: Logger,
                 quarantine_hook: int, use_splice: bool, pipe_size: int, diskfile: DiskFile,
                 keep_cache: bool=false) {.base.} =
        # Parameter tracking
        self.fp = fp
        self.data_file = data_file
        self.obj_size = obj_size
        self.etag = etag
        self.threadpool = threadpool
        self.diskfile = diskfile
        self.disk_chunk_size = disk_chunk_size
        self.device_path = device_path
        self.logger = logger
        #self.quarantine_hook = quarantine_hook
        self.use_splice = use_splice
        self.pipe_size = pipe_size
        if keep_cache:
            # Caller suggests we keep this in cache, only do it if the
            # object's size is less than the maximum.
            self.keep_cache = (obj_size < keep_cache_size)
        else:
            self.keep_cache = false

        # Internal Attributes
        #self.iter_etag = nil
        self.bytes_read = 0
        self.started_at_0 = false
        self.read_to_eof = false
        #self.md5_of_sent_bytes = nil
        self.suppress_file_closing = false
        self.quarantined_dir = nil

#method manager*(self: BaseDiskFileReader) =
#        return self.diskfile.manager

iterator iter*(self: BaseDiskFileReader) =
        #[Returns an iterator over the data file.]#

        var
            dropped_cache: int

        try:
            dropped_cache = 0
            self.bytes_read = 0
            self.started_at_0 = false
            self.read_to_eof = false
            if self.fp.tell() == 0:
                self.started_at_0 = true
                self.iter_etag = hashlib.md5()
            while true:
                chunk = self.threadpool.run_in_thread(
                    self.fp.read, self.disk_chunk_size)
                if chunk:
                    if self.iter_etag:
                        self.iter_etag.update(chunk)
                    self.bytes_read += len(chunk)
                    if self.bytes_read - dropped_cache > DROP_CACHE_WINDOW:
                        self.drop_cache(self.fp.fileno(), dropped_cache,
                                         self.bytes_read - dropped_cache)
                        dropped_cache = self.bytes_read
                    yield chunk
                else:
                    self.read_to_eof = true
                    self.drop_cache(self.fp.fileno(), dropped_cache,
                                     self.bytes_read - dropped_cache)
                    break
        finally:
            if not self.suppress_file_closing:
                self.close()

method can_zero_copy_send*(self: BaseDiskFileReader): bool =
        return self.use_splice

method zero_copy_send*(self: BaseDiskFileReader, wsockfd) =
        #[
        Does some magic with splice() and tee() to move stuff from disk to
        network without ever touching userspace.

        :param wsockfd: file descriptor (integer) of the socket out which to
                        send data
        ]#
        # Note: if we ever add support for zero-copy ranged GET responses,
        # we'll have to make this conditional.
        self.started_at_0 = true

        rfd = self.fp.fileno()
        client_rpipe, client_wpipe = os.pipe()
        hash_rpipe, hash_wpipe = os.pipe()
        md5_sockfd = get_md5_socket()

        # The actual amount allocated to the pipe may be rounded up to the
        # nearest multiple of the page size. If we have the memory allocated,
        # we may as well use it.
        #
        # Note: this will raise IOError on failure, so we don't bother
        # checking the return value.
        pipe_size = fcntl.fcntl(client_rpipe, F_SETPIPE_SZ, self.pipe_size)
        fcntl.fcntl(hash_rpipe, F_SETPIPE_SZ, pipe_size)

        dropped_cache = 0
        self.bytes_read = 0
        try:
            while true:
                # Read data from disk to pipe
                (bytes_in_pipe, _1, _2) = self.threadpool.run_in_thread(
                    splice, rfd, None, client_wpipe, None, pipe_size, 0)
                if bytes_in_pipe == 0:
                    self.read_to_eof = true
                    self.drop_cache(rfd, dropped_cache,
                                     self.bytes_read - dropped_cache)
                    break
                self.bytes_read += bytes_in_pipe

                # "Copy" data from pipe A to pipe B (really just some pointer
                # manipulation in the kernel, not actual copying).
                bytes_copied = tee(client_rpipe, hash_wpipe, bytes_in_pipe, 0)
                if bytes_copied != bytes_in_pipe:
                    # We teed data between two pipes of equal size, and the
                    # destination pipe was empty. If, somehow, the destination
                    # pipe was full before all the data was teed, we should
                    # fail here. If we don't raise an exception, then we will
                    # have the incorrect MD5 hash once the object has been
                    # sent out, causing a false-positive quarantine.
                    raise Exception("tee() failed: tried to move %d bytes, "
                                    "but only moved %d" %
                                    (bytes_in_pipe, bytes_copied))
                # Take the data and feed it into an in-kernel MD5 socket. The
                # MD5 socket hashes data that is written to it. Reading from
                # it yields the MD5 checksum of the written data.
                #
                # Note that we don't have to worry about splice() returning
                # None here (which happens on EWOULDBLOCK); we're splicing
                # $bytes_in_pipe bytes from a pipe with exactly that many
                # bytes in it, so read won't block, and we're splicing it into
                # an MD5 socket, which synchronously hashes any data sent to
                # it, so writing won't block either.
                (hashed, _1, _2) = splice(hash_rpipe, None, md5_sockfd, None,
                                          bytes_in_pipe, splice.SPLICE_F_MORE)
                if hashed != bytes_in_pipe:
                    raise Exception("md5 socket didn't take all the data? "
                                    "(tried to write %d, but wrote %d)" %
                                    (bytes_in_pipe, hashed))

                while bytes_in_pipe > 0:
                    try:
                        res = splice(client_rpipe, None, wsockfd, None,
                                     bytes_in_pipe, 0)
                        bytes_in_pipe -= res[0]
                    except IOError as exc:
                        if exc.errno == errno.EWOULDBLOCK:
                            trampoline(wsockfd, write=true)
                        else:
                            raise

                if self.bytes_read - dropped_cache > DROP_CACHE_WINDOW:
                    self.drop_cache(rfd, dropped_cache,
                                     self.bytes_read - dropped_cache)
                    dropped_cache = self.bytes_read
        finally:
            # Linux MD5 sockets return '00000000000000000000000000000000' for
            # the checksum if you didn't write any bytes to them, instead of
            # returning the correct value.
            if self.bytes_read > 0:
                bin_checksum = os.read(md5_sockfd, 16)
                hex_checksum = ''.join("%02x" % ord(c) for c in bin_checksum)
            else:
                hex_checksum = MD5_OF_EMPTY_STRING
            self.md5_of_sent_bytes = hex_checksum

            os.close(client_rpipe)
            os.close(client_wpipe)
            os.close(hash_rpipe)
            os.close(hash_wpipe)
            os.close(md5_sockfd)
            self.close()

iterator app_iter_range*(self: BaseDiskFileReader, start, stop) =
        #[Returns an iterator over the data file for range (start, stop)]#
        if start or start == 0:
            self.fp.seek(start)
        if stop is not None:
            length = stop - start
        else:
            length = None
        try:
            for chunk in self:
                if length is not None:
                    length -= len(chunk)
                    if length < 0:
                        # Chop off the extra:
                        yield chunk[:length]
                        break
                yield chunk
        finally:
            if not self.suppress_file_closing:
                self.close()

iterator app_iter_ranges*(self: BaseDiskFileReader, ranges, content_type, boundary, size) =
        #[Returns an iterator over the data file for a set of ranges]#
        if not ranges:
            yield ""
        else:
            try:
                self.suppress_file_closing = true
                for chunk in multi_range_iterator(
                        ranges, content_type, boundary, size,
                        self.app_iter_range):
                    yield chunk
            finally:
                self.suppress_file_closing = false
                self.close()

method drop_cache*(self: BaseDiskFileReader, fd, offset, length) =
        #[Method for no-oping buffer cache drop method.]#
        if not self.keep_cache:
            drop_buffer_cache(fd, offset, length)

method quarantine*(self: BaseDiskFileReader, msg: string) =
        self.quarantined_dir = self.threadpool.run_in_thread(
            self.manager.quarantine_renamer, self.device_path,
            self.data_file)
        self.logger.warning("Quarantined object %s: %s" % (
            self.data_file, msg))
        self.logger.increment('quarantines')
        self.quarantine_hook(msg)

method handle_close_quarantine*(self: BaseDiskFileReader) =
        #[Check if file needs to be quarantined]#
        if self.iter_etag and not self.md5_of_sent_bytes:
            self.md5_of_sent_bytes = self.iter_etag.hexdigest()

        if self.bytes_read != self.obj_size:
            self.quarantine(
                "Bytes read: %s, does not match metadata: %s" % (
                    self.bytes_read, self.obj_size))
        elif self.md5_of_sent_bytes and \
                self.etag != self.md5_of_sent_bytes:
            self.quarantine(
                "ETag %s and file's md5 %s do not match" % (
                    self.etag, self.md5_of_sent_bytes))

method close*(self: BaseDiskFileReader) =
        #[
        Close the open file handle if present.

        For this specific implementation, this method will handle quarantining
        the file if necessary.
        ]#
        if self.fp:
            try:
                if self.started_at_0 and self.read_to_eof:
                    self.handle_close_quarantine()
            except DiskFileQuarantined:
                raise
            except (Exception, Timeout) as e:
                self.logger.error(_(
                    'ERROR DiskFile %(data_file)s'
                    ' close failure: %(exc)s : %(stack)s'),
                    {'exc': e, 'stack': ''.join(traceback.format_stack()),
                     'data_file': self.data_file})
            finally:
                close(self.fp)
                fp = nil
                self.fp = nil


