#include "uv_os.h"

#include <errno.h>
#include <fcntl.h>
#include <libgen.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <sys/vfs.h>
#include <unistd.h>
#include <uv.h>

#include "assert.h"
#include "err.h"
#include "syscall.h"
#include "uv_error.h"

/* Default permissions when creating a directory. */
#define DEFAULT_DIR_PERM 0700

int UvOsOpen(const char *path, int flags, int mode)
{
    struct uv_fs_s req;
    return uv_fs_open(NULL, &req, path, flags, mode, NULL);
}

int UvOsClose(uv_file fd)
{
    struct uv_fs_s req;
    return uv_fs_close(NULL, &req, fd, NULL);
}

int UvOsFallocate(uv_file fd, off_t offset, off_t len)
{
    int rv;
    rv = posix_fallocate(fd, offset, len);
    if (rv != 0) {
        /* From the manual page:
         *
         *   posix_fallocate() returns zero on success, or an error number on
         *   failure.  Note that errno is not set.
         */
        return -rv;
    }
    return 0;
}

int UvOsTruncate(uv_file fd, off_t offset)
{
    struct uv_fs_s req;
    return uv_fs_ftruncate(NULL, &req, fd, offset, NULL);
}

int UvOsFsync(uv_file fd)
{
    struct uv_fs_s req;
    return uv_fs_fsync(NULL, &req, fd, NULL);
}

int UvOsStat(const char *path, uv_stat_t *sb)
{
    struct uv_fs_s req;
    int rv;
    rv = uv_fs_stat(NULL, &req, path, NULL);
    if (rv != 0) {
        return rv;
    }
    memcpy(sb, &req.statbuf, sizeof *sb);
    return 0;
}

int UvOsWrite(uv_file fd,
              const uv_buf_t bufs[],
              unsigned int nbufs,
              int64_t offset)
{
    struct uv_fs_s req;
    return uv_fs_write(NULL, &req, fd, bufs, nbufs, offset, NULL);
}

int UvOsUnlink(const char *path)
{
    struct uv_fs_s req;
    return uv_fs_unlink(NULL, &req, path, NULL);
}

int UvOsRename(const char *path1, const char *path2)
{
    struct uv_fs_s req;
    return uv_fs_rename(NULL, &req, path1, path2, NULL);
}

void UvOsJoin(const char *dir, const char *filename, char *path)
{
    assert(UV__DIR_HAS_VALID_LEN(dir));
    assert(UV__FILENAME_HAS_VALID_LEN(filename));
    strcpy(path, dir);
    strcat(path, "/");
    strcat(path, filename);
}

int UvOsIoSetup(unsigned nr, aio_context_t *ctxp)
{
    int rv;
    rv = io_setup(nr, ctxp);
    if (rv == -1) {
        return -errno;
    }
    return 0;
}

int UvOsIoDestroy(aio_context_t ctx)
{
    int rv;
    rv = io_destroy(ctx);
    if (rv == -1) {
        return -errno;
    }
    return 0;
}

int UvOsIoSubmit(aio_context_t ctx, long nr, struct iocb **iocbpp)
{
    int rv;
    rv = io_submit(ctx, nr, iocbpp);
    if (rv == -1) {
        return -errno;
    }
    assert(rv == nr); /* TODO: can something else be returned? */
    return 0;
}

int UvOsIoGetevents(aio_context_t ctx,
                    long min_nr,
                    long max_nr,
                    struct io_event *events,
                    struct timespec *timeout)
{
    int rv;
    do {
        rv = io_getevents(ctx, min_nr, max_nr, events, timeout);
    } while (rv == -1 && errno == EINTR);

    if (rv == -1) {
        return -errno;
    }
    assert(rv >= min_nr);
    assert(rv <= max_nr);
    return rv;
}

int UvOsEventfd(unsigned int initval, int flags)
{
    int rv;
    rv = eventfd(initval, flags);
    if (rv == -1) {
        return -errno;
    }
    return rv;
}

int UvOsSetDirectIo(uv_file fd)
{
    int flags; /* Current fcntl flags */
    int rv;
    flags = fcntl(fd, F_GETFL);
    rv = fcntl(fd, F_SETFL, flags | UV_FS_O_DIRECT);
    if (rv == -1) {
        return -errno;
    }
    return 0;
}

int uvOpenFile(const char *dir,
               const char *filename,
               int flags,
               uv_file *fd,
               char **errmsg)
{
    struct uv_fs_s req;
    char path[UV__PATH_SZ];

    assert(UV__DIR_HAS_VALID_LEN(dir));
    assert(UV__FILENAME_HAS_VALID_LEN(filename));

    UvOsJoin(dir, filename, path);
    *fd = uv_fs_open(NULL, &req, path, flags, S_IRUSR | S_IWUSR, NULL);
    if (*fd < 0) {
        int rv = *fd;
        *fd = -1;
        *errmsg = uvSysErrMsg("open", rv);
        return rv == UV_ENOENT ? UV__NOENT : UV__ERROR;
    }
    return 0;
}

int uvReadFully(const int fd, void *buf, const size_t n, char **errmsg)
{
    int rv;
    rv = read(fd, buf, n);
    if (rv == -1) {
        *errmsg = uvSysErrMsg("read", -errno);
        return UV__ERROR;
    }
    assert(rv >= 0);
    if ((size_t)rv < n) {
        *errmsg = errMsgPrintf("short read: %d bytes instead of %ld", rv, n);
        return UV__NODATA;
    }
    return 0;
}

/* Check if direct I/O is possible on the given fd. */
static int probeDirectIO(int fd, size_t *size, char **errmsg)
{
    int flags;             /* Current fcntl flags. */
    struct statfs fs_info; /* To check the file system type. */
    void *buf;             /* Buffer to use for the probe write. */
    int rv;

    flags = fcntl(fd, F_GETFL);
    rv = fcntl(fd, F_SETFL, flags | O_DIRECT);

    if (rv == -1) {
        if (errno != EINVAL) {
            /* UNTESTED: the parameters are ok, so this should never happen. */
            *errmsg = uvSysErrMsg("fnctl", -errno);
            return UV__ERROR;
        }
        rv = fstatfs(fd, &fs_info);
        if (rv == -1) {
            /* UNTESTED: in practice ENOMEM should be the only failure mode */
            *errmsg = uvSysErrMsg("fstatfs", -errno);
            return UV__ERROR;
        }
        switch (fs_info.f_type) {
            case 0x01021994: /* TMPFS_MAGIC */
            case 0x2fc12fc1: /* ZFS magic */
                *size = 0;
                return 0;
            default:
                /* UNTESTED: this is an unsupported file system. */
                *errmsg = errMsgPrintf("unsupported file system: %lx",
                                       fs_info.f_type);
                return UV__ERROR;
        }
    }

    /* Try to peform direct I/O, using various buffer size. */
    *size = 4096;
    while (*size >= 512) {
        buf = raft_aligned_alloc(*size, *size);
        if (buf == NULL) {
            /* UNTESTED: TODO */
            *errmsg = errMsgPrintf("can't allocate write buffer");
            return UV__ERROR;
        }
        memset(buf, 0, *size);
        rv = write(fd, buf, *size);
        raft_free(buf);
        if (rv > 0) {
            /* Since we fallocate'ed the file, we should never fail because of
             * lack of disk space, and all bytes should have been written. */
            assert(rv == (int)(*size));
            return 0;
        }
        assert(rv == -1);
        if (errno != EIO && errno != EOPNOTSUPP) {
            /* UNTESTED: this should basically fail only because of disk errors,
             * since we allocated the file with posix_fallocate. */

            /* FIXME: this is a workaround because shiftfs doesn't return EINVAL
             * in the fnctl call above, for example when the underlying fs is
             * ZFS. */
            if (errno == EINVAL && *size == 4096) {
                *size = 0;
                return 0;
            }

            *errmsg = uvSysErrMsg("write", -errno);
            return UV__ERROR;
        }
        *size = *size / 2;
    }

    *size = 0;
    return 0;
}

#if defined(RWF_NOWAIT)
/* Check if fully non-blocking async I/O is possible on the given fd. */
static int probeAsyncIO(int fd, size_t size, bool *ok, char **errmsg)
{
    void *buf;                  /* Buffer to use for the probe write */
    aio_context_t ctx = 0;      /* KAIO context handle */
    struct iocb iocb;           /* KAIO request object */
    struct iocb *iocbs = &iocb; /* Because the io_submit() API sucks */
    struct io_event event;      /* KAIO response object */
    int n_events;
    int rv;

    /* Setup the KAIO context handle */
    rv = UvOsIoSetup(1, &ctx);
    if (rv != 0) {
        *errmsg = uvSysErrMsg("io_setup", rv);
        /* UNTESTED: in practice this should fail only with ENOMEM */
        return rv;
    }

    /* Allocate the write buffer */
    buf = raft_aligned_alloc(size, size);
    if (buf == NULL) {
        /* UNTESTED: define a configurable allocator that can fail? */
        *errmsg = errMsgPrintf("can't allocate write buffer");
        return UV__ERROR;
    }
    memset(buf, 0, size);

    /* Prepare the KAIO request object */
    memset(&iocb, 0, sizeof iocb);
    iocb.aio_lio_opcode = IOCB_CMD_PWRITE;
    *((void **)(&iocb.aio_buf)) = buf;
    iocb.aio_nbytes = size;
    iocb.aio_offset = 0;
    iocb.aio_fildes = fd;
    iocb.aio_reqprio = 0;
    iocb.aio_rw_flags |= RWF_NOWAIT | RWF_DSYNC;

    /* Submit the KAIO request */
    rv = UvOsIoSubmit(ctx, 1, &iocbs);
    if (rv != 0) {
        /* UNTESTED: in practice this should fail only with ENOMEM */
        raft_free(buf);
        UvOsIoDestroy(ctx);
        /* On ZFS 0.8 this is not properly supported yet. */
        if (errno == EOPNOTSUPP) {
            *ok = false;
            return 0;
        }
        *errmsg = errMsgPrintf("can't allocate write buffer");
        return rv;
    }

    /* Fetch the response: will block until done. */
    n_events = UvOsIoGetevents(ctx, 1, 1, &event, NULL);
    assert(n_events == 1);

    /* Release the write buffer. */
    raft_free(buf);

    /* Release the KAIO context handle. */
    rv = UvOsIoDestroy(ctx);
    if (rv != 0) {
        *errmsg = uvSysErrMsg("io_destroy", rv);
        return rv;
    }

    if (event.res > 0) {
        assert(event.res == (int)size);
        *ok = true;
    } else {
        /* UNTESTED: this should basically fail only because of disk errors,
         * since we allocated the file with posix_fallocate and the block size
         * is supposed to be correct. */
        assert(event.res != EAGAIN);
        *ok = false;
    }

    return 0;
}
#endif /* RWF_NOWAIT */

int uvProbeIoCapabilities(const char *dir,
                          size_t *direct,
                          bool *async,
                          char **errmsg)
{
    char filename[UV__FILENAME_LEN]; /* Filename of the probe file */
    char path[UV__PATH_SZ];          /* Full path of the probe file */
    int fd;                          /* File descriptor of the probe file */
    int rv;

    assert(UV__DIR_HAS_VALID_LEN(dir));

    /* Create a temporary probe file. */
    strcpy(filename, ".probe-XXXXXX");
    UvOsJoin(dir, filename, path);
    fd = mkstemp(path);
    if (fd == -1) {
        *errmsg = uvSysErrMsg("mkstemp", -errno);
        goto err;
    }
    rv = posix_fallocate(fd, 0, 4096);
    if (rv != 0) {
        *errmsg = uvSysErrMsg("posix_fallocate", -rv);
        goto err_after_file_open;
    }
    unlink(path);

    /* Check if we can use direct I/O. */
    rv = probeDirectIO(fd, direct, errmsg);
    if (rv != 0) {
        goto err_after_file_open;
    }

#if !defined(RWF_NOWAIT)
    /* We can't have fully async I/O, since io_submit might potentially block.
     */
    *async = false;
#else
    /* If direct I/O is not possible, we can't perform fully asynchronous
     * I/O, because io_submit might potentially block. */
    if (*direct == 0) {
        *async = false;
        goto out;
    }
    rv = probeAsyncIO(fd, *direct, async, errmsg);
    if (rv != 0) {
        goto err_after_file_open;
    }
#endif /* RWF_NOWAIT */

#if defined(RWF_NOWAIT)
out:
#endif /* RWF_NOWAIT */
    close(fd);
    return 0;

err_after_file_open:
    close(fd);
err:
    return UV__ERROR;
}
