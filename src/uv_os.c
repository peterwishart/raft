#include "uv_os.h"

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#if defined(linux) || defined(__linux__)
#include <libgen.h>
#include <sys/eventfd.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <sys/vfs.h>
#include <unistd.h>
#else
#include <io.h>
#endif
#include <uv.h>

#include "assert.h"
#include "err.h"
#include "syscall.h"

/* Default permissions when creating a directory. */
#define DEFAULT_DIR_PERM 0700

int UvOsOpen(const char *path, int flags, int mode, uv_file *fd)
{
    struct uv_fs_s req;
    int rv;
    rv = uv_fs_open(NULL, &req, path, flags, mode, NULL);
    if (rv < 0) {
        return rv;
    }
    *fd = rv;
    return 0;
}

int UvOsClose(uv_file fd)
{
    struct uv_fs_s req;
    return uv_fs_close(NULL, &req, fd, NULL);
}

/* Emulate fallocate(). Mostly taken from glibc's implementation. */
static int uvOsFallocateEmulation(int fd, off_t offset, off_t len)
{
#ifndef _MSC_VER
    ssize_t increment;
    struct statfs f;
    int rv;

    rv = fstatfs(fd, &f);
    if (rv != 0) {
        return errno;
    }

    if (f.f_bsize == 0) {
        increment = 512;
    } else if (f.f_bsize < 4096) {
        increment = f.f_bsize;
    } else {
        increment = 4096;
    }

    for (offset += (len - 1) % increment; len > 0; offset += increment) {
        len -= increment;
        rv = (int)pwrite(fd, "", 1, offset);
        if (rv != 1)
            return errno;
    }
#endif
    return 0;
}

int UvOsFallocate(uv_file fd, off_t offset, off_t len)
{
#ifdef _WIN32
    LARGE_INTEGER li;
    HANDLE fh = (HANDLE)_get_osfhandle(fd);
    li.QuadPart = (LONGLONG)offset + len;
    if(!SetFilePointerEx(fh, li, NULL, FILE_BEGIN)) {
        return GetLastError();
    }
    if(!SetEndOfFile(fh)) {
        return GetLastError();
    }
#else
    int rv;
    rv = posix_fallocate(fd, offset, len);
    if (rv != 0) {
        /* From the manual page:
         *
         *   posix_fallocate() returns zero on success, or an error number on
         *   failure.  Note that errno is not set.
         */
        if (rv != EOPNOTSUPP) {
            return -rv;
        }
        /* This might be a libc implementation (e.g. musl) that doesn't
         * implement a transparent fallback if fallocate() is not supported
         * by the underlying file system. */
        rv = uvOsFallocateEmulation(fd, offset, len);
        if (rv != 0) {
            return -EOPNOTSUPP;
        }
    }
#endif
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

int UvOsFdatasync(uv_file fd)
{
    struct uv_fs_s req;
    return uv_fs_fdatasync(NULL, &req, fd, NULL);
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
#ifdef _WIN32
    // todo: instead of relying on folder fsync, ensure rename is written to disk synchronously
    if (MoveFileEx(path1, path2,
                   MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH)) {
        return 0;
    }    
    // MoveFileEx failed, fallback to posix type rename in case of errors
    return uv_fs_rename(NULL, &req, path1, path2, NULL);
#else
    return uv_fs_rename(NULL, &req, path1, path2, NULL);    
#endif
}

void UvOsJoin(const char *dir, const char *filename, char *path)
{
    assert(UV__DIR_HAS_VALID_LEN(dir));
    assert(UV__FILENAME_HAS_VALID_LEN(filename));
    strcpy(path, dir);
    strcat(path, "/");
    strcat(path, filename);
}

int UvOsIoSetup(unsigned nr, uv_aio_ctx *ctxp)
{
    int rv;
#ifdef _MSC_VER
#else
    rv = io_setup(nr, ctxp);
    if (rv == -1) {
        return -errno;
    }
#endif
    return 0;
}

int UvOsIoDestroy(uv_aio_ctx ctx)
{
    int rv;
#ifdef _MSC_VER
#else
    rv = io_destroy(ctx);
    if (rv == -1) {
        return -errno;
    }
#endif
    return 0;
}

int UvOsIoSubmit(uv_aio_ctx ctx, long nr, uv_iocb **iocbpp)
{
    int rv;
#ifdef _MSC_VER
#else
    rv = io_submit(ctx, nr, iocbpp);
    if (rv == -1) {
        return -errno;
    }
    assert(rv == nr); /* TODO: can something else be returned? */
#endif
    return 0;
}

int UvOsIoGetevents(uv_aio_ctx ctx,
                    long min_nr,
                    long max_nr,
                    uv_io_event *events,
                    struct timespec *timeout)
{
    int rv;
#ifdef _MSC_VER
    rv = 0;
#else
    do {
        rv = io_getevents(ctx, min_nr, max_nr, events, timeout);
    } while (rv == -1 && errno == EINTR);

    if (rv == -1) {
        return -errno;
    }
    assert(rv >= min_nr);
    assert(rv <= max_nr);
#endif
    return rv;
}

int UvOsEventfd(unsigned int initval, int flags)
{
    int rv;
#ifdef _MSC_VER
    rv = CreateEvent(0, TRUE, initval, "asdftodo");
    if (rv == INVALID_HANDLE_VALUE) {
        return -errno;
    }
#else
    /* At the moment only UV_FS_O_NONBLOCK is supported */
    assert(flags == UV_FS_O_NONBLOCK);
    flags = EFD_NONBLOCK | EFD_CLOEXEC;
    rv = eventfd(initval, flags);
    if (rv == -1) {
        return -errno;
    }
#endif
    return rv;
}

int UvOsSetDirectIo(uv_file fd)
{
#ifndef _MSC_VER
    int flags; /* Current fcntl flags */
    int rv;
    flags = fcntl(fd, F_GETFL);
    rv = fcntl(fd, F_SETFL, flags | UV_FS_O_DIRECT);
    if (rv == -1) {
        return -errno;
    }
#endif
    return 0;
}
