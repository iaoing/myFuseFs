#include <errno.h>   /* error# ok */
#include <fcntl.h>
#include <stdio.h>
#include <sys/mman.h>
#include <sys/statvfs.h>
#include <string>
#include <sys/stat.h>
#include <dirent.h>
using namespace std;

#  include <errno.h>
#  ifndef errno
#     define errno errno
#  endif

#include "posix_iostore.h"

#define POSIX_IO_ENTER(X) \
    BLog.log("POSIXIO_INFO: %s Entering %s: %s\n", __FILE__, __FUNCTION__, X);

#define POSIX_IO_EXIT(X, Y) \
    BLog.log("POSIXIO_INFO: %s Exiting %s: %s - %lld\n", __FILE__, __FUNCTION__, X, (long long int)Y);
/*
 * IOStore functions that return plfs_error_t should return PLFS_SUCCESS on success
 * and PLFS_E* on error.   The POSIX API uses 0 for success, -1 for failure
 * with the error code in the global error number variable.   This macro
 * translates POSIX to IOStore.
 */
#define get_err(X) (((X) >= 0) ? 0 : errno)  /* error# ok */

int
PosixIOStore::Close(const char *path, int fd) {
    POSIX_IO_ENTER(path);
    int rv;
    rv = close(fd);
    POSIX_IO_EXIT(path,rv);
    return(get_err(rv));
}

int
PosixIOStore::Fstat(const char *path, int fd, struct stat* buf) {
    POSIX_IO_ENTER(path);
    int rv;
    rv = fstat(fd, buf);
    POSIX_IO_EXIT(path,rv);
    return(get_err(rv));
}

int
PosixIOStore::Fsync(const char *path, int fd) {
    POSIX_IO_ENTER(path);
    int rv;
    rv = fsync(fd);
    POSIX_IO_EXIT(path,rv);
    return(get_err(rv));
}

int
PosixIOStore::Ftruncate(const char *path, int fd, off_t length) {
    POSIX_IO_ENTER(path);
    int rv;
    rv = ftruncate(fd, length);
    POSIX_IO_EXIT(path,rv);
    return(get_err(rv));
}

// int
// PosixIOStore::GetDataBuf(void **bufp, size_t length) {
//     POSIX_IO_ENTER(path);
//     void *b;
//     int ret = 0;

//     b = mmap(NULL, length, PROT_READ, MAP_SHARED|MAP_NOCACHE, fd, 0);
//     if (b == MAP_FAILED) {
//         ret = -1;
//     }else{
//         *bufp = b;
//     }
//     POSIX_IO_EXIT(path,ret);
//     return(get_err(ret));
// }

int
PosixIOStore::Pread(const char *path, int fd, void* buf, size_t count, off_t offset, ssize_t *bytes_read) {
    POSIX_IO_ENTER(path);
    ssize_t rv;
    rv = pread(fd, buf, count, offset);
    POSIX_IO_EXIT(path,rv);
    *bytes_read = rv;
    return(get_err(rv));
}

int
PosixIOStore::Pwrite(const char *path, int fd, const void* buf, size_t count,
                       off_t offset, ssize_t *bytes_written) {
    POSIX_IO_ENTER(path);
    ssize_t rv;
    rv = pwrite(fd, buf, count, offset);
    POSIX_IO_EXIT(path,rv);
    *bytes_written = rv;
    return(get_err(rv));
}

int
PosixIOStore::Read(const char *path, int fd, void *buf, size_t count, ssize_t *bytes_read) {
    POSIX_IO_ENTER(path);
    ssize_t rv;
    rv = read(fd, buf, count);
    POSIX_IO_EXIT(path,rv);
    *bytes_read = rv;
    return(get_err(rv));
}

// int
// PosixIOStore::ReleaseDataBuf(void *addr, size_t length)
// {
//     POSIX_IO_ENTER(path);
//     int rv;
//     rv = munmap(addr, length);
//     POSIX_IO_EXIT(path,rv);
//     return(get_err(rv));
// }

int
PosixIOStore::Size(const char *path, int fd, off_t *ret_offset) {
    POSIX_IO_ENTER(path);
    off_t rv;
    rv = lseek(fd, 0, SEEK_END);
    POSIX_IO_EXIT(path,rv);
    *ret_offset = rv;
    return(get_err(rv));
}

int
PosixIOStore::Write(const char *path, int fd, const void* buf, size_t len, ssize_t *bytes_written) {
    POSIX_IO_ENTER(path);
    ssize_t rv;
    rv = write(fd, buf, len);
    POSIX_IO_EXIT(path,rv);
    *bytes_written = rv;
    return(get_err(rv));
}

int
PosixIOStore::Closedir(const char *path, DIR *dp) {
    POSIX_IO_ENTER(path);
    int rv;
    rv = closedir(dp);
    POSIX_IO_EXIT(path,rv);
    return(get_err(rv));
}
    
int
PosixIOStore::Readdir_r(const char *path, DIR *dp, struct dirent *dst, struct dirent **dret) {
    POSIX_IO_ENTER(path);
    int rv;
    rv = readdir_r(dp, dst, dret);
    /* note: readdir_r returns 0 on success, err on failure */
    POSIX_IO_EXIT(path,rv);
    return((rv));
}

int
PosixIOStore::Access(const char *path, int amode) {
    POSIX_IO_ENTER(path);
    int rv;
    rv = access(path, amode);
    POSIX_IO_EXIT(path,rv);
    return(get_err(rv));
}

int
PosixIOStore::Chmod(const char* path, mode_t mode) {
    POSIX_IO_ENTER(path);
    int rv;
    rv = chmod(path, mode);
    POSIX_IO_EXIT(path,rv);
    return(get_err(rv));
}

int
PosixIOStore::Chown(const char *path, uid_t owner, gid_t group) {
    POSIX_IO_ENTER(path);
    int rv;
    rv = chown(path, owner, group);
    POSIX_IO_EXIT(path,rv);
    return(get_err(rv));
}

int
PosixIOStore::Lchown(const char *path, uid_t owner, gid_t group) {
    POSIX_IO_ENTER(path);
    int rv;
    rv = lchown(path, owner, group);
    POSIX_IO_EXIT(path,rv);
    return(get_err(rv));
}

int
PosixIOStore::Lstat(const char* path, struct stat* buf) {
    POSIX_IO_ENTER(path);
    int rv;
    rv = lstat(path, buf);
    POSIX_IO_EXIT(path,rv);
    return(get_err(rv));
}

int
PosixIOStore::Mkdir(const char* path, mode_t mode) {
    POSIX_IO_ENTER(path);
    int rv;
    rv = mkdir(path, mode);
    POSIX_IO_EXIT(path,rv);
    return(get_err(rv));
}

int
PosixIOStore::Open(const char *bpath, int flags, mode_t mode) {
    POSIX_IO_ENTER(bpath);
    int rv = 0;
    int fd;
    fd = open(bpath, flags, mode);
    if (fd < 0) {
        return(errno);
    }
    close(fd);

    POSIX_IO_EXIT(bpath,0);
    return rv;
}

int
PosixIOStore::Opendir(const char *bpath) {
    POSIX_IO_ENTER(bpath);
    DIR *dp;
    int ret;
    dp = opendir(bpath);
    if (!dp) {
        ret = errno;
    }else{
        ret = 0;
        closedir(dp);
    }
    POSIX_IO_EXIT(bpath,ret);
    return (ret);
}

int
PosixIOStore::Rename(const char *oldpath, const char *newpath) {
    POSIX_IO_ENTER(oldpath);
    int rv;
    rv = rename(oldpath, newpath);
    POSIX_IO_EXIT(oldpath,rv);
    return(get_err(rv));
}

int
PosixIOStore::Rmdir(const char* path) {
    POSIX_IO_ENTER(path);
    int rv;
    rv = rmdir(path);
    POSIX_IO_EXIT(path,rv);
    return(get_err(rv));
}

int
PosixIOStore::Stat(const char* path, struct stat* buf) {
    POSIX_IO_ENTER(path);
    int rv;
    rv = stat(path, buf);
    POSIX_IO_EXIT(path,rv);
    return(get_err(rv));
}

int
PosixIOStore::Statvfs( const char *path, struct statvfs* stbuf ) {
    POSIX_IO_ENTER(path);
    int rv;
    rv = statvfs(path, stbuf);
    POSIX_IO_EXIT(path,rv);
    return(get_err(rv));
}

int
PosixIOStore::Symlink(const char* oldpath, const char* newpath) {
    POSIX_IO_ENTER(oldpath);
    int rv;
    rv = symlink(oldpath, newpath);
    POSIX_IO_EXIT(oldpath,rv);
    return(get_err(rv));
}

int
PosixIOStore::Readlink(const char *link, char *buf, size_t bufsize, ssize_t *readlen) {
    POSIX_IO_ENTER(link);
    ssize_t rv;
    rv = readlink(link, buf, bufsize);
    POSIX_IO_EXIT(link,rv);
    *readlen = rv;
    return(get_err(rv));
}

int
PosixIOStore::Truncate(const char* path, off_t length) {
    POSIX_IO_ENTER(path);
    int rv;
    rv = truncate(path, length);
    POSIX_IO_EXIT(path,rv);
    return(get_err(rv));
}

int
PosixIOStore::Unlink(const char* path) {
    POSIX_IO_ENTER(path);
    int rv;
    rv = unlink(path);
    POSIX_IO_EXIT(path,rv);
    return(get_err(rv));
}

int
PosixIOStore::Utime(const char* path, const struct utimbuf *times) {
    POSIX_IO_ENTER(path);
    int rv;
    rv = utime(path, times);
    POSIX_IO_EXIT(path,rv);
    return(get_err(rv));
}
