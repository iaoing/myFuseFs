#ifndef _POSIX_IOSTORE_H_
#define _POSIX_IOSTORE_H_

#include <string>
#include <dirent.h>
#include <sys/types.h>
#include <utime.h>
#include "bing_log.h"
using namespace std;

class PosixIOStore{
public:
    ~PosixIOStore(){};
    int Fstat(const char *path, int fd, struct stat* buf);
    int Fsync(const char *path, int fd);
    int Ftruncate(const char *path, int fd, off_t length);
    // int GetDataBuf(void **bufp, size_t length);
    int Pread(const char *path, int fd, void* buf, size_t count, off_t offset, ssize_t *bytes_read);
    int Pwrite(const char *path, int fd, const void* buf, size_t count, off_t offset, ssize_t *bytes_written);
    int Read(const char *path, int fd, void *buf, size_t count, ssize_t *bytes_read);
    // int ReleaseDataBuf(void *buf, size_t length);
    int Size(const char *path, int fd, off_t *ret_offset);
    int Write(const char *path, int fd, const void* buf, size_t len, ssize_t *bytes_written);
    int Close(const char *path, int fd);

    int Readdir_r(const char *path, DIR *dp, struct dirent *dst, struct dirent **dret);
    int Closedir(const char *path, DIR *dp);

    int Access(const char *path, int amode);
    int Chmod(const char* path, mode_t mode);
    int Chown(const char *path, uid_t owner, gid_t group);
    int Lchown(const char *path, uid_t owner, gid_t group);
    int Lstat(const char* path, struct stat* buf);
    int Mkdir(const char* path, mode_t mode);
    int Open(const char *bpath, int flags, mode_t mode);
    int Opendir(const char *bpath);
    int Rename(const char*, const char*);
    int Rmdir(const char*);
    int Stat(const char*, struct stat*);
    int Statvfs(const char*, struct statvfs*);
    int Symlink(const char*, const char*);
    int Readlink(const char*, char*, size_t, ssize_t *);
    int Truncate(const char*, off_t);
    int Unlink(const char*);
    int Utime(const char*, const utimbuf*);

    // other api for open and opendir;
    int Open_fd(const char *bpath, int flags, mode_t mode);
    DIR *Opendir_dir(const char *bpath);

private:
	bingLog BLog;
};


#endif
