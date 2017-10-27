#include <errno.h>
#include <string>
#include <fstream>
#include <iostream>
#include <fcntl.h>
#include <iostream>
#include <limits>
#include <assert.h>
#include <stdlib.h>
#include <sys/fsuid.h>
#include <sys/types.h>
#include <sys/dir.h>
#include <sys/syscall.h>
#include <sys/param.h>
#include <sys/mount.h>
#include <sys/statvfs.h>
#include <sys/time.h>
#include <time.h>
#include <pwd.h>
#include <grp.h>
#include <map>
#include <iomanip>
#include <iostream>
#include <fstream>
#include <sstream>
#include "plfs_fuse.h"
#include "fusexx.h"

#ifdef HAVE_SYS_FSUID_H
#include <sys/fsuid.h>
#endif


using namespace std;

#define GET_GROUPS get_groups(&orig_groups);
#define SET_GROUPS(X) set_groups(X);
#define RESTORE_IDS    SET_IDS(save_uid,save_gid);
#define SAVE_IDS uid_t s_uid = plfs_getuid(); gid_t s_gid = plfs_getgid();
#define SET_IDS(X,Y)   plfs_setfsuid( X );    plfs_setfsgid( Y );
#define RESTORE_GROUPS if(getuid() == 0) setgroups( orig_groups.size(),         \
                                (const gid_t*)&(orig_groups.front()));


// #define FUSE_PLFS_ENTER self->BLog.log("FUSE_PLFS_ENTER, %s\n", __FUNCTION__);  \
//                    vector<gid_t> orig_groups;                                   \
//                    GET_GROUPS;                                                  \
//                    SET_GROUPS(fuse_get_context()->uid);                         \
//                    SAVE_IDS;                                                    \
//                    SET_IDS(fuse_get_context()->uid,fuse_get_context()->gid);    


// #define FUSE_PLFS_EXIT  self->BLog.log("FUSE_PLFS_EXIT, %s\n", __FUNCTION__);  \
//                    SET_IDS(s_uid,s_gid);                                       \
//                    RESTORE_GROUPS;                                      

#define FUSE_PLFS_ENTER {;}
#define FUSE_PLFS_EXIT  {;}


std::vector<std::string> &
split(const std::string& s, const char delim, std::vector<std::string> &elems)
{
    std::stringstream ss(s);
    std::string item;
    while(std::getline(ss, item, delim)) {
        elems.push_back(item);
    }
    return elems;
}

// Constructor
Plfs::Plfs ()
{
    ;
}

// set this up to parse command line args
// move code from constructor in here
// and stop using /etc config file
// return 0 or -errno
int Plfs::init( int *argc, char **argv )
{
    struct timeval time;
    if ( gettimeofday( &time, NULL ) != 0 ) {
        ;
    }
    double cur_time = (double)time.tv_sec + time.tv_usec/1.e6;    
    // create a dropping so we know when we start
    mode_t mode = (S_IRUSR|S_IWUSR|S_IXUSR|S_IXGRP|S_IXOTH);
    int fd = open( "/tmp/bing_fuse.starttime",
                   O_WRONLY | O_APPEND | O_CREAT, mode );
    char buffer[1024];
    snprintf( buffer, 1024, "bing_fuse started at %.2f\n", cur_time );
    if (write( fd, buffer, strlen(buffer) ) < 0) {
        /*ignore it*/;
    }
    close( fd );
    // init our mutex
    pthread_mutex_init( &(fd_mutex), NULL );
    pthread_mutex_init( &(modes_mutex), NULL );
    pthread_rwlock_init( &(write_lock), NULL );
    pthread_mutex_init( &(group_mutex), NULL );
    // we used to make a trash container but now that we moved to library,
    // fuse layer doesn't handle silly rename
    // we also have (temporarily?) removed the dangler stuff
    return 0;
}

int Plfs::f_access(const char *path, int mask)
{
    FUSE_PLFS_ENTER;
    self->BLog.log("Line:%05d, Begin %s, line %d\n", __LINE__, __FUNCTION__, __LINE__);
    string strPath = expandPath(path);
    self->BLog.log("%s, %d => path:%s, strPath:%s\n", __FUNCTION__, __LINE__, path, strPath.c_str());
    
    int ret = 0;
    struct stat st;
    mode_t mode;
    ret = lstat(strPath.c_str(), &st);
    if(ret == 0){
        mode = st.st_mode;
    }else{
        mode = 0;
    }

    mode_t open_mode = 0;
    if(S_ISDIR(mode)){
        ret = access(strPath.c_str(), mask);
    }else if(S_ISREG(mode)){
        ret = access(strPath.c_str(), F_OK);
        if(ret == 0){
            if(checkMask(mask,W_OK|R_OK)) {
                open_mode = O_RDWR;
            } else if(checkMask(mask,R_OK)||checkMask(mask,X_OK)) {
                open_mode = O_RDONLY;
            } else if(checkMask(mask,W_OK)) {
                open_mode = O_WRONLY;
            } else if(checkMask(mask,F_OK)) {
                FUSE_PLFS_EXIT;
                return 0;   // we already know this
            }
            int fd = open(strPath.c_str(), open_mode, 0777);
            if(fd < 0){
                FUSE_PLFS_EXIT;
                return fd;
            }
            close(fd);
        }
        FUSE_PLFS_EXIT;
        return ret;
    }else if(S_ISLNK(mode)){
        ret = access(strPath.c_str(), mask);
    }else{
        ret = ENOSYS;
    }

    self->BLog.log("%s, %d => path:%s, strPath:%s, ret:%d\n", __FUNCTION__, __LINE__, path, strPath.c_str(), ret);
    FUSE_PLFS_EXIT;
    return ret ;
}

int Plfs::f_mknod(const char *path, mode_t mode, dev_t rdev)
{
    FUSE_PLFS_ENTER;
    self->BLog.log("Line:%05d, Begin %s, line %d\n", __LINE__, __FUNCTION__, __LINE__);
    string strPath = expandPath(path);
    int fd;
    fd = open(strPath.c_str(), O_CREAT|O_TRUNC|O_WRONLY, mode);
    if (fd < 0) {
        self->BLog.log("%s, %d => path:%s, strPath:%s\n", __FUNCTION__, __LINE__, path, strPath.c_str());
        FUSE_PLFS_EXIT;
        return -1;        
    }
    close(fd);
    self->BLog.log("%s, %d => path:%s, strPath:%s\n", __FUNCTION__, __LINE__, path, strPath.c_str());
    FUSE_PLFS_EXIT;
    return 0;
}

int Plfs::f_create(const char *path, mode_t /*  mode */, 
                   struct fuse_file_info * /* fi */)
{
    FUSE_PLFS_ENTER;
    self->BLog.log("Line:%05d, Begin %s, line %d\n", __LINE__, __FUNCTION__, __LINE__);
    self->BLog.log("%s, %d => path:%s\n", __FUNCTION__, __LINE__, path);
    FUSE_PLFS_EXIT;
    return -ENOSYS;
}

// returns 0 or -err
// nothing to do for a read file
int Plfs::f_fsync(const char *path, int /* datasync */, struct fuse_file_info *fi)
{
    FUSE_PLFS_ENTER;
    self->BLog.log("Line:%05d, Begin %s, line %d\n", __LINE__, __FUNCTION__, __LINE__);
    // string strPath = expandPath(path);
    std::map<string, OpenFile*>::iterator it;
    it = self->open_files.find(path);
    if(it == self->open_files.end())
    {
        self->BLog.log("%s, %d => path:%s\n", __FUNCTION__, __LINE__, path);
        FUSE_PLFS_EXIT;
        return 0;
    }
    self->BLog.log("%s, %d => path:%s\n", __FUNCTION__, __LINE__, path);
    FUSE_PLFS_EXIT;
    return fsync(it->second->fd);
}

// this means it is an open file.  That means we also need to check our
// current write file and adjust those indices also if necessary
int Plfs::f_ftruncate(const char *path, off_t offset,
                      struct fuse_file_info *fi)
{
    FUSE_PLFS_ENTER;
    self->BLog.log("Line:%05d, Begin %s, line %d\n", __LINE__, __FUNCTION__, __LINE__);
    string strPath = expandPath(path);
    std::map<string, OpenFile*>::iterator it;
    it = self->open_files.find(path);
    if(it == self->open_files.end())
    {
        self->BLog.log("%s, %d => path:%s, strPath:%s\n", __FUNCTION__, __LINE__, path, strPath.c_str());
        FUSE_PLFS_EXIT;
        return truncate(strPath.c_str(), offset);
    }    
    self->BLog.log("%s, %d => path:%s, strPath:%s\n", __FUNCTION__, __LINE__, path, strPath.c_str());
    FUSE_PLFS_EXIT;
    return ftruncate(it->second->fd, offset);
}

// use removeDirectoryTree to remove all data but not the dir structure
// return 0 or -err
int Plfs::f_truncate( const char *path, off_t offset )
{
    FUSE_PLFS_ENTER;
    self->BLog.log("Line:%05d, Begin %s, line %d\n", __LINE__, __FUNCTION__, __LINE__);
    string strPath = expandPath(path);
    std::map<string, OpenFile*>::iterator it;
    it = self->open_files.find(path);
    if(it == self->open_files.end())
    {
        self->BLog.log("%s, %d => path:%s, strPath:%s\n", __FUNCTION__, __LINE__, path, strPath.c_str());
        FUSE_PLFS_EXIT;
        return truncate(strPath.c_str(), offset);
    }    
    self->BLog.log("%s, %d => path:%s, strPath:%s\n", __FUNCTION__, __LINE__, path, strPath.c_str());
    FUSE_PLFS_EXIT;
    return ftruncate(it->second->fd, offset);
}

int Plfs::f_fgetattr(const char *path, struct stat *stbuf,
                     struct fuse_file_info *fi)
{
    FUSE_PLFS_ENTER;
    self->BLog.log("Line:%05d, Begin %s, line %d\n", __LINE__, __FUNCTION__, __LINE__);
    int ret;
    string strPath = expandPath(path);
    std::map<string, OpenFile*>::iterator fd_it;
    fd_it = self->open_files.find(path);
    if ( fd_it == self->open_files.end() ) {
        makeNewFile(path, 0777);
        ret = lstat(strPath.c_str(), stbuf);
        self->BLog.log("%s, %d => path:%s, strPath:%s, ret:%d\n", __FUNCTION__, __LINE__, path, strPath.c_str(), ret);        
        FUSE_PLFS_EXIT;
        return ret;
    }else{
        ret = fstat(fd_it->second->fd, stbuf);
    }
    self->BLog.log("%s, %d => path:%s, strPath:%s, ret:%d\n", __FUNCTION__, __LINE__, path, strPath.c_str(), ret);
    FUSE_PLFS_EXIT;
    return ret;

}
int Plfs::f_getattr(const char *path, struct stat *stbuf)
{
    FUSE_PLFS_ENTER;
    self->BLog.log("Line:%05d, Begin %s, line %d\n", __LINE__, __FUNCTION__, __LINE__);
    int ret;
    string strPath = expandPath(path);
    std::map<string, OpenFile*>::iterator fd_it;
    fd_it = self->open_files.find(path);
    if ( fd_it == self->open_files.end() ) {
        makeNewFile(path, 0777);
        ret = lstat(strPath.c_str(), stbuf);
        self->BLog.log("%s, %d => path:%s, strPath:%s, ret:%d\n", __FUNCTION__, __LINE__, path, strPath.c_str(), ret);
        FUSE_PLFS_EXIT;
        return ret;
    }else{
        ret = fstat(fd_it->second->fd, stbuf);
    }
    self->BLog.log("%s, %d => path:%s, strPath:%s, ret:%d\n", __FUNCTION__, __LINE__, path, strPath.c_str(), ret);
    FUSE_PLFS_EXIT;
    return ret;
}

// needs to work differently for directories
int Plfs::f_utime (const char *path, struct utimbuf *ut)
{
    FUSE_PLFS_ENTER;
    self->BLog.log("Line:%05d, Begin %s, line %d\n", __LINE__, __FUNCTION__, __LINE__);
    string strPath = expandPath(path);
    self->BLog.log("%s, %d => path:%s, strPath:%s\n", __FUNCTION__, __LINE__, path, strPath.c_str());
    FUSE_PLFS_EXIT;
    return utime(strPath.c_str(), ut);
}

// this needs to recurse on all data and index files
int Plfs::f_chmod (const char *path, mode_t mode)
{
    FUSE_PLFS_ENTER;
    self->BLog.log("Line:%05d, Begin %s, line %d\n", __LINE__, __FUNCTION__, __LINE__);
    string strPath = expandPath(path);
    FUSE_PLFS_EXIT;
    return chmod(strPath.c_str(), mode);
}

int Plfs::f_chown (const char *path, uid_t uid, gid_t gid )
{
    FUSE_PLFS_ENTER;
    self->BLog.log("Line:%05d, Begin %s, line %d\n", __LINE__, __FUNCTION__, __LINE__);
    string strPath = expandPath(path);
    self->BLog.log("%s, %d => path:%s, strPath:%s\n", __FUNCTION__, __LINE__, path, strPath.c_str());
    FUSE_PLFS_EXIT;
    return chown(strPath.c_str(), uid, gid);
}

int Plfs::f_mkdir (const char *path, mode_t mode )
{
    FUSE_PLFS_ENTER;
    self->BLog.log("Line:%05d, Begin %s, line %d\n", __LINE__, __FUNCTION__, __LINE__);
    string strPath = expandDirPath(path);
    self->BLog.log("%s, %d => path:%s, strPath:%s\n", __FUNCTION__, __LINE__, path, strPath.c_str());
    FUSE_PLFS_EXIT;
    return mkdir(strPath.c_str(), mode);
}

int Plfs::f_rmdir( const char *path )
{
    FUSE_PLFS_ENTER;
    self->BLog.log("Line:%05d, Begin %s, line %d\n", __LINE__, __FUNCTION__, __LINE__);
    string strPath = expandDirPath(path);
    self->BLog.log("%s, %d => path:%s, strPath:%s\n", __FUNCTION__, __LINE__, path, strPath.c_str());
    FUSE_PLFS_EXIT;
    return rmdir(strPath.c_str());
}

// what if someone is calling unlink on an open file?
// boy I hope that never happens.  Actually, I think this should be OK
// because I believe that f_write will recreate the container if necessary.
// but not sure what will happen on a file open for read.
//
// anyway, not sure we need to worry about handling this weird stuff
// fine to leave it undefined.  users shouldn't do stupid stuff like this anyway
int Plfs::f_unlink( const char *path )
{
    FUSE_PLFS_ENTER;
    self->BLog.log("Line:%05d, Begin %s, line %d\n", __LINE__, __FUNCTION__, __LINE__);
    string strPath = expandPath(path);
    self->BLog.log("%s, %d => path:%s, strPath:%s\n", __FUNCTION__, __LINE__, path, strPath.c_str());
    FUSE_PLFS_EXIT;
    return unlink(strPath.c_str());
}

// see f_readdir for some documentation here
// returns 0 or -err
int Plfs::f_opendir( const char *path, struct fuse_file_info *fi )
{
    FUSE_PLFS_ENTER;
    self->BLog.log("Line:%05d, Begin %s, line %d\n", __LINE__, __FUNCTION__, __LINE__);
    string strPath = expandDirPath(path);
    OpenDir *fi_dir = new OpenDir;
    fi_dir->last_offset = 0;
    fi->fh = (uint64_t)NULL;

    ///////////
    DIR *this_dir;
    struct dirent entry;
    struct dirent* entryPtr = NULL;
    set<string>* names = ((set<string>*)&(fi_dir->entries));    
    int ret = 0;

    this_dir = opendir(strPath.c_str());
    if(!this_dir) {
        ret = -1;
        self->BLog.log("%s, %d => path:%s, strPath:%s, ret:%d\n", __FUNCTION__, __LINE__, path, strPath.c_str(), ret);
        FUSE_PLFS_EXIT;
        return ret;
    }

    while ((ret = readdir_r(this_dir, &entry, &entryPtr)) == 0 && entryPtr != NULL) {
        // if ((!strcmp(entryPtr->d_name,".") ||
        //                   !strcmp(entryPtr->d_name,".."))) {
        //     continue;   // skip the dots
        // }
        string file;
        file = entryPtr->d_name;
        names->insert(file);        
    } 
    closedir(this_dir);

    //////////
    if (ret == 0) {
        fi->fh = (uint64_t)fi_dir;
    } else {
        delete fi_dir;
    }
    self->BLog.log("%s, %d => path:%s, strPath:%s, ret:%d\n", __FUNCTION__, __LINE__, path, strPath.c_str(), ret);
    FUSE_PLFS_EXIT;
    return ret;
}

int Plfs::f_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
                    off_t offset, struct fuse_file_info *fi)
{
    FUSE_PLFS_ENTER;
    self->BLog.log("Line:%05d, Begin %s, line %d\n", __LINE__, __FUNCTION__, __LINE__);
    string strPath = expandDirPath(path);
    OpenDir *fi_dir = (OpenDir *)fi->fh;
    // struct dirent entry;
    // struct dirent* entryPtr = NULL;

    bool EOD = false; // are we at end of directory already?
    // skip out early if they're already read to end
    if (offset >= (off_t)fi_dir->entries.size()) {
        EOD = true;
    }

    ///////////
    DIR *this_dir;
    struct dirent entry;
    struct dirent* entryPtr = NULL;
    set<string>* names = ((set<string>*)&(fi_dir->entries));     
    int ret = 0;
    if (!EOD && fi_dir->last_offset > offset) {
        fi_dir->last_offset = offset;
        fi_dir->entries.clear();
        
        this_dir = opendir(strPath.c_str());
        if(!this_dir) {
            ret = -1;
            self->BLog.log("%s, %d => path:%s, strPath:%s, ret:%d\n", __FUNCTION__, __LINE__, path, strPath.c_str(), ret);
            FUSE_PLFS_EXIT;
            return ret;
        }        

        while ((ret = readdir_r(this_dir, &entry, &entryPtr)) == 0 && entryPtr != NULL) {
            // if ((!strcmp(entryPtr->d_name,".") ||
            //                   !strcmp(entryPtr->d_name,".."))) {
            //     continue;   // skip the dots
            // }
            string file;
            file = entryPtr->d_name;
            names->insert(file);            
        } 
        closedir(this_dir);
    }
    
    //////////
    set<string>::iterator itr;
    int i = 0;
    for(itr = fi_dir->entries.begin();
            ! EOD && ret == 0 && itr != fi_dir->entries.end(); itr++,i++) {
        fi_dir->last_offset = i;
        if ( i >= offset ) {
            if ( 0 != filler(buf,(*itr).c_str(),NULL,i+1) ) {
                break;
            }
        }
    }

    self->BLog.log("%s, %d => path:%s, strPath:%s\n", __FUNCTION__, __LINE__, path, strPath.c_str());
    FUSE_PLFS_EXIT;
    return ret;
}

int Plfs::f_releasedir( const char *path, struct fuse_file_info *fi )
{
    FUSE_PLFS_ENTER;
    self->BLog.log("Line:%05d, Begin %s, line %d\n", __LINE__, __FUNCTION__, __LINE__);
    if (fi->fh) {
        delete (OpenDir *)fi->fh;
    }
    self->BLog.log("%s, %d => path:%s\n", __FUNCTION__, __LINE__, path);
    FUSE_PLFS_EXIT;
    return 0;

}
// returns 0 or -err
// O_WRONLY and O_RDWR are handled as a write
// O_RDONLY is handled as a read
// PLFS is optimized for O_WRONLY and tries to do OK for O_RDONLY
// O_RDWR is optimized for writes but the reads might be horrible
int Plfs::f_open(const char *path, struct fuse_file_info *fi)
{
    FUSE_PLFS_ENTER;
    self->BLog.log("Line:%05d, Begin %s, line %d\n", __LINE__, __FUNCTION__, __LINE__);
    string strPath = expandPath(path);
    fi->fh = (uint64_t)NULL;
    bool newly_created = false;
    mode_t mode = get_mod( path );
    int fd = 0;

    // if(f_access(path, F_OK) != 0){
    //     if(f_mknod(path, 0664) != 0){
    //         return -1;
    //     }
    // }

    pthread_mutex_lock( &self->fd_mutex );
    string pathHash = string(path);
    std::map<string, OpenFile*>::iterator fd_it;
    fd_it = self->open_files.find(pathHash);
    struct OpenFile *of = new OpenFile;
    if ( fd_it == self->open_files.end() ) {
        newly_created = true;
        fd = open( strPath.c_str(), fi->flags, mode );
    }else{
        fd = fd_it->second->fd;
        of = fd_it->second;
    }

    if ( fd != 0 ) {
        of->fd      = fd;
        of->pid     = fuse_get_context()->pid;
        of->uid     = fuse_get_context()->uid;
        of->gid     = fuse_get_context()->gid;
        of->flags   = fi->flags;
        fi->fh = (uint64_t)of;
        if ( newly_created ) {
            of->wr_ref = 1;
            self->open_files[pathHash] = of;
        }else{
            of->wr_ref++;
        }
    }
    pthread_mutex_unlock( &self->fd_mutex );
    // we can safely add more writers to an already open file
    // bec FUSE checks f_access before allowing an f_open
    self->BLog.log("%s, %d => path:%s, strPath:%s, fd:%d\n", __FUNCTION__, __LINE__, path, strPath.c_str(), fd);
    FUSE_PLFS_EXIT;
    return fd;
}

int Plfs::f_release( const char *path, struct fuse_file_info *fi )
{
    FUSE_PLFS_ENTER;
    self->BLog.log("Line:%05d, Begin %s, line %d\n", __LINE__, __FUNCTION__, __LINE__);
    // string strPath = expandPath(path);
    struct OpenFile *openfile = (struct OpenFile*)fi->fh; 
    int fd = 0;                                   
    if ( openfile ) {                                     
        fd = openfile->fd;
        --(openfile->wr_ref);
    }  

    if ( fd ) {
        SET_IDS(    openfile->uid, openfile->gid );
        SET_GROUPS( openfile->uid );        
        pthread_mutex_lock( &self->fd_mutex );
        assert( openfile->flags == fi->flags );
 
        if(openfile->wr_ref == 0){
            close(fd);
            self->open_files.erase( path );
        }
        fi->fh = (uint64_t)NULL;

        delete openfile;
        openfile = NULL;
        pthread_mutex_unlock( &self->fd_mutex );
    }
    self->BLog.log("%s, %d => path:%s\n", __FUNCTION__, __LINE__, path);
    FUSE_PLFS_EXIT;
    return 0;
}

int Plfs::f_write(const char *path, const char *buf, size_t size, off_t offset,
                  struct fuse_file_info *fi)
{
    FUSE_PLFS_ENTER;
    self->BLog.log("Line:%05d, Begin %s, line %d\n", __LINE__, __FUNCTION__, __LINE__);
    string strPath = expandPath(path);
    std::map<string, OpenFile*>::iterator fd_it;
    fd_it = self->open_files.find(path);
    if ( fd_it == self->open_files.end() ) {
        self->BLog.log("%s, %d => path:%s, strPath:%s\n", __FUNCTION__, __LINE__, path, strPath.c_str());
        FUSE_PLFS_EXIT;
        return -1;
    }
    struct OpenFile *openfile = fd_it->second;

    int ret = 0;
    ssize_t bytes_written;

    pthread_rwlock_wrlock( &self->write_lock );
    bytes_written = pwrite( openfile->fd, buf, size, offset );
    pthread_rwlock_unlock( &self->write_lock );
    ret = (bytes_written == size) ? bytes_written : -1;

    self->BLog.log("%s, %d => path:%s, strPath:%s\n", __FUNCTION__, __LINE__, path, strPath.c_str());
    FUSE_PLFS_EXIT;
    return ret;
}

int Plfs::f_readlink (const char *path, char *buf, size_t bufsize)
{
    FUSE_PLFS_ENTER;
    self->BLog.log("Line:%05d, Begin %s, line %d\n", __LINE__, __FUNCTION__, __LINE__);
    string strPath = expandPath(path);
    int ret;
    int len;
    len = readlink(strPath.c_str(), buf, bufsize);
    ret = (len == bufsize) ? len : -1;
    if ( ret > 0 ) {
        ret = 0;
    }

    self->BLog.log("%s, %d => path:%s, strPath:%s\n", __FUNCTION__, __LINE__, path, strPath.c_str());
    FUSE_PLFS_EXIT;
    return ret;
}

// not support hard link;
int Plfs::f_link( const char *path, const char *to )
{
    FUSE_PLFS_ENTER;
    self->BLog.log("Line:%05d, Begin %s, line %d\n", __LINE__, __FUNCTION__, __LINE__);
    self->BLog.log("%s, %d => path:%s, to:%s\n", 
            __FUNCTION__, __LINE__, path, to);    
    FUSE_PLFS_EXIT;
    return -1;
}

int Plfs::f_symlink( const char *path, const char *to )
{
    FUSE_PLFS_ENTER;
    self->BLog.log("Line:%05d, Begin %s, line %d\n", __LINE__, __FUNCTION__, __LINE__);
    int ret = 0;
    string strPath   = expandPath(path);
    string strPathTo = expandPath(to);
    ret = symlink(strPath.c_str(), strPathTo.c_str());

    self->BLog.log("%s, %d => path:%s, strPath:%s, to:%s, strPathTo:%s\n", 
            __FUNCTION__, __LINE__, path, strPath.c_str(), to, strPathTo.c_str());
    FUSE_PLFS_EXIT;
    return ret;
}

int Plfs::f_statfs(const char *path, struct statvfs *stbuf)
{
    FUSE_PLFS_ENTER;
    self->BLog.log("Line:%05d, Begin %s, line %d\n", __LINE__, __FUNCTION__, __LINE__);
    string strPath = expandPath(path);
    int ret;
    ret = statvfs(strPath.c_str(), stbuf);
    // ret = statvfs(BMPOINT, stbuf);

    self->BLog.log("%s, %d => path:%s, strPath:%s, ret:%d\n", __FUNCTION__, __LINE__, path, strPath.c_str(), ret);
    FUSE_PLFS_EXIT;
    return ret;
}

// returns bytes read or -err
int Plfs::f_readn(const char *path, char *buf, size_t size, off_t offset,
                  struct fuse_file_info *fi)
{
    FUSE_PLFS_ENTER;
    self->BLog.log("Line:%05d, Begin %s, line %d\n", __LINE__, __FUNCTION__, __LINE__);
    int ret;
    std::map<string, OpenFile*>::iterator fd_it;
    fd_it = self->open_files.find(path);
    if ( fd_it == self->open_files.end() ) {
        self->BLog.log("%s, %d => path:%s\n", __FUNCTION__, __LINE__, path);
        FUSE_PLFS_EXIT;
        return -1;
    }
    struct OpenFile *of = fd_it->second;

    // calls plfs_sync to flush in-memory index.
    if (of) {
        fsync( of->fd );
    }
    // syncIfOpen(strPath);
    ssize_t bytes_read;
    bytes_read = pread( of->fd, buf, size, offset );
    ret = (bytes_read == size) ? bytes_read : -1;

    self->BLog.log("%s, %d => path:%s\n", __FUNCTION__, __LINE__, path);
    FUSE_PLFS_EXIT;
    return ret;
}

int Plfs::f_flush( const char *path, struct fuse_file_info *fi )
{
    FUSE_PLFS_ENTER;
    self->BLog.log("Line:%05d, Begin %s, line %d\n", __LINE__, __FUNCTION__, __LINE__);
    int ret = 0;
    std::map<string, OpenFile*>::iterator fd_it;
    fd_it = self->open_files.find(path);
    if ( fd_it == self->open_files.end() ) {
        self->BLog.log("%s, %d => path:%s\n", __FUNCTION__, __LINE__, path);
        FUSE_PLFS_EXIT;
        return 0;
    }
    struct OpenFile *of = fd_it->second;

    // calls plfs_sync to flush in-memory index.
    if (of) {
        ret = fsync( of->fd );
    }

    self->BLog.log("%s, %d => path:%s\n", __FUNCTION__, __LINE__, path);
    FUSE_PLFS_EXIT;
    return ret;
}

int Plfs::f_rename( const char *path, const char *to )
{
    FUSE_PLFS_ENTER;
    self->BLog.log("Line:%05d, Begin %s, line %d\n", __LINE__, __FUNCTION__, __LINE__);
    int ret = 0;
    string strPath = expandPath(path);
    string strPathTo = expandPath(to);
    std::map<string, OpenFile*>::iterator fd_it;
    fd_it = self->open_files.find(path);
    struct OpenFile *of;  
    if ( fd_it != self->open_files.end() ) {
        of = fd_it->second;  
    }
    
       

    pthread_mutex_lock( &self->fd_mutex );
    if(of && of->wr_ref > 1){
        FUSE_PLFS_EXIT;
        return -1;
    }
    ret = rename(strPath.c_str(), strPathTo.c_str());
    if(ret == 0){
        self->open_files[to] = of;
        self->open_files.erase( path );
    }
    pthread_mutex_unlock( &self->fd_mutex );
    
    self->BLog.log("%s, %d => path:%s, strPath:%s, to:%s, strPathTo:%s\n", 
            __FUNCTION__, __LINE__, path, strPath.c_str(), to, strPathTo.c_str());    
    FUSE_PLFS_EXIT;
    return ret;

}

// ===============================================================

mode_t Plfs::get_mod(const char* path)
{
    self->BLog.log("Line:%05d, Begin %s, line %d\n", __LINE__, __FUNCTION__, __LINE__);
    string strPath = expandPath(path);
    mode_t mode = 0;
    int ret;
    pthread_mutex_lock( &self->modes_mutex );
    std::map<string, mode_t>::iterator itr = self->known_modes.find( path );
    if ( itr == self->known_modes.end() ) {
        struct stat stbuf;
        ret = lstat(strPath.c_str(), &stbuf);
        if (ret == 0) {
            mode = stbuf.st_mode;
        }
        self->known_modes[path] = mode;
    } else {
        mode = itr->second;
    }
    pthread_mutex_unlock( &self->modes_mutex );

    self->BLog.log("%s, %d => path:%s, strPath:%s, mode:%d\n", __FUNCTION__, __LINE__, path, strPath.c_str(), mode);
    return mode;    
}

string Plfs::expandPath( const char *path )
{
    self->BLog.log("Line:%05d, Begin %s, line %d\n", __LINE__, __FUNCTION__, __LINE__);

    string full_logical = "";
    if(! strncmp(path, "/mnt/bing", sizeof("/mnt/bing"))){
        // already absolute;
        string s1 = path;
        std::vector<string> vc;
        split(s1, '/', vc);
        
        int size = vc.size();
        int subp = -1;
        for(int i = 0; i< size; ++i){
            if(vc[i] == "bing"){
                subp = i;
                break;
            }
        }
        if(subp != -1){
            vc[subp] = "bing_store";
        }

        
        size = vc.size();
        for(int i = 0; i < size; ++i){
            if(vc[i] == ""){
                ;
            }else{
                full_logical += "/" + vc[i];
            }
        }
    }else{
        full_logical = string("/mnt/bing_store/") + path; 
    }

    if(full_logical[0] != '/')
        full_logical = string("/") + full_logical;
    while(full_logical.find("//") != full_logical.npos){
        full_logical.replace( full_logical.find("//"), 2, "/", 1 );
    }    

    self->BLog.log("%s, %d => path:%s, strPath:%s\n", __FUNCTION__, __LINE__, path, full_logical.c_str());
    return full_logical;
}

string Plfs::expandDirPath( const char *path )
{
    self->BLog.log("Line:%05d, Begin %s, line %d\n", __LINE__, __FUNCTION__, __LINE__);

    string full_logical = "";
    if(! strncmp(path, "/mnt/bing", sizeof("/mnt/bing"))){
        // already absolute;
        string s1 = path;
        std::vector<string> vc;
        split(s1, '/', vc);
        
        int size = vc.size();
        int subp = -1;
        for(int i = 0; i< size; ++i){
            if(vc[i] == "bing"){
                subp = i;
                break;
            }
        }
        if(subp != -1){
            vc[subp] = "bing_store";
        }

        
        size = vc.size();
        for(int i = 0; i < size; ++i){
            if(vc[i] == ""){
                ;
            }else{
                full_logical += "/" + vc[i];
            }
        }
    }else{
        full_logical = string("/mnt/bing_store/") + path; 
    }

    if(full_logical[0] != '/')
        full_logical = string("/") + full_logical;
    while(full_logical.find("//") != full_logical.npos){
        full_logical.replace( full_logical.find("//"), 2, "/", 1 );
    }

    self->BLog.log("%s, %d => path:%s, strPath:%s\n", __FUNCTION__, __LINE__, path, full_logical.c_str());
    return full_logical;
}


bool Plfs::checkMask(int mask, int value)
{
    self->BLog.log("%s, %d => mask:%d, value:%d\n", __FUNCTION__, __LINE__, mask, value);
    return (mask& value||mask==value);
}

int Plfs::makeNewFile(const char *path, mode_t mode)
{
    return 0;
    self->BLog.log("Line:%05d, Begin %s, line %d\n", __LINE__, __FUNCTION__, __LINE__);
    if(strncmp(path, "/.Trash", strlen("/.Trash")) == 0){
        return 0;
    }
    string strPath = expandPath(path);
    int fd;
    fd = open(strPath.c_str(), O_CREAT|O_TRUNC|O_WRONLY, mode);
    if (fd < 0) {
        return -1;
        self->BLog.log("%s, %d => path:%s, strPath:%s\n", __FUNCTION__, __LINE__, path, strPath.c_str());
    }
    close(fd);
    self->BLog.log("%s, %d => path:%s, strPath:%s\n", __FUNCTION__, __LINE__, path, strPath.c_str());
    return 0;
}


// fills the set of supplementary groups of the effective uid
int Plfs::get_groups( vector<gid_t> *vec )
{
    self->BLog.log("Line:%05d, Begin %s, line %d\n", __LINE__, __FUNCTION__, __LINE__);
    int ngroups = getgroups(0, 0);
    gid_t *groups = new gid_t[ngroups];
    //(gid_t *) calloc(1, ngroups * sizeof (gid_t));
    int val = getgroups (ngroups, groups);
    //int val = fuse_getgroups(ngroups, groups);
    for( int i = 0; i < val; i++ ) {
        vec->push_back( groups[i] );
    }
    delete []groups;
    groups = NULL;
    self->BLog.log("Begin %s, line %d, ret:%d\n", __FUNCTION__, __LINE__, ( val >= 0 ? 0 : -1 ));
    return ( val >= 0 ? 0 : -1 );  /* error# ok */
}

int Plfs::set_groups( uid_t uid )
{
    self->BLog.log("Line:%05d, Begin %s, line %d, uid:%d\n", __LINE__, __FUNCTION__, __LINE__, uid);
    char *username;
    struct passwd *pwd;
    vector<gid_t> groups;
    vector<gid_t> *groups_ptr = NULL;
    static double age = getTime();
    // unfortunately, I think this whole thing needs to be in a mutex
    // it used to be the case that we only had the mutex around the
    // code to read the groups and the lookup was unprotected
    // but now we need to periodically purge the data-structure and
    // I'm not sure the data-structure is thread-safe
    // what if we get an itr, and then someone else frees the structure,
    // and then we try to dereference the itr?
    pthread_mutex_lock( &self->group_mutex );
    // purge the cache every 30 seconds
    if ( getTime() - age > 30 ) {
        self->memberships.clear();
        age = getTime();
    }
    // do the lookup
    map<uid_t, vector<gid_t> >::iterator itr =
        self->memberships.find( uid );
    // if not found, find it and cache it
    if ( itr == self->memberships.end() ) {
        pwd      = getpwuid( uid );
        if( pwd ) {
            self->BLog.log("%s, %d, Need to find groups for %d \n", __FUNCTION__, __LINE__ , (int)uid);            
            username = pwd->pw_name;
            // read the groups to discover the memberships of the caller
            struct group *grp;
            char         **members;
            setgrent();
            while( (grp = getgrent()) != NULL ) {
                members = grp->gr_mem;
                while (*members) {
                    if ( strcmp( *(members), username ) == 0 ) {
                        groups.push_back( grp->gr_gid );
                    }
                    members++;
                }
            }
            endgrent();
            self->memberships[uid] = groups;
            groups_ptr = &groups;
        }
    } else {
        groups_ptr = &(itr->second);
    }
    // now unlock the mutex, set the groups, and return
    pthread_mutex_unlock( &self->group_mutex );
    if ( groups_ptr == NULL) {
        self->BLog.log("%s, %d, WTF: Got a null group ptr for %d \n", __FUNCTION__, __LINE__ , (int)uid); 
    } else {
        if(getuid() == 0) {
            setgroups( groups_ptr->size(), (const gid_t *)&(groups_ptr->front()) );
        }
    }
    self->BLog.log("%s, line %d, ret:0\n", __FUNCTION__, __LINE__);
    return 0;
}


uid_t Plfs::plfs_getuid()
{
    self->BLog.log("Line:%05d, Begin %s, line %d\n", __LINE__, __FUNCTION__, __LINE__);
    return getuid();
}

gid_t Plfs::plfs_getgid()
{
    self->BLog.log("Line:%05d, Begin %s, line %d\n", __LINE__, __FUNCTION__, __LINE__);
    return getgid();
}


int Plfs::plfs_setfsuid(uid_t u)
{
    int uid;
    uid = setfsuid(u);
    self->BLog.log("Line:%05d, Begin %s, line %d, ret uid:%d\n", __LINE__, __FUNCTION__, __LINE__, uid);
    return uid;
}

int Plfs::plfs_setfsgid(gid_t g)
{
    int gid;
    gid = setfsgid(g);
    self->BLog.log("Line:%05d, Begin %s, line %d, ret gid:%d\n", __LINE__, __FUNCTION__, __LINE__, gid);
    return gid;
}


double Plfs::getTime( )
{
    // shoot this seems to be solaris only
    // how does MPI_Wtime() work?
    //return 1.0e-9 * gethrtime();
    self->BLog.log("%s, %d \n", __FUNCTION__, __LINE__);
    struct timeval time;
    if ( gettimeofday( &time, NULL ) != 0 ) {
        self->BLog.log("%s, %d \n", __FUNCTION__, __LINE__);
    }
    return (double)time.tv_sec + time.tv_usec/1.e6;
}