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

#define GET_GROUPS          do{get_groups(&orig_groups);}while(0);
#define SET_GROUPS(X)       do{set_groups(X);}while(0);
#define RESTORE_IDS         do{SET_IDS(save_uid,save_gid);}while(0);
#define SAVE_IDS            do{s_uid = plfs_getuid(); s_gid = plfs_getgid();}while(0);
#define SET_IDS(X,Y)        do{plfs_setfsuid( X );    plfs_setfsgid( Y );}while(0);
#define RESTORE_GROUPS      do{if(getuid() == 0) setgroups( orig_groups.size(), (const gid_t*)&(orig_groups.front()));}while(0);


// #define FUSE_PLFS_ENTER                     \
//         uid_t s_uid;                        \
//         gid_t s_gid;                        \
//         std::vector<gid_t> orig_groups;     \
//         do{                                 \
//             self->BLog.log("\nFUSE_PLFS_ENTER, %s\n", __FUNCTION__);      \
//             GET_GROUPS;                                                   \
//             SET_GROUPS(fuse_get_context()->uid);                          \
//             SAVE_IDS;                                                     \
//             SET_IDS(fuse_get_context()->uid, fuse_get_context()->gid);    \
//         }while(0)


// #define FUSE_PLFS_EXIT      \
//         do{                 \
//             self->BLog.log("FUSE_PLFS_EXIT, %s, ret: %d\n\n", __FUNCTION__, ret);  \
//             SET_IDS(s_uid, s_gid);                                                 \
//             RESTORE_GROUPS;                                                        \
//         }while(0) 

#define FUSE_PLFS_ENTER {self->BLog.log("\nFUSE_PLFS_ENTER, %s\n", __FUNCTION__);}
#define FUSE_PLFS_EXIT  {self->BLog.log("FUSE_PLFS_EXIT, %s, ret: %d\n\n", __FUNCTION__, ret);}


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
    pthread_mutex_init( &(of_wr_ref), NULL );
    // we used to make a trash container but now that we moved to library,
    // fuse layer doesn't handle silly rename
    // we also have (temporarily?) removed the dangler stuff
    return 0;
}

int Plfs::f_access(const char *path, int mask)
{
    FUSE_PLFS_ENTER;
    self->BLog.log("%s, %d\n", __FUNCTION__, __LINE__);
    string strPath = expandPath(path);
    self->BLog.log("%s, %d => path:%s, strPath:%s\n", __FUNCTION__, __LINE__, path, strPath.c_str());
    
    int ret = 0;
    struct stat st;
    mode_t mode;
    ret = self->PIO.Lstat(strPath.c_str(), &st);
    if(ret == 0){
        mode = st.st_mode;
    }else{
        mode = 0;
    }

    mode_t open_mode = 0;
    if(S_ISDIR(mode)){
        ret = self->PIO.Access(strPath.c_str(), mask);
    }else if(S_ISREG(mode)){
        ret = self->PIO.Access(strPath.c_str(), F_OK);
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
            ret = self->PIO.Open(strPath.c_str(), open_mode, 0777);
        }
        FUSE_PLFS_EXIT;
        return -ret;
    }else if(S_ISLNK(mode)){
        ret = self->PIO.Access(strPath.c_str(), mask);
    }

    self->BLog.log("%s, %d => path:%s, strPath:%s, ret:%d\n", __FUNCTION__, __LINE__, path, strPath.c_str(), ret);
    FUSE_PLFS_EXIT;
    return -ret ;
}

int Plfs::f_mknod(const char *path, mode_t mode, dev_t rdev)
{
    FUSE_PLFS_ENTER;
    self->BLog.log("%s, %d\n", __FUNCTION__, __LINE__);
    string strPath = expandPath(path);
    int ret;
    ret = self->PIO.Open(strPath.c_str(), O_CREAT|O_TRUNC|O_WRONLY, mode | S_IWUSR);
    // ret = self->PIO.Open(strPath.c_str(), O_CREAT|O_TRUNC|O_WRONLY, 0777);
    if(ret == 0){
        pthread_mutex_lock( &self->modes_mutex );
        self->known_modes[path] = mode;
        pthread_mutex_unlock( &self->modes_mutex );        
    }
    self->BLog.log("%s, %d => path:%s, strPath:%s, ret:%d\n", __FUNCTION__, __LINE__, path, strPath.c_str(), ret);
    FUSE_PLFS_EXIT;
    return -ret;
}

int Plfs::f_create(const char *path, mode_t /*  mode */, 
                   struct fuse_file_info * /* fi */)
{
    FUSE_PLFS_ENTER;
    int ret = -ENOSYS;
    self->BLog.log("%s, %d\n", __FUNCTION__, __LINE__);
    self->BLog.log("%s, %d => path:%s\n", __FUNCTION__, __LINE__, path);
    FUSE_PLFS_EXIT;
    return ret;
}

// returns 0 or -err
// nothing to do for a read file
int Plfs::f_fsync(const char *path, int /* datasync */, struct fuse_file_info *fi)
{
    FUSE_PLFS_ENTER;
    int ret = 0;
    self->BLog.log("%s, %d\n", __FUNCTION__, __LINE__);
    string strPath = expandPath(path);
    // std::map<string, OpenFile*>::iterator it;
    // it = self->open_files.find(path);
    // if(it == self->open_files.end())
    // {
    //     self->BLog.log("%s, %d => path:%s\n", __FUNCTION__, __LINE__, path);
    //     FUSE_PLFS_EXIT;
    //     return 0;
    // }
    struct OpenFile *of = (struct OpenFile*)fi->fh;
    if(of){
        ret = self->PIO.Fsync(strPath.c_str(), of->fd);
    }
    self->BLog.log("%s, %d => path:%s\n", __FUNCTION__, __LINE__, path);
    FUSE_PLFS_EXIT;
    return 0;
}

// this means it is an open file.  That means we also need to check our
// current write file and adjust those indices also if necessary
int Plfs::f_ftruncate(const char *path, off_t offset,
                      struct fuse_file_info *fi)
{
    FUSE_PLFS_ENTER;
    int ret = 0;
    self->BLog.log("%s, %d\n", __FUNCTION__, __LINE__);
    string strPath = expandPath(path);
    // std::map<string, OpenFile*>::iterator it;
    // it = self->open_files.find(path);
    // if(it == self->open_files.end())
    // {
    //     ret = self->PIO.Truncate(strPath.c_str(), offset);
    //     self->BLog.log("%s, %d => path:%s, strPath:%s, ret:%d\n", __FUNCTION__, __LINE__, path, strPath.c_str(), ret);
    //     FUSE_PLFS_EXIT;
    //     return -ret;
    // }
    struct OpenFile *of = (struct OpenFile*)fi->fh;
    if(of)    {
        ret = self->PIO.Fsync(strPath.c_str(), of->fd);
        ret = self->PIO.Ftruncate(strPath.c_str(), of->fd, offset);
    }else{
        ret = self->PIO.Truncate(strPath.c_str(), offset);
    }
    
    self->BLog.log("%s, %d => path:%s, strPath:%s, ret:%d\n", __FUNCTION__, __LINE__, path, strPath.c_str(), ret);
    FUSE_PLFS_EXIT;
    return -ret;
}

// use removeDirectoryTree to remove all data but not the dir structure
// return 0 or -err
int Plfs::f_truncate( const char *path, off_t offset )
{
    FUSE_PLFS_ENTER;
    int ret = 0;
    self->BLog.log("%s, %d\n", __FUNCTION__, __LINE__);
    string strPath = expandPath(path);
    // std::map<string, OpenFile*>::iterator it;
    // it = self->open_files.find(path);
    // if(it == self->open_files.end())
    // {
    //     ret = self->PIO.Truncate(strPath.c_str(), offset);
    //     self->BLog.log("%s, %d => path:%s, strPath:%s, ret:%d\n", __FUNCTION__, __LINE__, path, strPath.c_str(), ret);
    //     FUSE_PLFS_EXIT;
    //     return -ret;
    // }
    ret = self->PIO.Truncate(strPath.c_str(), offset);
    
    self->BLog.log("%s, %d => path:%s, strPath:%s, ret:%d\n", __FUNCTION__, __LINE__, path, strPath.c_str(), ret);
    FUSE_PLFS_EXIT;
    return -ret;
}

int Plfs::f_fgetattr(const char *path, struct stat *stbuf,
                     struct fuse_file_info *fi)
{
    FUSE_PLFS_ENTER;
    self->BLog.log("%s, %d\n", __FUNCTION__, __LINE__);
    int ret = 0;
    string strPath = expandPath(path);
    struct OpenFile *of = (struct OpenFile*)fi->fh;
    if ( of == NULL ) {
        ret = self->PIO.Lstat(strPath.c_str(), stbuf);
    }else{
        ret = self->PIO.Fstat(strPath.c_str(), of->fd, stbuf);
    }
    self->BLog.log("%s, %d => path:%s, strPath:%s, ret:%d\n", __FUNCTION__, __LINE__, path, strPath.c_str(), ret);
    FUSE_PLFS_EXIT;
    return -ret;

}
int Plfs::f_getattr(const char *path, struct stat *stbuf)
{
    FUSE_PLFS_ENTER;
    self->BLog.log("%s, %d\n", __FUNCTION__, __LINE__);
    int ret = 0;
    string strPath = expandPath(path);
    ret = self->PIO.Lstat(strPath.c_str(), stbuf);
    self->BLog.log("%s, %d => path:%s, strPath:%s, ret:%d\n", __FUNCTION__, __LINE__, path, strPath.c_str(), ret);
    FUSE_PLFS_EXIT;
    return -ret;
}

// needs to work differently for directories
int Plfs::f_utime (const char *path, struct utimbuf *ut)
{
    FUSE_PLFS_ENTER;
    int ret = 0;
    self->BLog.log("%s, %d\n", __FUNCTION__, __LINE__);
    string strPath = expandPath(path);
    ret = self->PIO.Utime(strPath.c_str(), ut);
    self->BLog.log("%s, %d => path:%s, strPath:%s\n", __FUNCTION__, __LINE__, path, strPath.c_str());
    FUSE_PLFS_EXIT;
    return -ret;
}

// this needs to recurse on all data and index files
int Plfs::f_chmod (const char *path, mode_t mode)
{
    FUSE_PLFS_ENTER;
    int ret = 0;
    self->BLog.log("%s, %d\n", __FUNCTION__, __LINE__);
    string strPath = expandPath(path);
    ret = self->PIO.Chmod(strPath.c_str(), mode);
    if(ret == 0){
        pthread_mutex_lock( &self->modes_mutex );
        self->known_modes[path] = mode;
        pthread_mutex_unlock( &self->modes_mutex );        
    }
    self->BLog.log("%s, %d => path:%s, strPath:%s, ret:%d\n", __FUNCTION__, __LINE__, path, strPath.c_str(), ret);
    FUSE_PLFS_EXIT;
    return -ret;
}

int Plfs::f_chown (const char *path, uid_t uid, gid_t gid )
{
    FUSE_PLFS_ENTER;
    int ret = 0;
    self->BLog.log("%s, %d\n", __FUNCTION__, __LINE__);
    string strPath = expandPath(path);
    ret = self->PIO.Chown(strPath.c_str(), uid, gid);
    self->BLog.log("%s, %d => path:%s, strPath:%s, ret:%d\n", __FUNCTION__, __LINE__, path, strPath.c_str(), ret);
    FUSE_PLFS_EXIT;
    return -ret;
}

int Plfs::f_mkdir (const char *path, mode_t mode )
{
    FUSE_PLFS_ENTER;
    int ret = 0;
    self->BLog.log("%s, %d\n", __FUNCTION__, __LINE__);
    string strPath = expandDirPath(path);
    ret = self->PIO.Mkdir(strPath.c_str(), mode);
    self->BLog.log("%s, %d => path:%s, strPath:%s, ret:%d\n", __FUNCTION__, __LINE__, path, strPath.c_str(), ret);
    FUSE_PLFS_EXIT;
    return -ret;
}

int Plfs::f_rmdir( const char *path )
{
    FUSE_PLFS_ENTER;
    int ret = 0;
    self->BLog.log("%s, %d\n", __FUNCTION__, __LINE__);
    string strPath = expandDirPath(path);
    ret = self->PIO.Rmdir(strPath.c_str());
    self->BLog.log("%s, %d => path:%s, strPath:%s, ret:%d\n", __FUNCTION__, __LINE__, path, strPath.c_str(), ret);
    FUSE_PLFS_EXIT;
    return -ret;
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
    int ret = 0;
    self->BLog.log("%s, %d\n", __FUNCTION__, __LINE__);
    string strPath = expandPath(path);
    ret = self->PIO.Unlink(strPath.c_str());
    self->BLog.log("%s, %d => path:%s, strPath:%s, ret:%d\n", __FUNCTION__, __LINE__, path, strPath.c_str(), ret);
    FUSE_PLFS_EXIT;
    return -ret;
}

// see f_readdir for some documentation here
// returns 0 or -err
int Plfs::f_opendir( const char *path, struct fuse_file_info *fi )
{
    FUSE_PLFS_ENTER;
    int ret = 0;
    self->BLog.log("%s, %d\n", __FUNCTION__, __LINE__);
    string strPath = expandDirPath(path);
    OpenDir *fi_dir = new OpenDir;
    fi_dir->last_offset = 0;
    fi->fh = (uint64_t)NULL;

    ///////////
    DIR *this_dir;
    struct dirent entry;
    struct dirent* entryPtr = NULL;
    set<string>* names = ((set<string>*)&(fi_dir->entries));    
    
    this_dir = opendir(strPath.c_str());
    if(!this_dir) {
        ret = errno;
        self->BLog.log("%s, %d => path:%s, strPath:%s, ret:%d\n", __FUNCTION__, __LINE__, path, strPath.c_str(), ret);
        FUSE_PLFS_EXIT;
        return -ret;
    }

    while ((ret = self->PIO.Readdir_r(strPath.c_str(), this_dir, &entry, &entryPtr)) == 0 && entryPtr != NULL) {
        // if ((!strcmp(entryPtr->d_name,".") ||
        //                   !strcmp(entryPtr->d_name,".."))) {
        //     continue;   // skip the dots
        // }
        string file;
        file = entryPtr->d_name;
        names->insert(file);        
    } 
    ret = self->PIO.Closedir(strPath.c_str(), this_dir);

    //////////
    if (ret == 0) {
        fi->fh = (uint64_t)fi_dir;
    } else {
        delete fi_dir;
    }
    self->BLog.log("%s, %d => path:%s, strPath:%s, ret:%d\n", __FUNCTION__, __LINE__, path, strPath.c_str(), ret);
    FUSE_PLFS_EXIT;
    return -ret;
}

int Plfs::f_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
                    off_t offset, struct fuse_file_info *fi)
{
    FUSE_PLFS_ENTER;
    int ret = 0;
    self->BLog.log("%s, %d\n", __FUNCTION__, __LINE__);
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
    if (!EOD && fi_dir->last_offset > offset) {
        fi_dir->last_offset = offset;
        fi_dir->entries.clear();
        
        this_dir = opendir(strPath.c_str());
        if(!this_dir) {
            ret = errno;
            self->BLog.log("%s, %d => path:%s, strPath:%s, ret:%d\n", __FUNCTION__, __LINE__, path, strPath.c_str(), ret);
            FUSE_PLFS_EXIT;
            return -ret;
        }        

        while ((ret = self->PIO.Readdir_r(strPath.c_str(), this_dir, &entry, &entryPtr)) == 0 && entryPtr != NULL) {
            // if ((!strcmp(entryPtr->d_name,".") ||
            //                   !strcmp(entryPtr->d_name,".."))) {
            //     continue;   // skip the dots
            // }
            string file;
            file = entryPtr->d_name;
            names->insert(file);            
        } 
        ret = self->PIO.Closedir(strPath.c_str(), this_dir);
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

    self->BLog.log("%s, %d => path:%s, strPath:%s, ret:%d\n", __FUNCTION__, __LINE__, path, strPath.c_str(), ret);
    FUSE_PLFS_EXIT;
    return -ret;
}

int Plfs::f_releasedir( const char *path, struct fuse_file_info *fi )
{
    FUSE_PLFS_ENTER;
    int ret = 0;
    string strPath = expandPath(path);
    self->BLog.log("%s, %d\n", __FUNCTION__, __LINE__);
    if (fi->fh) {
        delete (OpenDir *)fi->fh;
    }
    self->BLog.log("%s, %d => path:%s, strPath:%s, ret:%d\n", __FUNCTION__, __LINE__, path, strPath.c_str(), ret);
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
    int ret = 0;
    self->BLog.log("%s, %d\n", __FUNCTION__, __LINE__);
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
    std::map<string, OpenFile*>::iterator fd_it;
    fd_it = self->open_files.find(path);
    struct OpenFile *of = new OpenFile;
    if ( fd_it == self->open_files.end() ) {
        newly_created = true;
        fd = open( strPath.c_str(), fi->flags, mode );
        if(fd < 0){
            ret = errno;
        }
    }else{
        ret = 0;
        fd = fd_it->second->fd;
        of = fd_it->second;
    }

    if ( ret == 0 ) {
        of->fd      = fd;
        of->wr_ref  = 1;
        of->pid     = fuse_get_context()->pid;
        of->uid     = fuse_get_context()->uid;
        of->gid     = fuse_get_context()->gid;
        of->flags   = fi->flags;
        fi->fh = (uint64_t)of;
        if ( newly_created ) {
            self->open_files[path] = of;
        }else{            
            fd_it->second->wr_ref++;
        }
    }
    pthread_mutex_unlock( &self->fd_mutex );
    // we can safely add more writers to an already open file
    // bec FUSE checks f_access before allowing an f_open
    self->BLog.log("%s, %d => path:%s, strPath:%s, fd:%d, ret:%d\n", __FUNCTION__, __LINE__, path, strPath.c_str(), fd, ret);
    FUSE_PLFS_EXIT;
    return -ret;
}

int Plfs::f_release( const char *path, struct fuse_file_info *fi )
{
    FUSE_PLFS_ENTER;
    int ret = 0;
    self->BLog.log("%s, %d\n", __FUNCTION__, __LINE__);
    string strPath = expandPath(path);
    struct OpenFile *openfile = (struct OpenFile*)fi->fh; 
    int fd = 0;                                   
    if ( openfile ) {                                     
        fd = openfile->fd;
        
    }  
    self->BLog.log("%s, %d, fd:%d\n", __FUNCTION__, __LINE__, fd);

    if ( fd ) {
        // SET_IDS(    openfile->uid, openfile->gid );
        // SET_GROUPS( openfile->uid );       
        pthread_mutex_lock( &self->fd_mutex );
        self->BLog.log("%s %d, openfile->wr_ref:%d, openfile->flags:%d, fi->flags:%d\n", __FUNCTION__, __LINE__, openfile->wr_ref, openfile->flags, fi->flags);
        assert( openfile->flags == fi->flags );
        
        --(openfile->wr_ref);
        if(openfile->wr_ref == 0){
            self->PIO.Close(strPath.c_str(), fd);
            self->open_files.erase( path );
        }
        fi->fh = (uint64_t)NULL;

        delete openfile;
        openfile = NULL;
        pthread_mutex_unlock( &self->fd_mutex );
    }
    self->BLog.log("%s, %d => path:%s, strPath:%s, ret:%d\n", __FUNCTION__, __LINE__, path, strPath.c_str(), ret);
    FUSE_PLFS_EXIT;
    return ret;
}

int Plfs::f_write(const char *path, const char *buf, size_t size, off_t offset,
                  struct fuse_file_info *fi)
{
    FUSE_PLFS_ENTER;
    int ret = 0;
    self->BLog.log("%s, %d\n", __FUNCTION__, __LINE__);
    string strPath = expandPath(path);
    struct OpenFile *of = (struct OpenFile*)fi->fh;
    if ( of == NULL ) {
        self->BLog.log("%s, %d => path:%s, strPath:%s\n", __FUNCTION__, __LINE__, path, strPath.c_str());
        FUSE_PLFS_EXIT;
        return -1;
    }

    ssize_t bytes_written;

    pthread_rwlock_wrlock( &self->write_lock );
    ret = self->PIO.Pwrite(strPath.c_str(), of->fd, buf, size, offset, &bytes_written );
    pthread_rwlock_unlock( &self->write_lock );
    ret = (ret == 0) ? bytes_written : -ret;

    self->BLog.log("%s, %d => path:%s, strPath:%s, size:%d, bytes_written:%d, ret:%d\n", __FUNCTION__, __LINE__, path, strPath.c_str(), size, bytes_written, ret);
    FUSE_PLFS_EXIT;
    return ret;
}

int Plfs::f_readlink (const char *path, char *buf, size_t bufsize)
{
    FUSE_PLFS_ENTER;
    int ret = 0;
    self->BLog.log("%s, %d\n", __FUNCTION__, __LINE__);
    string strPath = expandPath(path);
    ssize_t len;
    ret = self->PIO.Readlink( strPath.c_str(), buf, bufsize, &len );
    ret = (len == bufsize) ? len : -ret;
    if ( ret > 0 ) {
        ret = 0;
    }

    self->BLog.log("%s, %d => path:%s, strPath:%s, ret:%d\n", __FUNCTION__, __LINE__, path, strPath.c_str(), ret);
    FUSE_PLFS_EXIT;
    return ret;
}

// not support hard link;
int Plfs::f_link( const char *path, const char *to )
{
    FUSE_PLFS_ENTER;
    int ret = 0;
    self->BLog.log("%s, %d\n", __FUNCTION__, __LINE__);
    string strPath = expandPath(path);
    string strPathTo = expandPath(to);
    ret = self->PIO.Symlink(strPath.c_str(), strPathTo.c_str());
    self->BLog.log("%s, %d => path:%s, strPath:%s, to:%s, strPathTo:%s ,ret:%d\n", __FUNCTION__, __LINE__, path, strPath.c_str(), to, strPathTo.c_str(), ret);    
    FUSE_PLFS_EXIT;
    return -ret;
}

int Plfs::f_symlink( const char *path, const char *to )
{
    FUSE_PLFS_ENTER;
    int ret = 0;
    self->BLog.log("%s, %d\n", __FUNCTION__, __LINE__);
    string strPath = expandPath(path);
    string strPathTo = expandPath(to);
    ret = self->PIO.Symlink(strPath.c_str(), strPathTo.c_str());
    self->BLog.log("%s, %d => path:%s, strPath:%s, to:%s, strPathTo:%s, ret:%d\n", __FUNCTION__, __LINE__, path, strPath.c_str(), to, strPathTo.c_str(), ret);    
    FUSE_PLFS_EXIT;
    return -ret;
}

int Plfs::f_statfs(const char *path, struct statvfs *stbuf)
{
    FUSE_PLFS_ENTER;
    int ret = 0;
    self->BLog.log("%s, %d\n", __FUNCTION__, __LINE__);
    string strPath = expandPath(path);
    ret = self->PIO.Statvfs(strPath.c_str(), stbuf);
    self->BLog.log("%s, %d => path:%s, strPath:%s, ret:%d\n", __FUNCTION__, __LINE__, path, strPath.c_str(), ret);
    FUSE_PLFS_EXIT;
    return -ret;
}

// returns bytes read or -err
int Plfs::f_readn(const char *path, char *buf, size_t size, off_t offset,
                  struct fuse_file_info *fi)
{
    FUSE_PLFS_ENTER;
    int ret = 0;
    self->BLog.log("%s, %d\n", __FUNCTION__, __LINE__);
    self->BLog.log("%s, %d, path:%s, size:%d, offset:%d\n", __FUNCTION__, __LINE__, path, size, offset);
    string strPath = expandPath(path);
    struct OpenFile *of = (struct OpenFile*)fi->fh;
    if ( of == NULL ) {
        self->BLog.log("%s, %d => path:%s, strPath:%s, ret:%d\n", __FUNCTION__, __LINE__, path, strPath.c_str(), ret);
        FUSE_PLFS_EXIT;
        return -1;
    }

    // calls plfs_sync to flush in-memory index.
    if (of) {
        self->PIO.Fsync(strPath.c_str(),  of->fd );
    }
    // syncIfOpen(strPath);
    ssize_t bytes_read;
    ret = self->PIO.Pread( strPath.c_str(), of->fd, buf, size, offset, &bytes_read );
    ret = (ret == 0) ? bytes_read : -ret;

    self->BLog.log("%s, %d => path:%s, strPath:%s, ret:%d\n", __FUNCTION__, __LINE__, path, strPath.c_str(), ret);
    FUSE_PLFS_EXIT;
    return ret;
}

int Plfs::f_flush( const char *path, struct fuse_file_info *fi )
{
    FUSE_PLFS_ENTER;
    int ret = 0;
    self->BLog.log("%s, %d\n", __FUNCTION__, __LINE__);
    string strPath = expandPath(path);
    struct OpenFile *of = (struct OpenFile*)fi->fh;;

    // calls plfs_sync to flush in-memory index.
    if (of) {
        ret = self->PIO.Fsync(strPath.c_str(),  of->fd );
    }

    self->BLog.log("%s, %d => path:%s, strPath:%s, ret:%d\n", __FUNCTION__, __LINE__, path, strPath.c_str(), ret);
    FUSE_PLFS_EXIT;
    return -ret;
}

int Plfs::f_rename( const char *path, const char *to )
{
    FUSE_PLFS_ENTER;
    int ret = 0;
    self->BLog.log("%s, %d\n", __FUNCTION__, __LINE__);
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
        self->BLog.log("%s, %d => path:%s, strPath:%s, ret:%d\n", __FUNCTION__, __LINE__, path, strPath.c_str(), ret);
        FUSE_PLFS_EXIT;
        return -1;
    }
    ret = self->PIO.Rename(strPath.c_str(), strPathTo.c_str());
    if(ret == 0){
        self->open_files[to] = of;
        self->open_files.erase( path );
    }
    pthread_mutex_unlock( &self->fd_mutex );
    
    self->BLog.log("%s, %d => path:%s, strPath:%s, to:%s, strPathTo:%s, ret:%d\n", 
            __FUNCTION__, __LINE__, path, strPath.c_str(), to, strPathTo.c_str(), ret);    
    FUSE_PLFS_EXIT;
    return -ret;

}

// ===============================================================

mode_t Plfs::get_mod(const char* path)
{
    self->BLog.log("%s, %d\n", __FUNCTION__, __LINE__);
    string strPath = expandPath(path);
    mode_t mode = 0;
    int ret;
    pthread_mutex_lock( &self->modes_mutex );
    std::map<string, mode_t>::iterator itr = self->known_modes.find( path );
    if ( itr == self->known_modes.end() ) {
        struct stat stbuf;
        ret = self->PIO.Lstat(strPath.c_str(), &stbuf);
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
    self->BLog.log("%s, %d\n", __FUNCTION__, __LINE__);

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
    self->BLog.log("%s, %d\n", __FUNCTION__, __LINE__);

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
    self->BLog.log("%s, %d\n", __FUNCTION__, __LINE__);
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
    self->BLog.log("%s, %d\n", __FUNCTION__, __LINE__);
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
    self->BLog.log("%s, %d, ret:%d\n", __FUNCTION__, __LINE__, ( val >= 0 ? 0 : -1 ));
    return ( val >= 0 ? 0 : -1 );  /* error# ok */
}

int Plfs::set_groups( uid_t uid )
{
    self->BLog.log("%s, %d, uid:%d\n", __FUNCTION__, __LINE__, uid);
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
    map<uid_t, vector<gid_t> >::iterator itr = self->memberships.find( uid );
    // if not found, find it and cache it
    if ( itr == self->memberships.end() ) {
        pwd = getpwuid( uid );
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
    self->BLog.log("%s, %d, ret:0\n", __FUNCTION__, __LINE__);
    return 0;
}


uid_t Plfs::plfs_getuid()
{
    self->BLog.log("%s, %d\n", __FUNCTION__, __LINE__);
    uid_t ret = getuid();
    self->BLog.log("%s, %d, uid:%d\n", __FUNCTION__, __LINE__, ret);
    return ret;
}

gid_t Plfs::plfs_getgid()
{
    self->BLog.log("%s, %d\n", __FUNCTION__, __LINE__);
    gid_t ret = getgid();
    self->BLog.log("%s, %d, uid:%d\n", __FUNCTION__, __LINE__, ret);
    return ret;
}


int Plfs::plfs_setfsuid(uid_t u)
{
    int uid;
    uid = setfsuid(u);
    self->BLog.log("%s, %d, ret uid:%d\n", __FUNCTION__, __LINE__, uid);
    return uid;
}

int Plfs::plfs_setfsgid(gid_t g)
{
    int gid;
    gid = setfsgid(g);
    self->BLog.log("%s, %d, ret gid:%d\n", __FUNCTION__, __LINE__, gid);
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