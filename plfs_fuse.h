#include "fusexx.h"
#include "bing_log.h"

class T;

#include <set>
#include <string>
#include <map>
#include <vector>
#include <list>
using namespace std;


typedef struct OpenDirStruct {
    set<string> entries;
    off_t last_offset;
} OpenDir;

struct OpenFile {
    int     fd;
    int     wr_ref;
    pid_t   pid;
    uid_t   uid;
    gid_t   gid;
    int     flags;
};

#define BMPOINT "/mnt/bing_store/"

//#include <hash_map>   // shoot, hash_map not found.  more appropriate though..
#define HASH_MAP map

// #define SET_IDS(X,Y)    setfsuid( X );    setfsgid( Y );
// #define SET_GROUPS(X)   set_groups(X);

class Plfs : public fusexx::fuse<Plfs>
{
    public:
        Plfs (); // Constructor

        // Overload the fuse methods
        static int f_access (const char *, int);
        static int f_chmod (const char *path, mode_t mode);
        static int f_chown (const char *path, uid_t uid, gid_t gid );
        static int f_create (const char *, mode_t, struct fuse_file_info *);
        static int f_fgetattr(const char *, struct stat *,
                              struct fuse_file_info *);
        static int f_flush (const char *, struct fuse_file_info *);
        static int f_ftruncate (const char *, off_t, struct fuse_file_info *);
        static int f_fsync(const char *path, int, struct fuse_file_info *fi);
        static int f_getattr (const char *, struct stat *);
        static int f_link (const char *, const char *);
        static int f_mkdir (const char *, mode_t);
        static int f_mknod(const char *path, mode_t mode, dev_t rdev);
        static int f_open (const char *, struct fuse_file_info *);
        static int f_opendir( const char *, struct fuse_file_info * );
        static int f_readlink (const char *, char *, size_t);
        static int f_readn(const char *, char *, size_t,
                           off_t, struct fuse_file_info *);
        static int f_readdir (const char *, void *,
                              fuse_fill_dir_t, off_t, struct fuse_file_info *);
        static int f_release(const char *path, struct fuse_file_info *fi);
        static int f_releasedir( const char *path, struct fuse_file_info *fi );
        static int f_rename (const char *, const char *);
        static int f_rmdir( const char * );
        static int f_statfs(const char *path, struct statvfs *stbuf);
        static int f_symlink(const char *, const char *);
        static int f_truncate( const char *path, off_t offset );
        static int f_unlink( const char * );
        static int f_utime (const char *path, struct utimbuf *ut);
        static int f_write (const char *, const char *, size_t,
                            off_t, struct fuse_file_info *);

        // not overloaded.  something I added to parse command line args
        int init( int *argc, char **argv );

    private:
        static mode_t get_mod(const char* path);
        static string expandPath( const char * );
        static string expandDirPath( const char * );
        static bool checkMask(int mask, int value);
        static int makeNewFile(const char *path, mode_t mode);

        std::map<string, OpenFile*>     open_files;
        // std::map<string, DIR>           dir_map;
        std::map<string, mode_t>        known_modes;  // cache when possible

        pthread_mutex_t                 modes_mutex;
        pthread_mutex_t                 fd_mutex;
        pthread_rwlock_t                write_lock;


    ////////// for debug;
    public:
        bingLog  BLog;
};
