#include <stdlib.h>
#include <unistd.h>
#include "plfs_fuse.h"

int main (int argc, char **argv) {  
	Plfs plfs;
	plfs.init(&argc, argv);
	plfs.BLog.log("\n\n==================\nbing_fuse started. \n");
	// The first 3 parameters are identical to the fuse_main function.
	// The last parameter gives a pointer to a class instance, which is
	// required for static methods to access instance variables/ methods.
    return plfs.main(argc, argv, NULL, &plfs);
}
