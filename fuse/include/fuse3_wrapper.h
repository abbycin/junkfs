#ifndef FUSE_USE_VERSION
#define FUSE_USE_VERSION 35
#endif

#if __has_include(<fuse3/fuse_lowlevel.h>)
#include <fuse3/fuse_lowlevel.h>
#elif __has_include(<fuse/fuse_lowlevel.h>)
#include <fuse/fuse_lowlevel.h>
#else
#error "fuse headers not found"
#endif
