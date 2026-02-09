#include "fuse3_wrapper.h"

extern void junkfs_ll_init(void *userdata, struct fuse_conn_info *conn);
extern void junkfs_ll_destroy(void *userdata);
extern void junkfs_ll_lookup(fuse_req_t req, fuse_ino_t parent, const char *name);
extern void junkfs_ll_getattr(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi);
extern void junkfs_ll_setattr(fuse_req_t req, fuse_ino_t ino, struct stat *attr, int to_set, struct fuse_file_info *fi);
extern void junkfs_ll_mknod(fuse_req_t req, fuse_ino_t parent, const char *name, mode_t mode, dev_t rdev);
extern void junkfs_ll_mkdir(fuse_req_t req, fuse_ino_t parent, const char *name, mode_t mode);
extern void junkfs_ll_unlink(fuse_req_t req, fuse_ino_t parent, const char *name);
extern void junkfs_ll_rmdir(fuse_req_t req, fuse_ino_t parent, const char *name);
extern void junkfs_ll_symlink(fuse_req_t req, const char *link, fuse_ino_t parent, const char *name);
extern void junkfs_ll_readlink(fuse_req_t req, fuse_ino_t ino);
extern void junkfs_ll_rename(fuse_req_t req, fuse_ino_t parent, const char *name, fuse_ino_t newparent, const char *newname, unsigned int flags);
extern void junkfs_ll_link(fuse_req_t req, fuse_ino_t ino, fuse_ino_t newparent, const char *newname);
extern void junkfs_ll_open(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi);
extern void junkfs_ll_read(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off, struct fuse_file_info *fi);
extern void junkfs_ll_write(fuse_req_t req, fuse_ino_t ino, const char *buf, size_t size, off_t off, struct fuse_file_info *fi);
extern void junkfs_ll_flush(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi);
extern void junkfs_ll_release(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi);
extern void junkfs_ll_opendir(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi);
extern void junkfs_ll_readdir(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off, struct fuse_file_info *fi);
extern void junkfs_ll_releasedir(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi);
extern void junkfs_ll_fsync(fuse_req_t req, fuse_ino_t ino, int datasync, struct fuse_file_info *fi);
extern void junkfs_ll_fsyncdir(fuse_req_t req, fuse_ino_t ino, int datasync, struct fuse_file_info *fi);
extern void junkfs_ll_create(fuse_req_t req, fuse_ino_t parent, const char *name, mode_t mode, struct fuse_file_info *fi);

static struct fuse_lowlevel_ops junkfs_ll_ops = {
    .init = junkfs_ll_init,
    .destroy = junkfs_ll_destroy,
    .lookup = junkfs_ll_lookup,
    .getattr = junkfs_ll_getattr,
    .setattr = junkfs_ll_setattr,
    .mknod = junkfs_ll_mknod,
    .mkdir = junkfs_ll_mkdir,
    .unlink = junkfs_ll_unlink,
    .rmdir = junkfs_ll_rmdir,
    .symlink = junkfs_ll_symlink,
    .readlink = junkfs_ll_readlink,
    .rename = junkfs_ll_rename,
    .link = junkfs_ll_link,
    .open = junkfs_ll_open,
    .read = junkfs_ll_read,
    .write = junkfs_ll_write,
    .flush = junkfs_ll_flush,
    .release = junkfs_ll_release,
    .opendir = junkfs_ll_opendir,
    .readdir = junkfs_ll_readdir,
    .releasedir = junkfs_ll_releasedir,
    .fsync = junkfs_ll_fsync,
    .fsyncdir = junkfs_ll_fsyncdir,
    .create = junkfs_ll_create,
};

const struct fuse_lowlevel_ops *junkfs_ll_ops_ptr(void) {
    return &junkfs_ll_ops;
}

size_t junkfs_ll_ops_size(void) {
    return sizeof(junkfs_ll_ops);
}

struct fuse_session *junkfs_fuse_session_new(struct fuse_args *args, void *userdata) {
    return fuse_session_new(args, &junkfs_ll_ops, sizeof(junkfs_ll_ops), userdata);
}

int junkfs_fuse_session_loop_mt(struct fuse_session *se, struct fuse_loop_config *config) {
    return fuse_session_loop_mt(se, config);
}

int junkfs_fuse_bridge_version(void) {
    return FUSE_USE_VERSION;
}
