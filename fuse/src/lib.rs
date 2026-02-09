#[allow(non_camel_case_types, non_snake_case, non_upper_case_globals)]
mod bindings {
    include!(concat!(env!("OUT_DIR"), "/bindings.rs"));
}

pub use bindings::*;

extern "C" {
    pub fn junkfs_fuse_bridge_version() -> ::std::os::raw::c_int;
    pub fn junkfs_ll_ops_ptr() -> *const fuse_lowlevel_ops;
    pub fn junkfs_ll_ops_size() -> usize;
    pub fn junkfs_fuse_session_new(args: *mut fuse_args, userdata: *mut ::std::os::raw::c_void) -> *mut fuse_session;
    pub fn junkfs_fuse_session_loop_mt(se: *mut fuse_session, config: *mut fuse_loop_config) -> ::std::os::raw::c_int;
}
