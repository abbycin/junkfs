use std::env;
use std::path::PathBuf;

fn main() {
    println!("cargo:rerun-if-changed=include/fuse3_wrapper.h");
    println!("cargo:rerun-if-changed=c/bridge.c");

    let (lib, fuse_use_version) = match pkg_config::Config::new().probe("fuse3") {
        Ok(lib) => (lib, "35"),
        Err(_) => {
            let lib = pkg_config::Config::new()
                .probe("fuse")
                .expect("fuse or fuse3 not found");
            (lib, "26")
        }
    };

    let mut builder = bindgen::Builder::default()
        .header("include/fuse3_wrapper.h")
        .clang_arg(format!("-DFUSE_USE_VERSION={}", fuse_use_version));

    for include in &lib.include_paths {
        builder = builder.clang_arg(format!("-I{}", include.display()));
    }

    let bindings = builder.generate().expect("bindgen failed");
    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    let bindings_path = out_path.join("bindings.rs");
    bindings
        .write_to_file(&bindings_path)
        .expect("write bindings");

    // rust 2024 requires unsafe extern blocks
    let raw = std::fs::read_to_string(&bindings_path).expect("read bindings");
    let patched = raw.replace("extern \"C\" {", "unsafe extern \"C\" {");
    if raw != patched {
        std::fs::write(&bindings_path, patched).expect("patch bindings");
    }

    let mut cc_build = cc::Build::new();
    cc_build
        .file("c/bridge.c")
        .include("include")
        .flag(format!("-DFUSE_USE_VERSION={}", fuse_use_version).as_str());
    for include in &lib.include_paths {
        cc_build.include(include);
    }
    cc_build.compile("junkfs_fuse_bridge");
}
