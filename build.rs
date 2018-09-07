extern crate cbindgen;


use std::{
    env,
    path::Path,
};

fn main() {
    let crate_dir = env::var("CARGO_MANIFEST_DIR").unwrap();
    let crate_dir: &Path = crate_dir.as_ref();

    match cbindgen::generate(crate_dir) {
        Ok(bindings) => { bindings.write_to_file("fuzzylog.h"); },
        Err(e) => eprintln!("{}", e),
    }

    let ext_toml = "cbindgen_async_ext.toml";
    let config = crate_dir.join(ext_toml);
    match config.exists() {
        true => {
            let mut config = cbindgen::Config::from_file(config.to_str().unwrap()).unwrap();
            config.sys_includes.push("fuzzylog.h".to_owned());
            match cbindgen::generate_with_config(crate_dir, config) {
                Ok(bindings) => { bindings.write_to_file("fuzzylog_async_ext.h"); },
                Err(e) => eprintln!("{}", e),
            }
        },
        false => panic!("{:?} does not exist", config),
    }
}
