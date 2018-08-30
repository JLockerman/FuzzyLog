extern crate cbindgen;


use std::env;

fn main() {
    let crate_dir = env::var("CARGO_MANIFEST_DIR").unwrap();

    match cbindgen::generate(crate_dir) {
        Ok(bindings) => { bindings.write_to_file("fuzzylog.h"); },
        Err(e) => eprintln!("{}", e),
    }
}
