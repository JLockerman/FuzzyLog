#!/bin/bash

# This a simple bash script to run `cargo clean` recursively.
#
# Usage: `bash cargo-clean.sh target_dir`

clean_recursive() {
    # First, check whether current directiry is the root of a cargo project.
    if [ -f "Cargo.toml" ]; then
        echo "Cleaning \"$(pwd)\""
        cargo clean
    fi
    # Then, visit the sub directory.
    for path in `ls`; do
        if [ -d $path ]; then
            cd $path
            clean_recursive
            cd ..
        fi
    done
}

main() {
    if [ x$1 != x ]; then
        if [ -d $1 ]; then
            cd $1
        else
            echo "Error: $1 is not a directory, done."
        fi
    else
        cd .
    fi

    echo "Scanning \"$(pwd)\""
    clean_recursive
    echo "Done."
}

main $*

