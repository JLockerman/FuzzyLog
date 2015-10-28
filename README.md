# delos-rust
##To Build
Download [Rust 1.4](https://www.rust-lang.org/downloads.html)  
To run local tests

    cargo test --release

To also run dynamodb tests (This requires credidentials in either an enviroment variable or file as in
[the java sdk](http://docs.aws.amazon.com/AWSSdkDocsJava/latest/DeveloperGuide/credentials.html))

    cargo test --release --features dynamodb_tests

(note that the first compilation will be slow)  

in mac os you need to add

    [credential]
	helper = osxkeychain

to you `.gitconfig`
