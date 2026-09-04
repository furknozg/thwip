use proxy_common::Config;
use std::{fs, path::PathBuf};

#[test]
fn every_example_configuration_is_valid() {
    let directory = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../examples/config");
    let mut examples: Vec<_> = fs::read_dir(&directory)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| {
            path.extension()
                .is_some_and(|extension| extension == "toml")
        })
        .collect();
    examples.sort();
    assert!(!examples.is_empty());

    for path in examples {
        let source = fs::read_to_string(&path).unwrap();
        Config::from_toml(&source)
            .unwrap_or_else(|error| panic!("{} is invalid: {error}", path.display()));
    }
}
