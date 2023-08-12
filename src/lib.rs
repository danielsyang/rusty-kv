use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::Result,
    path::Path,
};

type ByteString = Vec<u8>;

pub struct RustyKV {
    pub f: File,
    pub hash_map: HashMap<ByteString, u64>,
}

impl RustyKV {
    pub fn open(path: &Path) -> Option<Self> {
        let f = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .append(true)
            .open(path)
            .expect(format!("Something went wrong on path: {:?}", path).as_str());

        Some(RustyKV {
            f,
            hash_map: HashMap::new(),
        })
    }

    pub fn load(&mut self) -> Result<Self> {
        todo!("")
    }

    pub fn get(&self, key: &str) {
        todo!("not yet implemented!")
    }

    pub fn insert(&self, key: &str, value: &str) {
        todo!("not yet implemented!")
    }

    pub fn delete(&self, key: &str) {
        todo!("not yet implemented!")
    }

    pub fn update(&self, key: &str, value: &str) {
        todo!("not yet implemented!")
    }
}
