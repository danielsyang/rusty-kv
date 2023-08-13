use byteorder::{LittleEndian, ReadBytesExt};
use crc::{Crc, CRC_32_ISO_HDLC};
use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{BufReader, Read, Result, Seek, SeekFrom},
    path::Path,
};

type ByteStr = [u8];
type ByteString = Vec<u8>;

struct KeyValuePair {
    key: ByteString,
    value: ByteString,
}

pub struct RustyKV {
    pub f: File,
    pub index: HashMap<ByteString, u64>,
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
            index: HashMap::new(),
        })
    }

    pub fn load(&mut self) -> Result<Self> {
        let mut buff = BufReader::new(&mut self.f);

        loop {
            // .seek returns the number of bytes from the start of the file (index);
            let position = buff.seek(SeekFrom::Current(0)).unwrap();
            let maybe_kv = RustyKV::process_record(&mut buff);

            let val = match maybe_kv {
                Ok(v) => v,
                Err(err) => panic!(""),
            };
        }
    }

    fn process_record<R: Read>(b: &mut R) -> Result<KeyValuePair> {
        let saved_checksum = b
            .read_u32::<LittleEndian>()
            .expect("Unable to read saved_checksum");

        let key_len = b
            .read_u32::<LittleEndian>()
            .expect("Unable to retrieve key_len");

        let val_len = b
            .read_u32::<LittleEndian>()
            .expect("Unable to retrieve val_len");

        let data_len = key_len + val_len;

        let mut data = ByteString::with_capacity(data_len as usize);
        {
            b.by_ref().take(data_len as u64).read_to_end(&mut data)?;
        }
        debug_assert_eq!(data.len(), data_len as usize);

        let checksum = Crc::<u32>::new(&CRC_32_ISO_HDLC).checksum(&data);
        if checksum != saved_checksum {
            panic!(
                "data corruption encountered ({:08x} != {:08x})",
                checksum, saved_checksum
            );
        }

        let value = data.split_off(key_len as usize);
        let key = data;

        Ok(KeyValuePair { key, value })
    }

    pub fn get(&self, key: &str) -> Result<()> {
        todo!("not yet implemented!")
    }

    pub fn insert(&mut self, key: &ByteStr, value: &ByteStr) -> Result<()> {
        let position = self.insert_but_ignore_index(key, value).unwrap();

        self.index.insert(key.to_vec(), position);
        Ok(())
    }

    pub fn delete(&self, key: &str) -> Result<()> {
        todo!("not yet implemented!")
    }

    pub fn update(&self, key: &str, value: &str) -> Result<()> {
        todo!("not yet implemented!")
    }

    fn insert_but_ignore_index(&self, key: &ByteStr, value: &ByteStr) -> Result<u64> {
        todo!("not yet implemented!")
    }
}
