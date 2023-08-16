use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use crc::{Crc, CRC_32_ISO_HDLC};
use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{BufReader, BufWriter, Read, Result, Seek, SeekFrom, Write},
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

    pub fn load(&mut self) -> Result<()> {
        let mut buff = BufReader::new(&mut self.f);

        loop {
            // .seek returns the number of bytes from the start of the file (index);
            let position = buff.seek(SeekFrom::Current(0)).unwrap();
            let maybe_kv = RustyKV::process_record(&mut buff);

            let kv = match maybe_kv {
                Ok(v) => v,
                Err(err) => match err.kind() {
                    std::io::ErrorKind::UnexpectedEof => {
                        // <3>
                        break;
                    }
                    _ => return Err(err),
                },
            };

            self.index.insert(kv.key, position);
        }
        return Ok(());
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

    pub fn get(&mut self, key: &ByteStr) -> Result<Option<ByteString>> {
        let position = match self.index.get(key) {
            None => return Ok(None),
            Some(v) => *v,
        };

        let kv = self.get_at(position).unwrap();

        return Ok(Some(kv.value));
    }

    pub fn insert(&mut self, key: &ByteStr, value: &ByteStr) -> Result<()> {
        let position = self.insert_but_ignore_index(key, value).unwrap();

        self.index.insert(key.to_vec(), position);
        Ok(())
    }

    pub fn delete(&mut self, key: &ByteStr) -> Result<()> {
        return self.insert(key, b"");
    }

    pub fn update(&mut self, key: &ByteStr, value: &ByteStr) -> Result<()> {
        return self.insert(key, value);
    }

    fn insert_but_ignore_index(&mut self, key: &ByteStr, value: &ByteStr) -> Result<u64> {
        let mut bw = BufWriter::new(&mut self.f);

        let key_len = key.len();
        let val_len = value.len();
        let mut tmp = ByteString::with_capacity(val_len + key_len);

        for b in key {
            tmp.push(*b);
        }

        for b in value {
            tmp.push(*b)
        }

        let checksum = Crc::<u32>::new(&CRC_32_ISO_HDLC).checksum(&tmp);
        let next_byte = SeekFrom::End(0);
        let current_position = bw.seek(SeekFrom::Current(0))?;
        bw.seek(next_byte)?;
        bw.write_u32::<LittleEndian>(checksum).unwrap();
        bw.write_u32::<LittleEndian>(key_len as u32).unwrap();
        bw.write_u32::<LittleEndian>(val_len as u32).unwrap();
        bw.write_all(&mut tmp).unwrap();

        Ok(current_position)
    }

    fn get_at(&mut self, pos: u64) -> Result<KeyValuePair> {
        let mut bf = BufReader::new(&mut self.f);
        bf.seek(SeekFrom::Start(pos)).unwrap();
        let kv = RustyKV::process_record(&mut bf).unwrap();

        Ok(kv)
    }
}
