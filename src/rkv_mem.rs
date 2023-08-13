use std::{env, path::Path};

use librustykv::RustyKV;

#[cfg(target_os = "windows")]
const USAGE: &str = "
Usage:
    rkv_mem.exe FILE get KEY
    rkv_mem.exe FILE delete KEY
    rkv_mem.exe FILE insert KEY VALUE
    rkv_mem.exe FILE update KEY VALUE
";

#[cfg(not(target_os = "windows"))]
const USAGE: &str = "
Usage:
    rkv_mem FILE get KEY
    rkv_mem FILE delete KEY
    rkv_mem FILE insert KEY VALUE
    rkv_mem FILE update KEY VALUE
";

fn main() {
    let args = env::args().collect::<Vec<_>>();
    let filename = args.get(1).expect(&USAGE);
    let action = args.get(2).expect(&USAGE).as_ref();
    let key = args.get(3).expect(&USAGE);
    let value = args.get(4);

    let path = Path::new(&filename);
    let mut store =
        RustyKV::open(&path).expect(format!("Unable to open file: {}", filename).as_str());

    store.load().expect("Unable to load data");

    match action {
        "get" => store.get(key),
        "delete" => store.delete(key),
        "insert" => match value {
            Some(v) => store.insert(key, v),
            None => eprintln!("{}", &USAGE),
        },
        "update" => match value {
            Some(v) => store.update(key, v),
            None => eprintln!("{}", &USAGE),
        },
        _ => eprintln!("{}", &USAGE),
    }
}
