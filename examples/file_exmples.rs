use std::io::{Cursor, Read, Seek};

fn main() {
    let src = "abcdefghijklmnopqrstuvwxyz".as_bytes();
    let mut f = Cursor::new(src);
    let mut buf = [0u8; 8];

    f.read_exact(&mut buf).unwrap();
    println!("{:?}", buf);

    f.read_exact(&mut buf).unwrap();
    println!("{:?}", buf);

    f.seek(std::io::SeekFrom::Start(0)).unwrap();
    f.read_exact(&mut buf).unwrap();
    println!("{:?}", buf);
}
