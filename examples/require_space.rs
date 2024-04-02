use integer_encoding::{FixedInt, VarInt};

pub enum ValueType {
    TypeDeletion = 0,
    TypeValue = 1,
}

fn main() {
    let i = 129_u64;
    let len = i.required_space();

    assert_eq!(len, 2);

    let mut v = vec![0; len];
    v.resize(len, 0);
    assert_eq!(v.len(), 2);

    i.encode_var(&mut v[..]);

    assert_eq!(v, [129, 1]);

    let t = ValueType::TypeValue;

    let u = (t as u64) | 123 << 8;

    v.resize(len + 8, 0);
    assert_eq!(v.len(), 10);
    u.encode_fixed(&mut v[2..]);
    assert_eq!(v, [129, 1, 1, 123, 0, 0, 0, 0, 0, 0]);

    let mut index = 0;

    let (val, i): (u64, usize) = VarInt::decode_var(&v[..]).unwrap();
    assert_eq!(val, 129u64);
    assert_eq!(i, 2);

    index += i;

    let val2: u64 = FixedInt::decode_fixed(&v[index..]).unwrap();

    println!("{:?}", 123 << 8);
    println!("{:?}", ValueType::TypeValue as u64);
    println!("{:?}", (123 << 8) + ValueType::TypeValue as u64);

    println!("{:?}", val2);
}
