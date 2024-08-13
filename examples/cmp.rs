fn main() {
    let data1 = "abc".as_bytes();
    let data2 = "ab".as_bytes();

    assert!(data1.cmp(data2) == std::cmp::Ordering::Greater);
}
