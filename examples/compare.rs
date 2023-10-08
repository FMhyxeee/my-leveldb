fn main() {
    let a = "abc".as_bytes().to_vec();
    let b = &"abd".as_bytes().to_vec();

    println!("{:?}", a.cmp(b));
}
