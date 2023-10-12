fn main() {
    let mut v = (0..10).collect::<Vec<_>>();
    v.resize(20, 100);
    println!("{v:?}");
    let i = 1_u64 | (1 << 8);
    println!("{i:?}");
}
