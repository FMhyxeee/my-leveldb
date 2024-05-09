use std::path::Path;

fn main() {
    let name = "name";

    let path1 = Path::new(name);
    println!("Path 1{:?}", path1);
    let path2 = path1.join(format!("{:06}.ldb", 2));
    println!("Path 2{:?}", path2);
}
