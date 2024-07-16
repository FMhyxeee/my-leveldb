fn main() {
    let data = [1, 97, 98, 99, 100, 101, 102, 103, 104, 105];
    let data1 = [2, 49, 50, 51, 52, 53, 54, 55, 56, 57, 48];

    let crc_alg1 = crc::Crc::<u32>::new(&crc::CRC_32_CKSUM);

    let _a = crc_alg1.checksum(&data);
    let b = crc_alg1.checksum(&data1);

    let crc_alg2 = crc::Crc::<u32>::new(&crc::CRC_32_CKSUM);
    let b1 = crc_alg2.checksum(&data1);

    println!("b: {}", b);

    assert_eq!(b, b1);
}
