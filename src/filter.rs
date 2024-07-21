/// Encapsulates a filter algorithm allowing to search for keys more efficiently.
pub trait FilterPolicy {
    fn name(&self) -> &'static str;
    fn create_filter(&self, keys: &[&[u8]]) -> Vec<u8>;
    fn key_may_match(&self, key: &[u8], filter: &[u8]) -> bool;
}

pub struct BloomPolicy {
    bits_per_key: usize,
    k: usize,
}

impl BloomPolicy {
    pub fn new(bits_per_key: usize) -> Self {
        let mut k = (bits_per_key as f64 * 0.69) as usize;

        k = k.clamp(1, 30);

        BloomPolicy { bits_per_key, k }
    }
}

impl FilterPolicy for BloomPolicy {
    fn name(&self) -> &'static str {
        "leveldb.BuiltinBloomFilter2"
    }

    fn create_filter(&self, keys: &[&[u8]]) -> Vec<u8> {
        let filter_size = keys.len() * self.bits_per_key;
        let mut filter = Vec::new();

        if filter_size < 64 {
            filter.resize(8, 0u8);
        } else {
            filter.resize((filter_size + 7) / 8, 0);
        }

        filter
    }

    fn key_may_match(&self, _key: &[u8], _filter: &[u8]) -> bool {
        true
    }
}
