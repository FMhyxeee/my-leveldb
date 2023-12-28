use std::{
    cmp::Ordering,
    mem::{replace, size_of},
    rc::Rc,
    sync::Arc,
};

use rand::{rngs::StdRng, RngCore, SeedableRng};

use crate::{cmp::MemtableKeyCmp, options::Options, types::LdbIterator};

const MAX_HEIGHT: usize = 12;
const BRANCHING_FACTOR: u32 = 4;

/// A Node in a skipmap contains links to the next node and others are further away (skips);
/// `skips[0]` is the immediate element after, that is, the element contained in `next`.
struct Node {
    skips: Vec<Option<*mut Node>>,
    //skip[0] points to the element in next; next provides proper ownership
    next: Option<Box<Node>>,
    key: Vec<u8>,
    value: Vec<u8>,
}

/// Implements the backing store for a `Memtable`. The important methods are `insert()` and `contains()`;
/// in order to get full key and value for an entry, use a `SkipMapIter` instance, `seek()` to the key to
/// look up (this is as fast as any lookup in a skip map), and then call `current()`
pub struct InnerSkipMap {
    head: Box<Node>,
    rand: StdRng,
    len: usize,
    // approximation of memory used.
    approx_mem: usize,
    opt: Options,
}

pub struct SkipMap {
    map: Rc<InnerSkipMap>,
}

impl SkipMap {
    /// Returns a SkipMap that wraps the comparator from opt inside a MemtableKeyCmp
    pub fn new_memtable_map(mut opt: Options) -> SkipMap {
        opt.cmp = Arc::new(Box::new(MemtableKeyCmp(opt.cmp.clone())));
        SkipMap::new(opt)
    }

    /// Returns a SkipMap that uses the comparator from opt
    pub fn new(opt: Options) -> SkipMap {
        let mut s = Vec::new();
        s.resize(MAX_HEIGHT, None);

        SkipMap {
            map: Rc::new(InnerSkipMap {
                head: Box::new(Node {
                    skips: s,
                    next: None,
                    key: Vec::new(),
                    value: Vec::new(),
                }),
                rand: StdRng::from_entropy(),
                len: 0,
                approx_mem: size_of::<Self>() + MAX_HEIGHT * size_of::<Option<*mut Node>>(),
                opt,
            }),
        }
    }

    pub fn len(&self) -> usize {
        self.map.len
    }
    pub fn approx_memory(&self) -> usize {
        self.map.approx_mem
    }
    pub fn contains(&mut self, key: &[u8]) -> bool {
        self.map.contains(key)
    }
    pub fn insert(&mut self, key: Vec<u8>, val: Vec<u8>) {
        Rc::get_mut(&mut self.map).unwrap().insert(key, val);
    }

    pub fn iter(&self) -> SkipMapIter {
        SkipMapIter {
            map: self.map.clone(),
            current: self.map.head.as_ref() as *const Node,
        }
    }
}

impl InnerSkipMap {
    fn random_height(&mut self) -> usize {
        let mut height = 1;
        while height < MAX_HEIGHT && self.rand.next_u32() % BRANCHING_FACTOR == 0 {
            height += 1;
        }
        height
    }

    fn contains(&self, key: &[u8]) -> bool {
        if let Some(n) = self.get_greater_or_equal(key) {
            n.key.starts_with(key)
        } else {
            false
        }
    }

    /// Returns the node with key or the next greater one
    /// Returns None if the given key lies past the greatest key in th table.
    fn get_greater_or_equal<'a>(&'a self, key: &[u8]) -> Option<&'a Node> {
        // Start at the highest skip link of the head node, and work down from there
        let mut current: *const Node = self.head.as_ref() as *const Node;
        let mut level = self.head.skips.len() - 1;
        loop {
            unsafe {
                if let Some(next) = (*current).skips[level] {
                    let ord = self.opt.cmp.cmp((*next).key.as_slice(), key);

                    match ord {
                        Ordering::Less => {
                            current = next;
                            continue;
                        }
                        Ordering::Equal => return Some(&(*next)),
                        Ordering::Greater => {
                            if level == 0 {
                                return Some(&(*next));
                            }
                        }
                    }
                }
            }
            // At the bottom of Node and no more next node , we should break;
            if level == 0 {
                break;
            }
            level -= 1;
        }
        unsafe {
            if current.is_null() || self.opt.cmp.cmp(&(*current).key, key) == Ordering::Less {
                None
            } else {
                Some(&(*current))
            }
        }
    }

    /// Finds the node immediately before the node with key
    /// Returns None if no smaller key was found
    fn get_next_smaller<'a>(&'a self, key: &[u8]) -> Option<&'a Node> {
        // Start at the highest skip link of the head node, and work down from there
        let mut current: *const Node = self.head.as_ref() as *const Node;
        let mut level = self.head.skips.len() - 1;

        loop {
            unsafe {
                if let Some(next) = (*current).skips[level] {
                    let ord = self.opt.cmp.cmp((*next).key.as_slice(), key);

                    if ord == Ordering::Less {
                        current = next;
                        continue;
                    }
                }
            }

            if level == 0 {
                break;
            }
            level -= 1;
        }

        unsafe {
            if current.is_null()
                || (*current).key.is_empty()
                || self.opt.cmp.cmp(&(*current).key, key) != Ordering::Less
            {
                None
            } else {
                Some(&(*current))
            }
        }
    }

    fn insert(&mut self, key: Vec<u8>, val: Vec<u8>) {
        assert!(!key.is_empty());

        // Keeping track of skip entries that will need to be updated;
        let new_height = self.random_height();
        let mut prevs: Vec<Option<*mut Node>> = Vec::with_capacity(new_height);

        let mut level = MAX_HEIGHT - 1;
        let mut current: *mut Node = self.head.as_mut() as *mut Node;
        // Initialize all prevs entries with *head
        prevs.resize(new_height, Some(current));

        // Find the node after which we want to insert the new node; this is the node with the key
        // immediately smaller than the key to be inserted.
        loop {
            unsafe {
                if let Some(next) = (*current).skips[level] {
                    // If the wanted position is after the current node
                    let ord = self.opt.cmp.cmp(&(*next).key, &key);

                    assert!(ord != Ordering::Equal, "No duplicates allowed");

                    if ord == Ordering::Less {
                        current = next;
                        continue;
                    }
                }
            }

            if level < new_height {
                prevs[level] = Some(current);
            }

            if level == 0 {
                break;
            } else {
                level -= 1;
            }
        }

        // Construct new node
        let new_skips = vec![None; new_height];

        let mut new = Box::new(Node {
            skips: new_skips,
            next: None,
            key,
            value: val,
        });

        // newp is a raw Node point
        let newp = new.as_mut() as *mut Node;

        for (idx, item) in prevs.into_iter().enumerate().take(new_height) {
            if let Some(prev) = item {
                unsafe {
                    new.skips[idx] = (*prev).skips[idx];
                    // make prev node's every skips point to newp
                    (*prev).skips[idx] = Some(newp);
                }
            }
        }

        let added_mem = size_of::<Node>()
            + size_of::<Option<*mut Node>>() * new.skips.len()
            + new.key.len()
            + new.value.len();

        self.approx_mem += added_mem;
        self.len += 1;

        // Insert new node by first replacing the previous element's next field with None and
        // assigning its value to new next...
        new.next = unsafe { (*current).next.take() };

        // ...and then setting the previous element's next field to the new node
        unsafe { replace(&mut (*current).next, Some(new)) };
    }

    // Runs through the skipmap and prints everything including addresses
    fn dbg_print(&self) {
        let mut current: *const Node = self.head.as_ref() as *const Node;
        loop {
            unsafe {
                println!(
                    "{:?} {:?}/{:?} - {:?}",
                    current,
                    (*current).key,
                    (*current).value,
                    (*current).skips
                );

                if let Some(next) = (*current).skips[0] {
                    current = next;
                } else {
                    break;
                }
            }
        }
    }
}

pub struct SkipMapIter {
    map: Rc<InnerSkipMap>,
    current: *const Node,
}

impl LdbIterator for SkipMapIter {
    fn advance(&mut self) -> bool {
        // we first go to the next element, then return that -- in order to skip the head node
        let r = unsafe {
            (*self.current)
                .next
                .as_ref()
                .map(|next| {
                    self.current = next.as_ref() as *const Node;
                    true
                })
                .unwrap_or(false)
        };
        if !r {
            self.reset();
        }
        r
    }

    fn reset(&mut self) {
        self.current = self.map.head.as_ref();
    }

    fn seek(&mut self, key: &[u8]) {
        if let Some(node) = self.map.get_greater_or_equal(key) {
            self.current = node as *const Node;
            return;
        }
        self.reset();
    }

    fn valid(&self) -> bool {
        unsafe { !(*self.current).key.is_empty() }
    }

    fn current(&self, key: &mut Vec<u8>, val: &mut Vec<u8>) -> bool {
        if self.valid() {
            key.clear();
            val.clear();
            unsafe {
                key.extend_from_slice(&(*self.current).key);
                val.extend_from_slice(&(*self.current).value);
            }
            true
        } else {
            false
        }
    }
    fn prev(&mut self) -> bool {
        // Going after the original implementation here; we just seek to the node before current().
        if self.valid() {
            if let Some(prev) = self.map.get_next_smaller(unsafe { &(*self.current).key }) {
                self.current = prev as *const Node;
                if !prev.key.is_empty() {
                    return true;
                }
            }
        }
        self.reset();
        false
    }
}

#[cfg(test)]
pub mod tests {
    use crate::{
        test_util::{test_iterator_properties, LdbIteratorIter},
        types::*,
    };

    use super::*;

    pub fn make_skipmap() -> SkipMap {
        let mut skm = SkipMap::new(Options::default());
        let keys = vec![
            "aba", "abb", "abc", "abd", "abe", "abf", "abg", "abh", "abi", "abj", "abk", "abl",
            "abm", "abn", "abo", "abp", "abq", "abr", "abs", "abt", "abu", "abv", "abw", "abx",
            "aby", "abz",
        ];
        for key in keys {
            skm.insert(key.as_bytes().to_vec(), "def".as_bytes().to_vec());
        }
        skm
    }

    #[test]
    fn test_insert() {
        let skm = make_skipmap();
        assert_eq!(skm.len(), 26);
        skm.map.dbg_print();
    }

    #[test]
    #[should_panic]
    fn test_no_dupes() {
        let mut skm = make_skipmap();
        // this should panic
        skm.insert("abc".as_bytes().to_vec(), "def".as_bytes().to_vec());
        skm.insert("abf".as_bytes().to_vec(), "def".as_bytes().to_vec());
    }

    #[test]
    fn test_contains() {
        let mut skm = make_skipmap();
        assert!(skm.contains("aby".as_bytes()));
        assert!(skm.contains("abc".as_bytes()));
        assert!(skm.contains("abz".as_bytes()));
        assert!(skm.contains("ab".as_bytes()));
        assert!(!skm.contains("123".as_bytes()));
        assert!(!skm.contains("aaa".as_bytes()));
        assert!(!skm.contains("456".as_bytes()));
    }

    #[test]
    fn test_find() {
        let skm = make_skipmap();
        assert_eq!(
            skm.map.get_greater_or_equal("abf".as_bytes()).unwrap().key,
            "abf".as_bytes()
        );
        assert!(skm.map.get_greater_or_equal("ab{".as_bytes()).is_none(),);
        assert_eq!(
            skm.map.get_greater_or_equal("aaa".as_bytes()).unwrap().key,
            "aba".as_bytes()
        );
        assert_eq!(
            skm.map
                .get_greater_or_equal("ab".as_bytes())
                .unwrap()
                .key
                .as_slice(),
            "aba".as_bytes()
        );
        assert_eq!(
            skm.map
                .get_greater_or_equal("abc".as_bytes())
                .unwrap()
                .key
                .as_slice(),
            "abc".as_bytes()
        );
        assert_eq!(
            skm.map
                .get_next_smaller("abd".as_bytes())
                .unwrap()
                .key
                .as_slice(),
            "abc".as_bytes()
        );
        assert_eq!(
            skm.map
                .get_next_smaller("ab{".as_bytes())
                .unwrap()
                .key
                .as_slice(),
            "abz".as_bytes()
        );
    }

    #[test]
    fn test_skipmap_iterator_0() {
        let skm = SkipMap::new(Options::default());
        let mut i = 0;

        for (_, _) in LdbIteratorIter::wrap(&mut skm.iter()) {
            i += 1;
        }

        assert_eq!(i, 0);
        assert!(!skm.iter().valid());
    }

    #[test]
    fn test_skipmap_iterator_init() {
        let skm = make_skipmap();
        let mut iter = skm.iter();

        assert!(!iter.valid());
        iter.next();
        assert!(iter.valid());
    }

    #[test]
    fn test_skipmap_iterator() {
        let skm = make_skipmap();
        let mut i = 0;

        for (k, v) in LdbIteratorIter::wrap(&mut skm.iter()) {
            assert!(!k.is_empty());
            assert!(!v.is_empty());
            i += 1;
        }

        assert_eq!(i, 26);
    }

    #[test]
    fn test_skipmap_iterator_seek_valid() {
        let skm = make_skipmap();
        let mut iter = skm.iter();

        iter.next();
        assert!(iter.valid());
        assert_eq!(current_key_val(&iter).unwrap().0, "aba".as_bytes());
        iter.seek("abz".as_bytes());
        // go back to beginning
        iter.seek("aba".as_bytes());
        assert_eq!(
            current_key_val(&iter).unwrap(),
            ("aba".as_bytes().to_vec(), "def".as_bytes().to_vec())
        );

        iter.seek("".as_bytes());
        assert!(iter.valid());
        iter.prev();
        assert!(!iter.valid());

        while iter.advance() {}

        assert!(!iter.valid());
        assert!(!iter.prev());
        assert_eq!(current_key_val(&iter), None);
    }

    #[test]
    fn test_skipmap_behavior() {
        let mut skm = SkipMap::new(Options::default());
        let keys = vec!["aba", "abb", "abc", "abd"];
        for k in keys {
            skm.insert(k.as_bytes().to_vec(), "def".as_bytes().to_vec());
        }
        test_iterator_properties(skm.iter());
    }

    #[test]
    fn test_skipmap_iterator_prev() {
        let skm = make_skipmap();
        let mut iter = skm.iter();

        iter.next();
        assert!(iter.valid());
        iter.prev();
        assert!(!iter.valid());
        iter.seek("abc".as_bytes());
        iter.prev();
        assert_eq!(
            current_key_val(&iter).unwrap(),
            ("abb".as_bytes().to_vec(), "def".as_bytes().to_vec())
        );
    }
}
