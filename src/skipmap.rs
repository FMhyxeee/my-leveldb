use std::{
    cmp::Ordering,
    mem::{replace, size_of, transmute_copy},
};

use rand::{
    rngs::{StdRng, ThreadRng},
    RngCore, SeedableRng,
};

use crate::types::{Comparator, LdbIterator, StandardComparator};

const MAX_HEIGHT: usize = 12;
const BRANCHING_FACTOR: u32 = 4;

/// A node is in skipmap contains links to the next node and others that are further away (skips);
/// `Skips[0]` is the immedicate element after, that is, the element contains in `next`.
struct Node {
    skips: Vec<Option<*mut Node>>,
    next: Option<Box<Node>>,
    key: Vec<u8>,
    value: Vec<u8>,
}

/// Implements the backing store for a `MemTable`. The impoertant methods are `insert()` and
/// `contains()`; in order to get full key and value for an entry, use a `SkipMapIter` instance,
/// `seek()` to the key to look up (this is as fast as any lookup in a skip map), and then call
/// `current()`.
pub struct SkipMap<C: Comparator> {
    head: Box<Node>,
    rand: StdRng,
    cmp: C,
    len: usize,
    // approximation of memory used.
    approx_mem: usize,
}

impl SkipMap<StandardComparator> {
    pub fn new() -> SkipMap<StandardComparator> {
        SkipMap::new_with_cmp(StandardComparator)
    }
}

impl<C: Comparator> SkipMap<C> {
    pub fn new_with_cmp(cmp: C) -> SkipMap<C> {
        let s = vec![None; MAX_HEIGHT];

        SkipMap {
            head: Box::new(Node {
                skips: s,
                next: None,
                key: Vec::new(),
                value: Vec::new(),
            }),
            rand: StdRng::from_rng(ThreadRng::default()).unwrap(),
            cmp,
            len: 0,
            approx_mem: size_of::<Self>() + MAX_HEIGHT * size_of::<Option<*mut Node>>(),
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn approx_mem(&self) -> usize {
        self.approx_mem
    }

    fn random_height(&mut self) -> usize {
        let mut height = 1;
        while height < MAX_HEIGHT && self.rand.next_u32() % BRANCHING_FACTOR == 0 {
            height += 1;
        }
        height
    }

    pub fn contains(&self, key: &[u8]) -> bool {
        if let Some(n) = self.get_greater_or_equal(key) {
            n.key.starts_with(key)
        } else {
            false
        }
    }

    /// Return the node with key or the next greater one.
    /// Returns None if the given key lies past the greatest key in the table.
    fn get_greater_or_equal(&self, key: &[u8]) -> Option<&Node> {
        // Start at the highest skip link of the head node, and work down from there
        let mut current: *const Node = unsafe { transmute_copy(&self.head.as_ref()) };
        let mut level = self.head.skips.len() - 1;

        loop {
            unsafe {
                if let Some(next) = (*current).skips[level] {
                    match C::cmp(&(*next).key, key) {
                        std::cmp::Ordering::Less => {
                            current = next;
                            continue;
                        }
                        std::cmp::Ordering::Equal => {
                            return Some(&*next);
                        }
                        std::cmp::Ordering::Greater => {
                            if level == 0 {
                                return Some(&(*next));
                            }
                        }
                    }
                }
            }

            if level == 0 {
                break;
            }
            level -= 1;
        }
        unsafe {
            if current.is_null() || C::cmp(&(*current).key, key) == Ordering::Less {
                None
            } else {
                Some(&*current)
            }
        }
    }

    /// Finds the node immediately before the node with key.
    /// Returns None if no smaller key was found.
    fn get_next_smaller(&self, key: &[u8]) -> Option<&Node> {
        // Start at the highest skip link of the head node, and work down from there
        let mut current: *const Node = unsafe { transmute_copy(&self.head.as_ref()) };
        let mut level = self.head.skips.len() - 1;

        loop {
            unsafe {
                if let Some(next) = (*current).skips[level] {
                    if C::cmp(&(*next).key, key) == Ordering::Less {
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
                || C::cmp(&(*current).key, key) != Ordering::Less
            {
                None
            } else {
                Some(&*current)
            }
        }
    }

    pub fn insert(&mut self, key: &[u8], value: &[u8]) {
        assert!(!key.is_empty());

        // Keeping track of skip entries what will need to be update.

        let new_height = self.random_height();
        let mut current: *mut Node = unsafe { transmute_copy(&self.head.as_mut()) };

        let mut level = MAX_HEIGHT - 1;
        let mut prevs: Vec<Option<*mut Node>> = vec![Some(current); new_height];

        // Find the node after which we want to insert the new node; this is the node with the key
        // immediately smaller than the key to be inserted.
        loop {
            unsafe {
                if let Some(next) = (*current).skips[level] {
                    // If the wanted position is after the current node
                    let ord = C::cmp(&(*next).key, key);
                    assert!(
                        ord != std::cmp::Ordering::Equal,
                        "No duplicate keys allowed"
                    );
                    if ord == std::cmp::Ordering::Less {
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

        // Construct the new node
        let mut new = Box::new(Node {
            skips: vec![None; new_height],
            next: None,
            key: key.to_vec(),
            value: value.to_vec(),
        });

        let newp = unsafe { transmute_copy(&new.as_mut()) };

        for (idx, prev) in prevs.iter().enumerate().take(new_height) {
            if let &Some(prev) = prev {
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

        // Insert new node by first replacing the previous element's next field to the new node
        // assigning its value to new.next...
        new.next = unsafe { (*current).next.take() };

        let _ = unsafe { replace(&mut (*current).next, Some(new)) };
    }

    pub fn iter(&self) -> SkipMapIter<C> {
        SkipMapIter {
            map: self,
            current: &*self.head,
        }
    }

    /// Runs through the skipmap and prints everything including addresses
    fn dbg_print(&self) {
        let mut current: *const Node = &*self.head;
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

pub struct SkipMapIter<'a, C: Comparator> {
    map: &'a SkipMap<C>,
    current: *const Node,
}

impl<'a, C: Comparator + 'a> Iterator for SkipMapIter<'a, C> {
    type Item = (&'a [u8], &'a [u8]);

    fn next(&mut self) -> Option<Self::Item> {
        // we first go to the next element, then return that -- in order to skip the head node
        unsafe {
            (*self.current).next.as_ref().map(|next| {
                self.current = transmute_copy(&next.as_ref());
                (&next.key[..], &next.value[..])
            })
        }
    }
}

impl<'a, C: Comparator> LdbIterator for SkipMapIter<'a, C> {
    fn seek(&mut self, key: &[u8]) {
        if let Some(node) = self.map.get_greater_or_equal(key) {
            self.current = node;
        } else {
            self.reset();
        }
    }

    fn reset(&mut self) {
        self.current = &*self.map.head;
    }

    fn valid(&self) -> bool {
        unsafe { !(*self.current).key.is_empty() }
    }

    fn current(&self) -> Option<Self::Item> {
        if self.valid() {
            Some((unsafe { &(*self.current).key }, unsafe {
                &(*self.current).value
            }))
        } else {
            None
        }
    }

    fn prev(&mut self) -> Option<Self::Item> {
        // Going after the original Implementation here; we just seek to the node before current().
        if let Some(current) = self.current() {
            if let Some(prev) = self.map.get_next_smaller(current.0) {
                self.current = prev;

                if !prev.key.is_empty() {
                    return Some(unsafe { (&(*self.current).key, &(*self.current).value) });
                }
            }
        }

        self.reset();
        None
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;

    pub fn make_skipmap() -> SkipMap<StandardComparator> {
        let mut skm = SkipMap::new();
        let keys = vec![
            b"aba", b"abb", b"abc", b"abd", b"abe", b"abf", b"abg", b"abh", b"abi", b"abj", b"abk",
            b"abl", b"abm", b"abn", b"abo", b"abp", b"abq", b"abr", b"abs", b"abt", b"abu", b"abv",
            b"abw", b"abx", b"aby", b"abz",
        ];

        for k in keys {
            skm.insert(k, b"def");
        }
        skm
    }

    #[test]
    fn test_insert() {
        let sm = make_skipmap();
        assert_eq!(sm.len(), 26);

        sm.dbg_print();
    }

    #[test]
    #[should_panic]
    fn test_no_dupes() {
        let mut skm = make_skipmap();
        // This should panic
        skm.insert(b"abc", b"def");
    }

    #[test]
    fn test_contains() {
        let sm = make_skipmap();
        assert!(sm.contains(b"abc"));
        assert!(!sm.contains(b"xyz"));
    }

    #[test]
    fn test_find() {
        let skm = make_skipmap();
        assert_eq!(skm.get_greater_or_equal(b"abf").unwrap().key, b"abf");
        assert_eq!(skm.get_greater_or_equal(b"aaa").unwrap().key, b"aba");
        assert_eq!(skm.get_greater_or_equal(b"ab").unwrap().key, b"aba");
        assert_eq!(skm.get_greater_or_equal(b"abc").unwrap().key, b"abc");
        assert!(skm.get_greater_or_equal(b"ab{").is_none());
        assert_eq!(skm.get_next_smaller(b"abd").unwrap().key, b"abc");
        assert_eq!(skm.get_next_smaller(b"ab{").unwrap().key, b"abz");
        assert!(skm.get_next_smaller(b"aaa").is_none());
    }

    #[test]
    fn test_iterator_0() {
        let skm = SkipMap::new();
        let mut i = 0;
        for _ in skm.iter() {
            i += 1;
        }
        assert_eq!(i, 0);
        assert!(!skm.iter().valid());
    }

    #[test]
    fn test_iterator_init() {
        let skm = make_skipmap();
        let mut iter = skm.iter();

        assert!(!iter.valid());
        iter.next();
        assert!(iter.valid());
        iter.reset();
        assert!(!iter.valid());

        iter.next();
        assert!(iter.valid());
        iter.prev();
        assert!(!iter.valid());
    }

    #[test]
    fn test_iterator() {
        let skm = make_skipmap();
        let mut i = 0;
        for (k, v) in skm.iter() {
            assert!(!k.is_empty());
            assert!(!v.is_empty());
            i += 1;
        }
        assert_eq!(i, 26);
    }

    #[test]
    fn test_iterator_seek_valid() {
        let skm = make_skipmap();
        let mut iter = skm.iter();

        iter.next();
        assert!(iter.valid());
        iter.seek(b"abz");
        assert_eq!(
            iter.current().unwrap(),
            ("abz".as_bytes(), "def".as_bytes())
        );

        // go back to beginning
        iter.seek(b"aba");
        assert_eq!(
            iter.current().unwrap(),
            ("aba".as_bytes(), "def".as_bytes())
        );

        iter.seek(b"");
        assert!(iter.valid());
        iter.prev();
        assert!(!iter.valid());

        loop {
            if iter.next().is_some() {
                continue;
            } else {
                break;
            }
        }
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn test_approx_mem() {
        let skm = SkipMap::new();
        let mem = skm.approx_mem();
        let initial_mem =
            size_of::<SkipMap<StandardComparator>>() + MAX_HEIGHT * size_of::<Option<*mut Node>>();
        assert_eq!(mem, initial_mem);
    }

    #[test]
    fn test_iterator_prev() {
        let skm = make_skipmap();

        let mut iter = skm.iter();

        iter.next();
        assert!(iter.valid());
        iter.prev();
        assert!(!iter.valid());
        iter.seek(b"abc");
        iter.prev();
        assert_eq!(
            iter.current().unwrap(),
            ("abb".as_bytes(), "def".as_bytes())
        );
    }
}
