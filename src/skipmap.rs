#![allow(dead_code)]

use std::{
    cmp::Ordering,
    mem::{replace, transmute_copy},
};

use rand::{rngs::StdRng, RngCore, SeedableRng};

const MAX_HEIGHT: usize = 12;
const BRANCHING_FACTOR: u32 = 4;

pub trait Comparator {
    fn cmp(a: &[u8], b: &[u8]) -> Ordering;
}

pub struct StandardComparator;

impl Comparator for StandardComparator {
    fn cmp(a: &[u8], b: &[u8]) -> Ordering {
        a.cmp(b)
    }
}

struct Node {
    skips: Vec<Option<*mut Node>>,
    //skip[0] points to the element in next; next provides proper ownership
    next: Option<Box<Node>>,
    key: Vec<u8>,
    value: Vec<u8>,
}

pub struct SkipMap<C: Comparator> {
    head: Box<Node>,
    rand: StdRng,
    cmp: C,
    len: usize,
}

impl SkipMap<StandardComparator> {
    fn new() -> SkipMap<StandardComparator> {
        SkipMap::new_with_cmp(StandardComparator {})
    }
}

impl<C: Comparator> SkipMap<C> {
    fn new_with_cmp(c: C) -> SkipMap<C> {
        let mut s = Vec::new();
        s.resize(MAX_HEIGHT, None);

        SkipMap {
            head: Box::new(Node {
                skips: s,
                next: None,
                key: Vec::new(),
                value: Vec::new(),
            }),
            rand: StdRng::from_seed([47u8; 32]),
            cmp: c,
            len: 0,
        }
    }

    fn len(&self) -> usize {
        self.len
    }

    fn random_height(&mut self) -> usize {
        let mut height = 1;
        while height < MAX_HEIGHT && self.rand.next_u32() % BRANCHING_FACTOR == 0 {
            height += 1;
        }
        height
    }

    fn contains(&mut self, key: &[u8]) -> bool {
        if key.is_empty() {
            return false;
        }

        // Start at the highest skip link of the head node, and work down from there
        let mut current: *const Node = unsafe { transmute_copy(&self.head.as_ref()) };
        let mut level = self.head.skips.len() - 1;

        loop {
            unsafe {
                if let Some(next) = (*current).skips[level] {
                    let ord = C::cmp(&(*next).key, key);

                    match ord {
                        Ordering::Less => {
                            current = next;
                            continue;
                        }
                        Ordering::Equal => return true,
                        Ordering::Greater => (),
                    }
                }
            }

            if level == 0 {
                break;
            }
            level -= 1;
        }
        false
    }

    fn insert(&mut self, key: Vec<u8>, val: Vec<u8>) {
        assert!(!key.is_empty());
        assert!(!val.is_empty());

        let new_height = self.random_height();
        let mut prevs: Vec<Option<*mut Node>> = Vec::with_capacity(new_height);

        let mut level = MAX_HEIGHT - 1;
        let mut current: *mut Node = unsafe { transmute_copy(&self.head.as_mut()) };
        // Initialized all prevs entries with *head
        prevs.resize(new_height, Some(current));

        // Find the node after which we want to insert the new node; this is the node with the key
        // immediately smaller than the key to be inserted.
        loop {
            unsafe {
                if let Some(next) = (*current).skips[level] {
                    // If the wanted position is after the current node
                    let ord = C::cmp(&(*next).key, &key);

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
        let mut new_skips = Vec::with_capacity(new_height);
        new_skips.resize(new_height, None);

        let mut new = Box::new(Node {
            skips: new_skips,
            next: None,
            key,
            value: val,
        });

        let newp = unsafe { transmute_copy(&(new.as_mut())) };

        for (idx, item) in prevs.into_iter().enumerate().take(new_height) {
            if let Some(prev) = item {
                unsafe {
                    new.skips[idx] = (*prev).skips[idx];
                    (*prev).skips[idx] = Some(newp);
                }
            }
        }

        new.next = unsafe { replace(&mut (*current).next, None) };

        unsafe { replace(&mut (*current).next, Some(new)) };

        self.len += 1;
    }

    fn dbg_print(&self) {
        let mut current: *const Node = unsafe { transmute_copy(&self.head.as_ref()) };
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

#[cfg(test)]
mod tests {
    use super::{SkipMap, StandardComparator};

    fn make_skipmap() -> SkipMap<StandardComparator> {
        let mut skm = SkipMap::new();
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
        skm.dbg_print();
    }

    #[test]
    #[should_panic]
    fn test_no_dupes() {
        let mut skm = make_skipmap();
        // this should panic
        skm.insert("abc".as_bytes().to_vec(), "def".as_bytes().to_vec());
    }

    #[test]
    fn test_contains() {
        let mut skm = make_skipmap();
        assert!(skm.contains("aby".as_bytes()));
        assert!(skm.contains("abc".as_bytes()));
        assert!(skm.contains("abz".as_bytes()));
        assert!(!skm.contains("123".as_bytes()));
        assert!(!skm.contains("aaa".as_bytes()));
        assert!(!skm.contains("456".as_bytes()));
    }
}
