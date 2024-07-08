use std::mem::{replace, transmute_copy};

use rand::{
    rngs::{StdRng, ThreadRng},
    RngCore, SeedableRng,
};

const MAX_HEIGHT: usize = 12;
const BRANCHING_FACTOR: usize = 4;

pub trait Comparator {
    fn cmp(a: &[u8], b: &[u8]) -> std::cmp::Ordering;
}

pub struct StandardComparator;

impl Comparator for StandardComparator {
    fn cmp(a: &[u8], b: &[u8]) -> std::cmp::Ordering {
        a.cmp(b)
    }
}

struct Node {
    skips: Vec<Option<*mut Node>>,
    // skips[0] points to the element in next; next provides proper ownership
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
        SkipMap::new_with_cmp(StandardComparator)
    }
}

impl<C: Comparator> SkipMap<C> {
    fn new_with_cmp(cmp: C) -> SkipMap<C> {
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
        }
    }

    fn len(&self) -> usize {
        self.len
    }

    fn random_height(&mut self) -> usize {
        let mut height = 1;
        while height < MAX_HEIGHT && self.rand.next_u32() % BRANCHING_FACTOR as u32 == 0 {
            height += 1;
        }
        height
    }

    fn contains(&mut self, key: &[u8]) -> bool {
        if key.is_empty() {
            return false;
        }

        // Start at the highest skip link of the head node, and work down from there
        let mut current: *const Node = &*self.head;
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
                            return true;
                        }
                        std::cmp::Ordering::Greater => (),
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

    fn insert(&mut self, key: &[u8], value: &[u8]) {
        assert!(!key.is_empty());
        assert!(!value.is_empty());

        // Keeping track of skip entries what will need to be update.

        let new_height = self.random_height();

        let mut level = MAX_HEIGHT - 1;
        let mut current: *mut Node = unsafe { transmute_copy(&self.head.as_mut()) };

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

        // Insert new node by first replacing the previous element's next field to the new node
        // assigning its value to new.next...
        new.next = unsafe { (*current).next.take() };

        let _ = unsafe { replace(&mut (*current).next, Some(new)) };

        self.len += 1;
    }

    // Runs through the skipmap and prints everything including addresses
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

#[cfg(test)]
mod tests {
    use super::*;

    fn make_skipmap() -> SkipMap<StandardComparator> {
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
        let mut sm = make_skipmap();
        assert!(sm.contains(b"abc"));
        assert!(!sm.contains(b"xyz"));
    }
}
