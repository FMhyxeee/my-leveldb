#![allow(dead_code)]

use std::{
    cmp::Ordering,
    mem::{replace, transmute_copy},
};

use rand::{rngs::StdRng, RngCore, SeedableRng};

const MAX_HEIGHT: usize = 12;

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
        1 + (self.rand.next_u32() as usize % (MAX_HEIGHT - 1))
    }

    fn contains(&mut self, key: &Vec<u8>) -> bool {
        if key.is_empty() {
            return false;
        }

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

        prevs.resize(new_height, Some(current));

        loop {
            unsafe {
                if let Some(next) = (*current).skips[level] {
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

        let mut new_skips = Vec::with_capacity(new_height);
        new_skips.resize(new_height, None);

        let mut new = Box::new(Node {
            skips: new_skips,
            next: None,
            key,
            value: val,
        });

        let newp = unsafe { transmute_copy(&(new.as_mut())) };

        // for idx in 0..new_height {
        //     if let Some(prev) = prevs[idx] {
        //         unsafe {
        //             new.skips[idx] = (*prev).skips[idx];
        //             (*prev).skips[idx] = Some(newp);
        //         }
        //     }
        // }

        for (idx, _item) in prevs.iter().enumerate().take(new_height) {
            if let Some(prev) = prevs[idx] {
                unsafe {
                    new.skips[idx] = (*prev).skips[idx];
                    (*prev).skips[idx] = Some(newp);
                }
            }
        }

        new.next = unsafe { replace(&mut (*current).next, None) };

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
mod tests {}
