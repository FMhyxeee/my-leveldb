use std::{
    collections::HashMap,
    fmt::Debug,
    mem::{swap, transmute_copy},
};

use crate::block::BlockContents;

type LRUHandle<T> = *mut LRUNode<T>;

struct LRUNode<T> {
    next: Option<Box<LRUNode<T>>>, // None in the list's last node
    prev: Option<*mut LRUNode<T>>,
    data: Option<T>, // if None, then we have reached the head node
}

struct LRUList<T> {
    head: LRUNode<T>,
    count: usize,
}

/// This is likely unstable; more investigation is needed into correct hehavior!
impl<T: Debug> LRUList<T> {
    pub fn new() -> Self {
        Self {
            head: LRUNode {
                next: None,
                prev: None,
                data: None,
            },
            count: 0,
        }
    }

    /// Inserts new element at front (least recently used element)
    pub fn insert(&mut self, elem: T) -> LRUHandle<T> {
        self.count += 1;
        // Not first element
        if self.head.next.is_some() {
            // todo: repalce next by head.next; set head.next to new; set next.prev to new
            let mut new = Box::new(LRUNode {
                data: Some(elem),
                next: None,
                prev: Some(&mut self.head),
            });

            let newp = unsafe { transmute_copy(&new.as_mut()) };

            // Set up the node after the new node
            self.head.next.as_mut().unwrap().prev = Some(newp);
            // Replace head.next with None and set the new node's next to that
            new.next = self.head.next.take();
            self.head.next = Some(new);

            newp
        } else {
            // First node; the only node right now is an empty head node
            let mut new = Box::new(LRUNode {
                data: Some(elem),
                next: None,
                prev: Some(&mut self.head),
            });
            let newp = unsafe { transmute_copy(&new.as_mut()) };

            // Set tail
            self.head.prev = Some(newp);
            // Set first node
            self.head.next = Some(new);
            newp
        }
    }

    fn remove_last(&mut self) -> Option<T> {
        if self.head.prev.is_some() {
            let mut lasto = unsafe { (*((*self.head.prev.unwrap()).prev.unwrap())).next.take() };

            if let Some(ref mut last) = lasto {
                self.head.prev = last.prev;
                self.count -= 1;
                last.data.take()
            } else {
                None
            }
        } else {
            None
        }
    }

    /// Reinserts the referenced node at the front.
    fn reinsert_front(&mut self, node_handle: LRUHandle<T>) {
        unsafe {
            let prevp = (*node_handle).prev.unwrap();

            // Remove current node from list by setting next.prev to current's prev node
            if let Some(ref mut next) = (*node_handle).next {
                next.prev = (*node_handle).prev;
            }
            // Also, update head.prev if we're reinserting the last element
            if Some(node_handle) == self.head.prev {
                self.head.prev = (*node_handle).prev;
            }
            // Then swap the previous element's next (Box of current node) with the current node's next
            swap(&mut (*prevp).next, &mut (*node_handle).next);

            // Here, the element is removed.
            // To reinsert after head, swap the current node's next (Box of itself) with head's next
            swap(&mut (*node_handle).next, &mut self.head.next);

            // Proceed with setting references: Set the current node's prev to head..
            (*node_handle).prev = Some(&mut self.head);
            // ...and the next node's prev to a reference to the current node.
            if let Some(ref mut next) = (*node_handle).next {
                next.prev = Some(node_handle);
            }

            assert!(self.head.next.is_some());
        }
    }

    fn count(&self) -> usize {
        self.count
    }

    fn _testing_head_ref(&self) -> Option<&T> {
        if let Some(ref first) = self.head.next {
            first.data.as_ref()
        } else {
            None
        }
    }
}

type Cachehandle = usize;

/// Implementation of `SharedLRUCache`.
/// Based on a HashMap; the elements are linked in order to support the LRU ordering.
pub struct BlockCache {
    list: LRUList<Cachehandle>,
    map: HashMap<Cachehandle, BlockContents>,
    handle_counter: Cachehandle,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_blockcache_lru_1() {
        let mut lru = LRUList::<usize>::new();

        lru.insert(56);
        lru.insert(22);
        lru.insert(244);
        lru.insert(12);

        assert_eq!(lru.count(), 4);

        assert_eq!(Some(56), lru.remove_last());
        assert_eq!(Some(22), lru.remove_last());
        assert_eq!(Some(244), lru.remove_last());

        assert_eq!(lru.count(), 1);

        assert_eq!(Some(12), lru.remove_last());

        assert_eq!(lru.count(), 0);

        assert_eq!(None, lru.remove_last());
    }

    #[test]
    fn test_blockcache_lru_reinsert() {
        let mut lru = LRUList::<usize>::new();

        let handle1 = lru.insert(56);
        let handle2 = lru.insert(22);
        let handle3 = lru.insert(244);

        assert_eq!(lru._testing_head_ref().copied().unwrap(), 244);

        lru.reinsert_front(handle1);

        assert_eq!(lru._testing_head_ref().copied().unwrap(), 56);

        lru.reinsert_front(handle3);

        assert_eq!(lru._testing_head_ref().copied().unwrap(), 244);

        lru.reinsert_front(handle2);

        assert_eq!(lru._testing_head_ref().copied().unwrap(), 22);

        assert_eq!(lru.remove_last(), Some(56));
        assert_eq!(lru.remove_last(), Some(244));
        assert_eq!(lru.remove_last(), Some(22));
    }
}
