use std::collections::HashMap;

use crate::types::SequenceNumber;

// Opaque snapshot handler; Represents index to Shapshotlist.map
pub type Snapshot = u64;

/// A list of all snapshot is kept in the DB.
#[derive(Default)]
pub struct SnapshotList {
    map: HashMap<Snapshot, SequenceNumber>,
    newest: Snapshot,
    oldest: Snapshot,
}

impl SnapshotList {
    pub fn new() -> SnapshotList {
        Default::default()
    }

    pub fn new_snapshot(&mut self, seq: SequenceNumber) -> Snapshot {
        self.newest += 1;
        self.map.insert(self.newest, seq);
        if self.oldest == 0 {
            self.oldest = self.newest;
        }

        self.newest
    }

    pub fn oldest(&self) -> SequenceNumber {
        self.map.get(&self.oldest).copied().unwrap()
    }

    pub fn newest(&self) -> SequenceNumber {
        self.map.get(&self.newest).copied().unwrap()
    }

    pub fn delete(&mut self, ss: Snapshot) {
        if self.oldest == ss {
            self.oldest += 1;
        }

        if self.newest == ss {
            self.newest -= 1;
        }

        self.map.remove(&ss);
    }

    pub fn empty(&self) -> bool {
        self.oldest == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_snapshot_list() {
        let mut l = SnapshotList::new();
        assert!(l.empty());

        let oldest = l.new_snapshot(1);
        l.new_snapshot(2);
        let newest = l.new_snapshot(0);

        assert!(!l.empty());

        assert_eq!(l.oldest(), 1);
        assert_eq!(l.newest(), 0);

        l.delete(newest);

        assert_eq!(l.newest(), 2);
        assert_eq!(l.oldest(), 1);

        l.delete(oldest);

        assert_eq!(l.oldest(), 2);
    }
}
