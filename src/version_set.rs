use crate::cmp::Cmp;
use crate::cmp::InternalKeyCmp;
use crate::key_types::parse_internal_key;
use crate::key_types::InternalKey;
use crate::key_types::UserKey;
use crate::options::Options;
use crate::types::Shared;
use crate::types::NUM_LEVELS;
use crate::version::FileMetaHandle;
use crate::version::Version;
use crate::version_edit::VersionEdit;
use std::cmp::Ordering;
use std::rc::Rc;

struct Compaction {
    level: usize,
    max_file_size: usize,
    input_version: Option<Shared<Version>>,
    level_ixs: [usize; NUM_LEVELS],
    cmp: Rc<Box<dyn Cmp>>,

    // "parent" inputs from level and level+1.
    inputs: [Vec<FileMetaHandle>; 2],
    grandparent_ix: usize,
    // remaining inputs from level+2..NUM_LEVELS
    grandparents: Option<Vec<FileMetaHandle>>,
    overlapped_bytes: usize,
    seen_key: bool,
    pub edit: VersionEdit,
}

impl Compaction {
    // Note: opt.cmp should be the user-supplied or default comparator (not an InternalKeyCmp).
    fn new(opt: &Options, level: usize) -> Compaction {
        Compaction {
            level,
            max_file_size: opt.max_file_size,
            input_version: None,
            level_ixs: Default::default(),
            cmp: opt.cmp.clone(),

            inputs: Default::default(),
            grandparent_ix: 0,
            grandparents: Default::default(),
            overlapped_bytes: 0,
            seen_key: false,
            edit: VersionEdit::new(),
        }
    }

    /// add_input_deletions marks the current input files as deleted in the inner VersionEdit.
    fn add_input_deletions(&mut self) {
        for parent in 0..2 {
            for f in &self.inputs[parent] {
                self.edit.delete_file(self.level + parent, f.borrow().num);
            }
        }
    }

    fn input(&self, parent: usize, i: usize) -> FileMetaHandle {
        assert!(parent < 2);
        assert!(i < self.inputs[parent].len());

        self.inputs[parent][i].clone()
    }

    /// is_base_level_for checks whether the given key may exist in levels higher than this
    /// compaction's level plus 2. I.e., whether the levels for this compaction are the last ones
    /// to contain the key.
    fn is_base_level_for(&mut self, k: UserKey) -> bool {
        if let Some(ref inp_version) = self.input_version {
            for level in self.level + 2..NUM_LEVELS {
                let files = &inp_version.borrow().files[level];
                while self.level_ixs[level] < files.len() {
                    let f = files[self.level_ixs[level]].borrow();
                    if self.cmp.cmp(k, parse_internal_key(&f.largest).2) <= Ordering::Equal {
                        if self.cmp.cmp(k, parse_internal_key(&f.smallest).2) >= Ordering::Equal {
                            // key is in this file's range, so this is not the base level.
                            return false;
                        }
                        break;
                    }
                    self.level_ixs[level] += 1;
                }
            }
            true
        } else {
            unimplemented!()
        }
    }

    fn num_inputs(&self, parent: usize) -> usize {
        assert!(parent < 2);
        self.inputs[parent].len()
    }

    fn is_trivial_move(&self) -> bool {
        let inputs_size = self
            .grandparents
            .as_ref()
            .unwrap_or(&vec![])
            .iter()
            .fold(0, |a, f| a + f.borrow().size);
        self.num_inputs(0) == 1
            && self.num_inputs(1) == 0
            && inputs_size < 10 * self.max_file_size as u64
    }

    fn should_stop_before(&mut self, k: InternalKey) -> bool {
        assert!(self.grandparents.is_some());
        let grandparents = self.grandparents.as_ref().unwrap();
        let icmp = InternalKeyCmp(self.cmp.clone());
        while self.grandparent_ix < grandparents.len()
            && icmp.cmp(k, &grandparents[self.grandparent_ix].borrow().largest) == Ordering::Greater
        {
            if self.seen_key {
                self.overlapped_bytes += grandparents[self.grandparent_ix].borrow().size as usize;
            }
            self.grandparent_ix += 1;
        }
        self.seen_key = true;

        if self.overlapped_bytes > 10 * self.max_file_size {
            self.overlapped_bytes = 0;
            true
        } else {
            false
        }
    }
}
