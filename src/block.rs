use crate::{types::StandardComparator, Comparator};

use integer_encoding::FixedInt;

pub type BlockContents = Vec<u8>;

/// A block is a list of ENTRIES followed by a list of RESTARTS, terminated by a fixed u32
/// N_RESTARTS.
///
/// An ENTRY consists of three varints, SHARED, NON_SHARED, VALSIZE, a KEY and a VALUE.
///
/// SHARED denotes how many bytes the entry's key shares with the previous one.
///
/// NON_SHARED is the size of the key minus SHARED.
///
/// VALSIZE is the size of the value.
///
/// KEY and VALUE are byte strings; the length of KEY is NON_SHARED.
///
/// A RESTART is a fixed u32 pointing to the beginning of an ENTRY.
///
/// N_RESTARTS contains the number of restarts.
pub struct Block<C: Comparator> {
    data: BlockContents,
    restarts_off: usize,
    cmp: C,
}

impl Block<StandardComparator> {
    pub fn new(contents: BlockContents) -> Block<StandardComparator> {
        Self::new_with_cmp(contents, StandardComparator)
    }
}

impl<C: Comparator> Block<C> {
    pub fn new_with_cmp(contents: BlockContents, cmp: C) -> Block<C> {
        assert!(contents.len() > 4);
        let restarts = u32::decode_fixed(&contents[contents.len() - 4..]).unwrap() as usize;
        let restart_offset = contents.len() - 4 * (restarts + 1);

        Block {
            data: contents,
            restarts_off: restart_offset,
            cmp,
        }
    }

    fn number_restarts(&self) -> usize {
        ((self.data.len() - self.restarts_off) / 4) - 1
    }

    fn get_restart_point(&self, ix: usize) -> usize {
        let restart = self.restarts_off + 4 * ix;
        usize::decode_fixed(&self.data[restart..restart + 4]).unwrap()
    }

    // pub fn iter<'a>(&'a self) -> BlockIter<'a, C> {
    //     BlockIter {
    //         block: self,
    //         current_restart_ix: 0,
    //         offset: 0,
    //         key: Vec::new(),
    //         val_offset: 0,
    //     }
    // }
}
