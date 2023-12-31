use std::{cmp::Ordering, sync::Arc};

use crate::{
    key_types::{self, LookupKey},
    types,
};

/// Comparator trait, supporting types that can be nested (i.e., add additional functionality on)
/// top of an inner comparator)
pub trait Cmp {
    /// compare to byte strings, bytewise.
    fn cmp(&self, a: &[u8], b: &[u8]) -> Ordering;
    /// Return the shortest byte string that compares "Greater" to the first argument and "Less" to
    /// the second one.
    fn find_shortest_sep(&self, a: &[u8], b: &[u8]) -> Vec<u8>;
    /// Return the shortest byte string that comares "Greater" to the argument.
    fn find_short_succ(&self, a: &[u8]) -> Vec<u8>;
    /// A unique identifier for a comparator. A comparator warpper (like InternalKeyCmp) nay
    /// return the id of its inner comparator.
    fn id(&self) -> &'static str;
}

/// Lexical comparator.
#[derive(Clone)]
pub struct DefaultCmp;

impl Cmp for DefaultCmp {
    fn cmp(&self, a: &[u8], b: &[u8]) -> Ordering {
        a.cmp(b)
    }

    fn id(&self) -> &'static str {
        "leveldb.BytewiseComparator"
    }

    fn find_shortest_sep(&self, a: &[u8], b: &[u8]) -> Vec<u8> {
        if a == b {
            return a.to_vec();
        }

        let min = if a.len() < b.len() { a.len() } else { b.len() };
        let mut diff_at = 0;

        while diff_at < min && a[diff_at] == b[diff_at] {
            diff_at += 1;
        }

        while diff_at < min {
            let diff = a[diff_at];
            if diff < 0xff && diff + 1 < b[diff_at] {
                let mut sep = Vec::from(&a[0..diff_at + 1]);
                sep[diff_at] += 1;
                assert!(self.cmp(&sep, b) == Ordering::Less);
                return sep;
            }

            diff_at += 1;
        }

        // Backup case: either `a` is full of 0xff, or all different places are less than 2
        // characters apart.
        // The result is not necessarily short, but a good separator.
        let mut sep = a.to_vec();
        sep[a.len() - 1] += 1;
        sep
    }

    fn find_short_succ(&self, a: &[u8]) -> Vec<u8> {
        let mut result = a.to_vec();
        for i in 0..a.len() {
            if a[i] != 0xff {
                result[i] += 1;
                result.truncate(i + 1);
                return result;
            }
        }
        // Rare path
        result.push(255);
        result
    }
}

impl InternalKeyCmp {
    /// cmp_inner compares a and b using the underlying comparator (the "user comparator").
    fn cmp_inner(&self, a: &[u8], b: &[u8]) -> Ordering {
        self.0.cmp(a, b)
    }
}

/// Same as memtable_key_cmp, buf for InternalKeys.
#[derive(Clone)]
pub struct InternalKeyCmp(pub Arc<Box<dyn Cmp>>);

impl Cmp for InternalKeyCmp {
    fn cmp(&self, a: &[u8], b: &[u8]) -> Ordering {
        let (_, seqa, keya) = key_types::parse_internal_key(a);
        let (_, seqb, keyb) = key_types::parse_internal_key(b);

        match self.0.cmp(keya, keyb) {
            Ordering::Less => Ordering::Less,
            Ordering::Greater => Ordering::Greater,
            // reverse comparison!
            Ordering::Equal => seqb.cmp(&seqa),
        }
    }

    fn id(&self) -> &'static str {
        self.0.id()
    }

    fn find_shortest_sep(&self, a: &[u8], b: &[u8]) -> Vec<u8> {
        let (_, seqa, keya) = key_types::parse_internal_key(a);
        let (_, _, keyb) = key_types::parse_internal_key(b);

        let sep: Vec<u8> = self.0.find_shortest_sep(keya, keyb);

        if sep.len() < keya.len() && self.0.cmp(keya, &sep) == Ordering::Less {
            return LookupKey::new(&sep, types::MAX_SEQUENCE_NUMBER)
                .internal_key()
                .to_vec();
        }

        return LookupKey::new(&sep, seqa).internal_key().to_vec();
    }

    fn find_short_succ(&self, a: &[u8]) -> Vec<u8> {
        let (_, seq, key) = key_types::parse_internal_key(a);
        let succ: Vec<u8> = self.0.find_short_succ(key);
        return LookupKey::new(&succ, seq).internal_key().to_vec();
    }
}

/// An internal comparator wrapping a user-supplied comparator. This comparator is used to compare
/// memtable keys, which contain length prefixes and a sequence number.
/// The ordering is determined by asking the wrapped comparator; ties are broken by *reverse*
/// ordering the sequence number. (This means that when having an entry abx/4 and searching for abx/5,
/// then abx/4 is counted as "greater-or-equal", making snapshot functionality work at all)
#[derive(Clone)]
pub struct MemtableKeyCmp(pub Arc<Box<dyn Cmp>>);

impl Cmp for MemtableKeyCmp {
    fn cmp(&self, a: &[u8], b: &[u8]) -> Ordering {
        let (akeylen, akeyoff, atag, _, _) = key_types::parse_memtable_key(a);
        let (bkeylen, bkeyoff, btag, _, _) = key_types::parse_memtable_key(b);

        let userkey_a = &a[akeyoff..akeyoff + akeylen];
        let userkey_b = &b[bkeyoff..bkeyoff + bkeylen];

        match self.0.cmp(userkey_a, userkey_b) {
            Ordering::Less => Ordering::Less,
            Ordering::Greater => Ordering::Greater,
            Ordering::Equal => {
                let (_, aseq) = key_types::parse_tag(atag);
                let (_, bseq) = key_types::parse_tag(btag);

                // reverse!
                bseq.cmp(&aseq)
            }
        }
    }

    fn id(&self) -> &'static str {
        self.0.id()
    }

    // The following two impls should not be used (by principle) although they should be correct.
    // They will crash the program.
    fn find_shortest_sep(&self, _: &[u8], _: &[u8]) -> Vec<u8> {
        panic!("find* functions are invalid on MemtableKeyCmp");

        // let (akeylen, akeyoff, atag, _, _) = key_types::parse_memtable_key(a);
        // let (bkeylen, bkeyoff, _, _, _) = key_types::parse_memtable_key(a);
        // let (atyp, aseq) = key_types::parse_tag(atag);
        //
        // let sep: Vec<u8> = self.0.find_shortest_sep(&a[akeyoff..akeyoff + akeylen],
        // &b[bkeyoff..bkeyoff + bkeylen]);
        //
        // if sep.len() < akeylen &&
        // self.0.cmp(&a[akeyoff..akeyoff + akeylen], &sep) == Ordering::Less {
        // return key_types::build_memtable_key(&sep, &[0; 0], atyp, types::MAX_SEQUENCE_NUMBER);
        // }
        // return key_types::build_memtable_key(&sep, &[0; 0], atyp, aseq);
        //
    }

    fn find_short_succ(&self, _: &[u8]) -> Vec<u8> {
        panic!("find* functions are invalid on MemtableKeyCmp");

        // let (keylen, keyoff, tag, _, _) = key_types::parse_memtable_key(a);
        // let (typ, seq) = key_types::parse_tag(tag);
        //
        // let succ: Vec<u8> = self.0.find_short_succ(&a[keyoff..keyoff + keylen]);
        // return key_types::build_memtable_key(&succ, &[0; 0], typ, seq);
        //
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use key_types::LookupKey;

    use std::sync::Arc;

    #[test]
    fn test_cmp_defaultcmp_shortest_sep() {
        assert_eq!(
            DefaultCmp.find_shortest_sep("abcd".as_bytes(), "abcf".as_bytes()),
            "abce".as_bytes()
        );
        assert_eq!(
            DefaultCmp.find_shortest_sep("abc".as_bytes(), "acd".as_bytes()),
            "abd".as_bytes()
        );
        assert_eq!(
            DefaultCmp.find_shortest_sep("abcdefghi".as_bytes(), "abcffghi".as_bytes()),
            "abce".as_bytes()
        );
        assert_eq!(
            DefaultCmp.find_shortest_sep("a".as_bytes(), "a".as_bytes()),
            "a".as_bytes()
        );
        assert_eq!(
            DefaultCmp.find_shortest_sep("a".as_bytes(), "b".as_bytes()),
            "b".as_bytes()
        );
        assert_eq!(
            DefaultCmp.find_shortest_sep("abc".as_bytes(), "zzz".as_bytes()),
            "b".as_bytes()
        );
        assert_eq!(
            DefaultCmp.find_shortest_sep("yyy".as_bytes(), "z".as_bytes()),
            "yyz".as_bytes()
        );
        assert_eq!(
            DefaultCmp.find_shortest_sep("".as_bytes(), "".as_bytes()),
            "".as_bytes()
        );
    }

    #[test]
    fn test_cmp_defaultcmp_short_succ() {
        assert_eq!(
            DefaultCmp.find_short_succ("abcd".as_bytes()),
            "b".as_bytes()
        );
        assert_eq!(
            DefaultCmp.find_short_succ("zzzz".as_bytes()),
            "{".as_bytes()
        );
        assert_eq!(DefaultCmp.find_short_succ(&[]), &[0xff]);
        assert_eq!(
            DefaultCmp.find_short_succ(&[0xff, 0xff, 0xff]),
            &[0xff, 0xff, 0xff, 0xff]
        );
    }

    #[test]
    fn test_cmp_internalkeycmp_shortest_sep() {
        let cmp = InternalKeyCmp(Arc::new(Box::new(DefaultCmp)));
        assert_eq!(
            cmp.find_shortest_sep(
                LookupKey::new("abcd".as_bytes(), 1).internal_key(),
                LookupKey::new("abcf".as_bytes(), 2).internal_key()
            ),
            LookupKey::new("abce".as_bytes(), 1).internal_key()
        );
        assert_eq!(
            cmp.find_shortest_sep(
                LookupKey::new("abc".as_bytes(), 1).internal_key(),
                LookupKey::new("zzz".as_bytes(), 2).internal_key()
            ),
            LookupKey::new("b".as_bytes(), types::MAX_SEQUENCE_NUMBER).internal_key()
        );
        assert_eq!(
            cmp.find_shortest_sep(
                LookupKey::new("abc".as_bytes(), 1).internal_key(),
                LookupKey::new("acd".as_bytes(), 2).internal_key()
            ),
            LookupKey::new("abd".as_bytes(), 1).internal_key()
        );
        assert_eq!(
            cmp.find_shortest_sep(
                LookupKey::new("abc".as_bytes(), 1).internal_key(),
                LookupKey::new("abe".as_bytes(), 2).internal_key()
            ),
            LookupKey::new("abd".as_bytes(), 1).internal_key()
        );
        assert_eq!(
            cmp.find_shortest_sep(
                LookupKey::new("".as_bytes(), 1).internal_key(),
                LookupKey::new("".as_bytes(), 2).internal_key()
            ),
            LookupKey::new("".as_bytes(), 1).internal_key()
        );
        assert_eq!(
            cmp.find_shortest_sep(
                LookupKey::new("abc".as_bytes(), 2).internal_key(),
                LookupKey::new("abc".as_bytes(), 2).internal_key()
            ),
            LookupKey::new("abc".as_bytes(), 2).internal_key()
        );
    }

    #[test]
    fn test_cmp_internalkeycmp() {
        let cmp = InternalKeyCmp(Arc::new(Box::new(DefaultCmp)));
        // a < b < c
        let a = LookupKey::new("abc".as_bytes(), 2).internal_key().to_vec();
        let b = LookupKey::new("abc".as_bytes(), 1).internal_key().to_vec();
        let c = LookupKey::new("abd".as_bytes(), 3).internal_key().to_vec();
        let d = "xyy".as_bytes();
        let e = "xyz".as_bytes();

        assert_eq!(Ordering::Less, cmp.cmp(&a, &b));
        assert_eq!(Ordering::Equal, cmp.cmp(&a, &a));
        assert_eq!(Ordering::Greater, cmp.cmp(&b, &a));
        assert_eq!(Ordering::Less, cmp.cmp(&a, &c));
        assert_eq!(Ordering::Less, cmp.cmp_inner(d, e));
        assert_eq!(Ordering::Greater, cmp.cmp_inner(e, d));
    }

    #[test]
    #[should_panic]
    fn test_cmp_memtablekeycmp_panics() {
        let cmp = MemtableKeyCmp(Arc::new(Box::new(DefaultCmp)));
        cmp.cmp(&[1, 2, 3], &[4, 5, 6]);
    }
}
