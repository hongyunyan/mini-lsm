use anyhow::Result;

use crate::key::Key;

use super::StorageIterator;

/// Merges two iterators of different types into one. If the two iterators have the same key, only
/// produce the key once and prefer the entry from A.
pub struct TwoMergeIterator<A: StorageIterator, B: StorageIterator> {
    a: A,
    b: B,
    choose_a: bool,
}

impl<
        A: 'static + StorageIterator,
        B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
    > TwoMergeIterator<A, B>
{
    pub fn create(a: A, b: B) -> Result<Self> {
        let mut iter = TwoMergeIterator {
            a,
            b,
            choose_a: true,
        };
        iter.choose_a = iter.choose_a();
        println!("when begin choose_a: {}", iter.choose_a);
        Ok(iter)
    }

    fn choose_a(&self) -> bool {
        if !self.a.is_valid() {
            return false;
        }
        if !self.b.is_valid() {
            return true;
        }
        self.a.key() <= self.b.key()
    }

    fn skip_b(&mut self) -> Result<()> {
        if self.b.is_valid() && self.b.key() == self.a.key() {
            self.b.next()?;
        }
        Ok(())
    }
}

impl<
        A: 'static + StorageIterator,
        B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
    > StorageIterator for TwoMergeIterator<A, B>
{
    type KeyType<'a> = A::KeyType<'a>;

    fn key(&self) -> Self::KeyType<'_> {
        if self.choose_a {
            self.a.key()
        } else {
            self.b.key()
        }
    }

    fn value(&self) -> &[u8] {
        if self.choose_a {
            self.a.value()
        } else {
            self.b.value()
        }
    }

    fn is_valid(&self) -> bool {
        if self.choose_a {
            self.a.is_valid()
        } else {
            self.b.is_valid()
        }
    }

    fn next(&mut self) -> Result<()> {
        if self.choose_a {
            self.skip_b()?;
            self.a.next()?;
        } else {
            self.b.next()?;
        }
        self.choose_a = self.choose_a();
        println!("next: choose_a: {}", self.choose_a,);
        Ok(())
    }
}
