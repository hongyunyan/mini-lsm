use anyhow::{Ok, Result};
use moka::sync::Iter;
use std::cmp::{self};
use std::collections::binary_heap::PeekMut;
use std::collections::BinaryHeap;
use std::mem;

use crate::key::KeySlice;

use super::StorageIterator;

struct HeapWrapper<I: StorageIterator>(pub usize, pub Box<I>);

impl<I: StorageIterator> PartialEq for HeapWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.partial_cmp(other).unwrap() == cmp::Ordering::Equal
    }
}

impl<I: StorageIterator> Eq for HeapWrapper<I> {}

impl<I: StorageIterator> PartialOrd for HeapWrapper<I> {
    #[allow(clippy::non_canonical_partial_ord_impl)]
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        match self.1.key().cmp(&other.1.key()) {
            cmp::Ordering::Greater => Some(cmp::Ordering::Greater),
            cmp::Ordering::Less => Some(cmp::Ordering::Less),
            cmp::Ordering::Equal => self.0.partial_cmp(&other.0),
        }
        .map(|x| x.reverse())
    }
}

impl<I: StorageIterator> Ord for HeapWrapper<I> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}

/// Merge multiple iterators of the same type. If the same key occurs multiple times in some
/// iterators, prefer the one with smaller index.
pub struct MergeIterator<I: StorageIterator> {
    iters: BinaryHeap<HeapWrapper<I>>,
    current: Option<HeapWrapper<I>>,
}

impl<I: StorageIterator> MergeIterator<I> {
    pub fn create(iters: Vec<Box<I>>) -> Self {
        if iters.is_empty() {
            return Self {
                iters: BinaryHeap::new(),
                current: None,
            };
        }
        println!("create mergeIterator non empty");

        let mut merge_iter = MergeIterator {
            iters: BinaryHeap::new(),
            current: None,
        };
        for (idx, iter) in iters.into_iter().enumerate() {
            if iter.is_valid() {
                merge_iter.iters.push(HeapWrapper(idx, iter));
            }
        }
        if merge_iter.iters.is_empty() {
            return merge_iter;
        }
        merge_iter.current = merge_iter.iters.pop();
        merge_iter
    }
}

impl<I: 'static + for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>> StorageIterator
    for MergeIterator<I>
{
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        self.current.as_ref().unwrap().1.key()
    }

    fn value(&self) -> &[u8] {
        self.current.as_ref().unwrap().1.value()
    }

    fn is_valid(&self) -> bool {
        self.current
            .as_ref()
            .map(|x| x.1.is_valid())
            .unwrap_or(false)
    }

    fn next(&mut self) -> Result<()> {
        // // 因为所有的iter 内部也是按照 key 排序的，而且 binary heap 也是先根据 iter 的 第一个 key 排序的
        // // 所以我们每次先取出有这个 key 的 iter，把他们第一个都删掉。
        // // 把含有这个 key 的 iter 做 pop，然后塞进 binary heap

        let current_iter = self.current.as_mut().unwrap();

        // 找到下一个 key 相同的 iter
        while let Some(mut iter) = self.iters.peek_mut() {
            if iter.1.key() == current_iter.1.key() {
                if let e @ Err(_) = iter.1.next() {
                    PeekMut::pop(iter);
                    return e;
                }

                if !iter.1.is_valid() {
                    PeekMut::pop(iter);
                }
            } else {
                break;
            }
        }

        current_iter.1.next()?;

        if current_iter.1.is_valid() {
            // compare with the top one
            if let Some(mut iter) = self.iters.peek_mut() {
                if *current_iter < *iter {
                    std::mem::swap(&mut *iter, current_iter)
                };
            }
        } else {
            if let Some(iter) = self.iters.pop() {
                self.current = Some(iter);
            }
        }

        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.iters
            .iter()
            .map(|x| x.1.num_active_iterators())
            .sum::<usize>()
            + self
                .current
                .as_ref()
                .map(|x| x.1.num_active_iterators())
                .unwrap_or(0)
    }
}
