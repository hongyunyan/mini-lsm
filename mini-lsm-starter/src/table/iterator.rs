#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::sync::Arc;

use anyhow::Result;

use super::SsTable;
use crate::{
    block::{Block, BlockIterator},
    iterators::StorageIterator,
    key::KeySlice,
};

/// An iterator over the contents of an SSTable.
pub struct SsTableIterator {
    table: Arc<SsTable>,
    blk_iter: BlockIterator,
    blk_idx: usize,
}

impl SsTableIterator {
    /// Create a new iterator and seek to the first key-value pair in the first data block.
    pub fn create_and_seek_to_first(table: Arc<SsTable>) -> Result<Self> {
        Ok(Self {
            blk_iter: BlockIterator::create_and_seek_to_first(table.read_block_cached(0).unwrap()),
            blk_idx: 0,
            table,
        })
    }

    /// Seek to the first key-value pair in the first data block.
    pub fn seek_to_first(&mut self) -> Result<()> {
        self.blk_iter =
            BlockIterator::create_and_seek_to_first(self.table.read_block_cached(0).unwrap());
        self.blk_idx = 0;
        Ok(())
    }

    fn find_block_with_key(table: &Arc<SsTable>, key: KeySlice) -> (BlockIterator, usize) {
        // use binary search to find the block for the first key-value pair
        let mut begin = 0;
        let mut end = table.block_meta.len() - 1;
        while begin < end {
            let mid = (begin + end) / 2;
            let mid_first_key = table.block_meta[mid].first_key.as_key_slice();
            let mid_last_key = table.block_meta[mid].last_key.as_key_slice();
            if mid_first_key.cmp(&key) != std::cmp::Ordering::Greater
                && mid_last_key.cmp(&key) != std::cmp::Ordering::Less
            {
                // key is in the range of the block
                return (
                    BlockIterator::create_and_seek_to_key(
                        table.read_block_cached(mid).unwrap(),
                        key,
                    ),
                    mid,
                );
            }
            if mid_first_key.cmp(&key) == std::cmp::Ordering::Greater {
                // mid_first_key > key
                end = mid;
            } else {
                // mid_first_key < key && mid_last_key < key
                begin = mid + 1;
            }
        }

        // 说明 key 大于所有 block 的 key value pair
        if begin >= table.block_meta.len() {
            return (
                BlockIterator::create_and_seek_to_key(
                    table.read_block_cached(table.block_meta.len() - 1).unwrap(),
                    key,
                ),
                table.block_meta.len() - 1,
            );
        }
        // 判断 key 是否大于 block 的 last key，是的话就变成下一个 block
        let cur_block_last_key = table.block_meta[begin].last_key.as_key_slice();
        if cur_block_last_key.cmp(&key) == std::cmp::Ordering::Less {
            // return next block
            if begin + 1 >= table.block_meta.len() {
                return (
                    BlockIterator::create_and_seek_to_key(
                        table.read_block_cached(begin).unwrap(),
                        key,
                    ),
                    begin,
                );
            }
            return (
                BlockIterator::create_and_seek_to_first(
                    table.read_block_cached(begin + 1).unwrap(),
                ),
                begin + 1,
            );
        };
        (
            BlockIterator::create_and_seek_to_key(table.read_block_cached(begin).unwrap(), key),
            begin,
        )
    }

    /// Create a new iterator and seek to the first key-value pair which >= `key`.
    pub fn create_and_seek_to_key(table: Arc<SsTable>, key: KeySlice) -> Result<Self> {
        let block_iter_with_idx = Self::find_block_with_key(&table, key);
        Ok(Self {
            blk_iter: block_iter_with_idx.0,
            blk_idx: block_iter_with_idx.1,
            table,
        })
    }

    /// Seek to the first key-value pair which >= `key`.
    /// Note: You probably want to review the handout for detailed explanation when implementing
    /// this function.
    pub fn seek_to_key(&mut self, key: KeySlice) -> Result<()> {
        let block_iter_with_idx = Self::find_block_with_key(&self.table, key);
        self.blk_iter = block_iter_with_idx.0;
        self.blk_idx = block_iter_with_idx.1;
        Ok(())
    }
}

impl StorageIterator for SsTableIterator {
    type KeyType<'a> = KeySlice<'a>;

    /// Return the `key` that's held by the underlying block iterator.
    fn key(&self) -> KeySlice {
        self.blk_iter.key()
    }

    /// Return the `value` that's held by the underlying block iterator.
    fn value(&self) -> &[u8] {
        self.blk_iter.value()
    }

    /// Return whether the current block iterator is valid or not.
    fn is_valid(&self) -> bool {
        self.blk_iter.is_valid()
    }

    /// Move to the next `key` in the block.
    /// Note: You may want to check if the current block iterator is valid after the move.
    fn next(&mut self) -> Result<()> {
        self.blk_iter.next();
        if !self.blk_iter.is_valid() {
            if self.blk_idx + 1 < self.table.block_meta.len() {
                self.blk_idx += 1;
                self.blk_iter = BlockIterator::create_and_seek_to_first(
                    self.table.read_block_cached(self.blk_idx).unwrap(),
                );
            }
        }
        Ok(())
    }
}
