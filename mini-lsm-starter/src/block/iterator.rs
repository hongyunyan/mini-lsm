use std::sync::Arc;

use crate::key::{KeySlice, KeyVec};

use super::Block;

/// Iterates on a block.
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// The current key, empty represents the iterator is invalid
    key: KeyVec,
    /// the value range from the block,用来记 value 的 begin 和 end
    value_range: (usize, usize),
    /// Current index of the key-value pair, should be in range of [0, num_of_elements)
    idx: usize,
    /// The first key in the block // 这玩意有啥用？
    first_key: KeyVec,
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        Self {
            block,
            key: KeyVec::new(),
            value_range: (0, 0),
            idx: 0,
            first_key: KeyVec::new(),
        }
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let mut iter = Self::new(block);
        iter.seek_to_first();
        iter.idx = 0;
        iter.first_key = iter.key.clone();
        iter
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: KeySlice) -> Self {
        let mut iter = Self::new(block);
        iter.seek_to_key(key);
        iter.idx = 0;
        iter
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> KeySlice {
        self.key.as_key_slice()
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        self.block.data[self.value_range.0..self.value_range.1].as_ref()
    }

    /// Returns true if the iterator is valid.
    /// Note: You may want to make use of `key`
    pub fn is_valid(&self) -> bool {
        !self.key.is_empty()
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        self.key = self.get_key_bases_on_offset(0);
        self.value_range = self.get_value_range_bases_on_offset(0);
        self.idx = 0;
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        if self.idx + 1 < self.block.offsets.len() {
            self.idx += 1;
            let offset = self.block.offsets[self.idx] as usize;
            self.key = self.get_key_bases_on_offset(offset);
            self.value_range = self.get_value_range_bases_on_offset(offset);
        } else {
            self.key.clear();
        }
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by
    /// callers.
    pub fn seek_to_key(&mut self, key: KeySlice) {
        let mut begin = 0;
        let mut end = self.block.offsets.len() - 1;

        while begin < end {
            let mid = (begin + end) / 2;
            let offset = self.block.offsets[mid] as usize;
            let mid_key = self.get_key_bases_on_offset(offset);
            if mid_key.as_key_slice().cmp(&key) == std::cmp::Ordering::Equal {
                self.idx = mid;
                self.key = mid_key;
                self.value_range = self.get_value_range_bases_on_offset(offset);
                return;
            }
            if mid_key.as_key_slice().cmp(&key) == std::cmp::Ordering::Less {
                begin = mid + 1;
            } else {
                end = mid;
            }
        }

        self.idx = begin;
        let offset = self.block.offsets[self.idx] as usize;
        self.key = self.get_key_bases_on_offset(offset);
        self.value_range = self.get_value_range_bases_on_offset(offset);
    }

    fn get_key_bases_on_offset(&self, offset: usize) -> KeyVec {
        assert!(self.block.data.len() > offset + 2);
        let key_len =
            u16::from_be_bytes([self.block.data[offset], self.block.data[offset + 1]]) as usize;
        assert!(self.block.data.len() > offset + 2 + key_len);
        KeyVec::from_vec(self.block.data[offset + 2..offset + 2 + key_len].to_vec())
    }

    fn get_value_range_bases_on_offset(&self, offset: usize) -> (usize, usize) {
        assert!(self.block.data.len() > offset + 2);
        let key_len =
            u16::from_be_bytes([self.block.data[offset], self.block.data[offset + 1]]) as usize;
        let value_offset_begin = offset + 2 + key_len;

        assert!(self.block.data.len() > value_offset_begin + 2);
        let value_len = u16::from_be_bytes([
            self.block.data[value_offset_begin],
            self.block.data[value_offset_begin + 1],
        ]) as usize;

        assert!(self.block.data.len() >= value_offset_begin + 2 + value_len);
        (value_offset_begin + 2, value_offset_begin + 2 + value_len)
    }
}
