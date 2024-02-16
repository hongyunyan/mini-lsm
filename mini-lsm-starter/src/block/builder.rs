use crate::key::{KeySlice, KeyVec};

use super::Block;

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        Self {
            offsets: Vec::new(),
            data: Vec::new(),
            block_size,
            first_key: KeyVec::new(),
        }
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        if !self.is_empty()
            && self.offsets.len() * 2 + self.data.len() + 2 + key.len() + value.len() + 4
                > self.block_size
        {
            return false;
        };

        self.offsets.push(self.data.len() as u16);
        // key len
        self.data
            .extend_from_slice(&(key.len() as u16).to_be_bytes());
        // push key into data
        self.data.extend_from_slice(key.into_inner());
        // value len
        self.data
            .extend_from_slice(&(value.len() as u16).to_be_bytes());
        // push value into data
        self.data.extend_from_slice(value);

        if self.is_empty() {
            self.first_key.set_from_slice(key);
        }
        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.first_key.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }
}
