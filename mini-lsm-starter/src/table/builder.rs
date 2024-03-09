use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use bytes::BufMut;
use nom::AsBytes;

use super::{BlockMeta, FileObject, SsTable};
use crate::{block::BlockBuilder, key::KeyBytes, key::KeySlice, lsm_storage::BlockCache};

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    builder: BlockBuilder,
    first_key: Vec<u8>, //用来存当前的 first key 用来构建meta
    last_key: Vec<u8>,  //还不太理解为什么要这个，为了确认范围？
    data: Vec<u8>,
    pub(crate) meta: Vec<BlockMeta>,
    block_size: usize,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        Self {
            builder: BlockBuilder::new(block_size),
            first_key: Vec::new(),
            last_key: Vec::new(),
            data: Vec::new(),
            meta: Vec::new(),
            block_size,
        }
    }

    /// Adds a key-value pair to SSTable.
    ///
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may
    /// be helpful here)
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        if self.first_key.is_empty() {
            self.first_key.extend(key.into_inner());
        }
        if self.builder.add(key, value) {
            self.last_key.clear();
            self.last_key.extend(key.into_inner());
            return;
        }
        // finish the laster block and create new block
        self.finish_block();
        self.builder = BlockBuilder::new(self.block_size);
        let flag = self.builder.add(key, value);

        self.first_key.extend(key.into_inner());
        self.last_key.extend(key.into_inner());
        assert!(flag);
    }

    /// Get the estimated size of the SSTable.
    ///
    /// Since the data blocks contain much more data than meta blocks, just return the size of data
    /// blocks here.
    pub fn estimated_size(&self) -> usize {
        self.data.len()
    }

    // 把最后一个block写入data
    fn finish_block(&mut self) {
        let builder = std::mem::replace(&mut self.builder, BlockBuilder::new(self.block_size));
        let last_block = builder.build();

        let encode_block = last_block.encode();
        self.data.extend_from_slice(&encode_block);

        self.meta.push(BlockMeta {
            offset: self.data.len() - encode_block.len(),
            first_key: KeyBytes::from_bytes(std::mem::take(&mut self.first_key).into()),
            last_key: KeyBytes::from_bytes(std::mem::take(&mut self.last_key).into()),
        });
    }

    /// Builds the SSTable and writes it to the given path. Use the `FileObject` structure to manipulate the disk objects.
    pub fn build(
        mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        self.finish_block();
        let mut buf = self.data;
        let offset = buf.len();
        BlockMeta::encode_block_meta(&self.meta, &mut buf);
        buf.put_u32(offset as u32);

        let sst_table = SsTable {
            file: FileObject::create(path.as_ref(), buf)?,
            block_cache,
            id,
            first_key: self.meta.first().unwrap().first_key.clone(),
            last_key: self.meta.last().unwrap().first_key.clone(),
            bloom: None,
            max_ts: u64::MAX,
            block_meta_offset: offset,
            block_meta: self.meta,
        };
        Ok(sst_table)
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
