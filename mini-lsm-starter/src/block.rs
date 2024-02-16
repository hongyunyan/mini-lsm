mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::{BufMut, Bytes};
pub use iterator::BlockIterator;

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the tutorial
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        // 检查正确性
        assert!(!self.offsets.is_empty() && !self.data.is_empty());
        let mut data = self.data.clone();
        let len = self.offsets.len();
        for offset in &self.offsets {
            data.put_u16(*offset);
        }
        data.put_u16(len as u16);
        data.into()
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        assert!(data.len() > 4); // 2 for offset, 2 for size
        let mut data_vec = data.to_vec();
        let size = data_vec.split_off(data_vec.len() - 2);
        let size = u16::from_be_bytes([size[0], size[1]]) as usize;

        let offsets_vec = data_vec.split_off(data_vec.len() - size * 2);
        assert!(offsets_vec.len() % 2 == 0);

        let offsets: Vec<u16> = offsets_vec
            .chunks_exact(2)
            .map(|chunk| u16::from_be_bytes([chunk[0], chunk[1]]))
            .collect();

        Self {
            data: data_vec,
            offsets: offsets,
        }
    }
}
