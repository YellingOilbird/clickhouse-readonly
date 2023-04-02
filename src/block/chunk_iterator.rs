use std::cmp;

use crate::block::Block;
use crate::column::ColumnType;

pub struct ChunkIterator<'a, K: ColumnType> {
    position: usize,
    size: usize,
    block: &'a Block<K>,
}

impl<'a, K: ColumnType> Iterator for ChunkIterator<'a, K> {
    type Item = Block;

    fn next(&mut self) -> Option<Block> {
        let m = self.block.row_count();

        if m == 0 && self.position == 0 {
            self.position += 1;
            return Some(Block::default());
        }

        if self.position >= m {
            return None;
        }

        let mut result = Block::new();
        let size = cmp::min(self.size, m - self.position);

        for column in self.block.columns().iter() {
            let range = self.position..self.position + size;
            let data = column.slice(range);
            result = result.column(column.name(), data);
        }

        self.position += size;
        Some(result)
    }
}

impl<'a, K: ColumnType> ChunkIterator<'a, K> {
    pub fn new(size: usize, block: &Block<K>) -> ChunkIterator<K> {
        ChunkIterator {
            position: 0,
            size,
            block,
        }
    }
}
