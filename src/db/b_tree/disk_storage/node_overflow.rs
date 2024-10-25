use core::num;
use std::{io::Read, ops::{Deref, DerefMut}};

use crate::db::shared::{constants::node_overflow::{NODE_OVERFLOW_ITEM_OFFSET_OFFSET, NODE_OVERFLOW_ITEM_OFFSET_SIZE, NODE_OVERFLOW_ITEM_SIZE, NODE_OVERFLOW_ITEM_SIZE_OFFSET, NODE_OVERFLOW_ITEM_SIZE_SIZE, NODE_OVERFLOW_META_DATA_SIZE, NODE_OVERFLOW_NUMBER_ITEMS_OFFSET}, utils::bytes_to_u32};


#[derive(Debug)]
pub struct NodeOverflow(Vec<u8>);

impl NodeOverflow {
    pub fn new(offset: u32, size: usize) -> NodeOverflow {
        let num_overflow_items: u32 = 1;
        let bytes = vec![]
            .into_iter()
            .chain(num_overflow_items.to_le_bytes())
            .chain((size as u32).to_le_bytes())
            .chain(offset.to_le_bytes())
            .collect();

        NodeOverflow(bytes)
    }

    pub fn from_bytes(bytes: Vec<u8>) -> NodeOverflow {
        NodeOverflow(bytes)
    }

    pub fn add_item(&mut self, offset: u32, size: usize) {
        let overflow_item: Vec<u8> = vec![]
            .into_iter()
            .chain((size as u32).to_le_bytes())
            .chain(offset.to_le_bytes())
            .collect();

        (*self).extend(overflow_item);
        self.increment_num_items();
    }

    pub fn num_items(&self) -> usize {
        let (start, end) = NodeOverflow::num_items_range();

        bytes_to_u32(&(*self)[start..end]) as usize
    }

    pub fn item(&self, i: usize) -> (u32, usize) {
        let start = NODE_OVERFLOW_META_DATA_SIZE + (NODE_OVERFLOW_ITEM_SIZE * i);
        let end = start + NODE_OVERFLOW_ITEM_SIZE;
        let (offset, size) = NodeOverflow::item_from_bytes(&(*self)[start..end]);

        (offset, size)
    }

    pub fn items(&self) -> Vec<(u32, usize)> {
        (*self)[NODE_OVERFLOW_META_DATA_SIZE..]
            .chunks(NODE_OVERFLOW_ITEM_SIZE)
            .map(|i| NodeOverflow::item_from_bytes(i))
            .collect()
    }

    fn increment_num_items(&mut self) {
        let (start, end) = NodeOverflow::num_items_range();

        let num_items = bytes_to_u32(&(*self)[start..end]) + 1;
        (*self).splice(start..end, num_items.to_le_bytes());
    }

    fn item_from_bytes(item: &[u8]) -> (u32, usize) {
        let size = bytes_to_u32(&item[NODE_OVERFLOW_ITEM_SIZE_OFFSET.. NODE_OVERFLOW_ITEM_SIZE_OFFSET + NODE_OVERFLOW_ITEM_SIZE_SIZE]) as usize;
        let offset = bytes_to_u32(&item[NODE_OVERFLOW_ITEM_OFFSET_OFFSET..NODE_OVERFLOW_ITEM_OFFSET_OFFSET + NODE_OVERFLOW_ITEM_OFFSET_SIZE]);

        (offset, size)
    }

    // TODO: *** I need to normalize this to be ELEMENT_SIZE bytes (SLAB allocation...) ***
    pub fn data(&self) -> &[u8] {
        &(*self)
    }

    fn num_items_range() -> (usize, usize) {
        (NODE_OVERFLOW_NUMBER_ITEMS_OFFSET, NODE_OVERFLOW_NUMBER_ITEMS_OFFSET + NODE_OVERFLOW_ITEM_OFFSET_SIZE)
    }
}

impl Deref for NodeOverflow {
    type Target = Vec<u8>;

   fn deref(&self) -> &Self::Target {
      &self.0
    }
}

impl DerefMut for NodeOverflow {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
