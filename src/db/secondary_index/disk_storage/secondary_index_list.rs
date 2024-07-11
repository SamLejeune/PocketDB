use std::ops::{Deref, DerefMut};

use crate::db::shared::{constants::{secondary_index_item::{SECONDARY_INDEX_ITEM_COLUMN_INDEX_OFFSET, SECONDARY_INDEX_ITEM_COLUMN_INDEX_SIZE, SECONDARY_INDEX_ITEM_OFFSET_OFFSET, SECONDARY_INDEX_ITEM_OFFSET_SIZE, SECONDARY_INDEX_ITEM_SIZE}, secondary_index_list::{SECONDARY_INDEX_LIST_META_DATA_OFFSET, SECONDARY_INDEX_LIST_META_DATA_SIZE, SECONDARY_INDEX_LIST_NUMBER_ITEMS_OFFSET, SECONDARY_INDEX_LIST_NUMBER_ITEMS_SIZE, SECONDARY_INDEX_LIST_SIZE_OFFSET, SECONDARY_INDEX_LIST_SIZE_SIZE}}, utils::bytes_to_u32};

#[derive(Debug)]
pub struct SecondaryIndexList(Vec<u8>);

impl SecondaryIndexList {
    pub fn new() -> SecondaryIndexList {
        let size = (SECONDARY_INDEX_LIST_META_DATA_SIZE as u32).to_le_bytes();
        let num_items = 0u32.to_le_bytes();

        let bytes = vec![]
            .into_iter()
            .chain(size)
            .chain(num_items)
            .collect();

        SecondaryIndexList(bytes)
    }

    pub fn from_bytes(mut bytes: Vec<u8>) -> SecondaryIndexList {
        let (start, end) = SecondaryIndexList::size_range();
        let size = bytes_to_u32(&(&bytes)[start..end]) as usize;
        bytes.drain(size..);

        SecondaryIndexList(bytes)
    }

    pub fn add_item(&mut self, offset: u32, column_index: u32) {
        let secondary_index_item: Vec<u8> = vec![]
            .into_iter()
            .chain(offset.to_le_bytes())
            .chain(column_index.to_le_bytes())
            .collect();

        self.extend(secondary_index_item);
        self.increment_size();
        self.increment_num_items();
    }

    pub fn set_item_offset(&mut self, offset: u32, i: usize) {
        if i < self.num_items() {
            let (start, end) = self.item_offset_range(i);

            (*self).splice(start..end, offset.to_le_bytes());
        }
    }

    pub fn item(&self, i: usize) -> Option<(u32, usize)> {
        if i < self.num_items() {
            let (offset_start, offset_end) = self.item_offset_range(i);
            let (column_index_start, column_index_end) = self.item_column_index_range(i);
            
            let item_offset = bytes_to_u32(&(*self)[offset_start..offset_end]);
            let column_index = bytes_to_u32(&(*self)[column_index_start..column_index_end]);

            return Some((item_offset, column_index as usize));
        }
        None
    }

    pub fn size(&self) -> usize {
        let (start, end) = SecondaryIndexList::size_range();

        bytes_to_u32(&(*self)[start..end]) as usize
    }

    pub fn num_items(&self) -> usize {
        let (start, end) = SecondaryIndexList::num_items_range();

        bytes_to_u32(&(*self)[start..end]) as usize
    }

    pub fn data(&self) -> &[u8] {
        &(*self)
    }

    fn increment_size(&mut self) {
        let (start, end) = SecondaryIndexList::size_range();
        let size = (self.size() + SECONDARY_INDEX_ITEM_SIZE) as u32;
        
        (*self).splice(start..end, size.to_le_bytes());
    }

    fn increment_num_items(&mut self) {
        let (start, end) = SecondaryIndexList::num_items_range();
        let num_items = (self.num_items() + 1) as u32;
        
        (*self).splice(start..end, num_items.to_le_bytes());
    }

    fn size_range() -> (usize, usize) {
        (SECONDARY_INDEX_LIST_SIZE_OFFSET, SECONDARY_INDEX_LIST_SIZE_OFFSET + SECONDARY_INDEX_LIST_SIZE_SIZE)
    }

    fn num_items_range() -> (usize, usize) {
        (SECONDARY_INDEX_LIST_NUMBER_ITEMS_OFFSET, SECONDARY_INDEX_LIST_NUMBER_ITEMS_OFFSET + SECONDARY_INDEX_LIST_NUMBER_ITEMS_SIZE)
    }

    fn meta_data_range() -> (usize, usize) {
        (SECONDARY_INDEX_LIST_META_DATA_OFFSET, SECONDARY_INDEX_LIST_META_DATA_OFFSET + SECONDARY_INDEX_LIST_META_DATA_SIZE)
    }

    fn item_range(&self, i: usize) -> (usize, usize) {
        let (_, end) = SecondaryIndexList::meta_data_range();
        let start = end + (SECONDARY_INDEX_ITEM_SIZE * i);

        (start, start + SECONDARY_INDEX_ITEM_SIZE)
    }

    fn item_offset_range(&self, i: usize) -> (usize, usize) {
        let (start, _) = self.item_range(i);

        (start + SECONDARY_INDEX_ITEM_OFFSET_OFFSET, start + SECONDARY_INDEX_ITEM_OFFSET_OFFSET + SECONDARY_INDEX_ITEM_OFFSET_SIZE)
    }

    fn item_column_index_range(&self, i: usize) -> (usize, usize) {
        let (start, _) = self.item_range(i);

        (start + SECONDARY_INDEX_ITEM_COLUMN_INDEX_OFFSET, start + SECONDARY_INDEX_ITEM_COLUMN_INDEX_OFFSET + SECONDARY_INDEX_ITEM_COLUMN_INDEX_SIZE)
    }     
}

impl Deref for SecondaryIndexList {
  type Target = Vec<u8>;

  fn deref(&self) -> &Self::Target {
      &self.0
  }
}

impl DerefMut for SecondaryIndexList {
  fn deref_mut(&mut self) -> &mut Self::Target {
      &mut self.0
  }
}