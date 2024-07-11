use std::{f32::consts::E, ops::{Deref, DerefMut}};

use crate::db::shared::{constants::free_list_item::{FREE_ITEM_DATA_OFFSET, FREE_ITEM_DATA_OFFSET_OFFSET, FREE_ITEM_DATA_SIZE, FREE_ITEM_DATA_SIZE_OFFSET, FREE_ITEM_SIZE}, utils::bytes_to_u32};


#[derive(Debug)]
pub struct FreeListItem(Vec<u8>);

impl FreeListItem {
    pub fn new(offset: usize, size: usize) -> FreeListItem {
        let mut bytes = vec![];
        bytes.extend((offset as u32).to_le_bytes());
        bytes.extend((size as u32).to_le_bytes());

        FreeListItem(bytes)
    }

    pub fn from_bytes(bytes: &[u8]) -> FreeListItem {
        FreeListItem(bytes.to_vec())
    }

    pub fn offset(&self) -> usize {
        let (start, end) = FreeListItem::offset_range();
        
        bytes_to_u32(&(*self)[start..end]) as usize
    }

    pub fn size(&self) -> usize {
        let (start, end) = FreeListItem::size_range();

        bytes_to_u32(&(*self)[start..end]) as usize
    }

    pub fn data(&self) -> &[u8] {
        &(*self)
    }

    pub fn move_offset(&mut self, move_by: usize) {
        let (start, end) = FreeListItem::offset_range();
        let offset = self.offset() + move_by;

        (*self).splice(start..end, (offset as u32).to_le_bytes());
    }

    pub fn increase_size(&mut self, size: usize) {
        let (start, end) = FreeListItem::size_range();
        let size = self.size() + size;

        (*self).splice(start..end, (size as u32).to_le_bytes());   
    }

    pub fn decrease_size(&mut self, size: usize) {
        let (start, end) = FreeListItem::size_range();
        let size = self.size() - size;

        (*self).splice(start..end, (size as u32).to_le_bytes());    
    }

    fn offset_range() -> (usize, usize) {
        (FREE_ITEM_DATA_OFFSET_OFFSET, FREE_ITEM_DATA_OFFSET_OFFSET + FREE_ITEM_DATA_OFFSET)
    }

    fn size_range() -> (usize, usize) {
        (FREE_ITEM_DATA_SIZE_OFFSET, FREE_ITEM_DATA_SIZE_OFFSET + FREE_ITEM_DATA_SIZE)
    }
}

impl Deref for FreeListItem {
    type Target = Vec<u8>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for FreeListItem {
    fn deref_mut(&mut self) -> &mut Self::Target {
            &mut self.0
    }
}

#[derive(Debug)]
pub struct FreeList {
    free_list: Vec<FreeListItem>,
    reclaim_list: Vec<FreeListItem>,
}

impl FreeList {
    pub fn new() -> FreeList {
        FreeList { free_list: vec![], reclaim_list: vec![] }
    }

    pub fn from_bytes(free_list_bytes: Vec<u8>, reclaim_list_bytes: Vec<u8>) -> FreeList {
        let free_list = free_list_bytes
            .chunks(FREE_ITEM_SIZE)
            .map(|item_bytes| FreeListItem::from_bytes(&item_bytes))
            .collect();
        
        let reclaim_list = reclaim_list_bytes
            .chunks(FREE_ITEM_SIZE)
            .map(|item_bytes| FreeListItem::from_bytes(&item_bytes))
            .collect(); 
        
        FreeList { free_list, reclaim_list }
    }

    pub fn add_to_free_list(&mut self, item: FreeListItem) {
        self.free_list.push(item);
    }

    pub fn reclaim_from_free_list(&mut self, size: usize) -> Option<usize> {
        if let Some(mut item) = self.reclaim_list.pop() {
            if size <= item.size() {
                let curr_offset = item.offset();

                if size < item.size() {
                    item.move_offset(size);
                    item.decrease_size(size);
                    self.reclaim_list.push(item);
                }

                return Some(curr_offset);
            } else {
                if let Some(item) = self.reclaim_list
                    .iter_mut()
                    .find(|item| size < item.size()) {
                        let curr_offset = item.offset();
                        item.move_offset(size);
                        item.decrease_size(size);
                        
                        return Some(curr_offset);
                    } else {
                        self.free_list.push(item);
                        return None;
                    }
            }
        }

        None 
    }

    pub fn condense_free_list(&mut self) {
        let mut free_memory = vec![];
        free_memory.extend(self.free_list.drain(..));
        free_memory.extend(self.reclaim_list.drain(..));
        free_memory
            .sort_by(|a, b| b.offset().cmp(&a.offset()));
        
        let mut condensed_free_list: Vec<FreeListItem> = vec![];
        for item in free_memory {
            if let Some(condensed_item) = condensed_free_list.last_mut() {
                if condensed_item.offset() + condensed_item.size() == item.offset() {
                    condensed_item.increase_size(item.size());
                } else {
                    condensed_free_list.push(item);
                }
            } else {
                condensed_free_list.push(item);
            }
        }

        condensed_free_list
            .sort_by(|a, b| b.size().cmp(&a.size()));

        self.reclaim_list = condensed_free_list;
    }

    pub fn free_list_data(&self) -> Vec<u8> {
        self.free_list.iter()
            .fold(vec![], |mut data, item| {
                data.extend(item.data().to_vec());
                data
            })
    }

    pub fn recliam_list_data(&self) -> Vec<u8> {
        self.reclaim_list.iter()
            .fold(vec![], |mut data, item| {
                data.extend(item.data().to_vec());
                data
        }) 
    }

    pub fn free_list_len(&self) -> usize {
        self.free_list.len()
    }

    pub fn reclaim_list_len(&self) -> usize {
        self.reclaim_list.len()
    }
}