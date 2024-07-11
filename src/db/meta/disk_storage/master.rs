use core::num;
use std::ops::{Deref, DerefMut};

use crate::db::shared::{constants::{free_list_item::FREE_ITEM_SIZE, master::*}, utils::{self, bytes_to_u32}};

#[derive(Debug)]
pub struct Master(Vec<u8>);

impl Master {
    pub fn new() -> Master {
        Master(vec![0; MASTER_SIZE])
    }

    pub fn from_bytes(bytes: &[u8]) -> Master {
        Master(bytes.to_vec())
    }

    pub fn set_primary_root_size(&mut self, size: u32) {
        let (start, end) = Master::primary_root_size_range();

        (*self).splice(start..end, size.to_le_bytes());
    }

    pub fn set_primary_root_offset(&mut self, offset: u32) {
        let (start, end) = Master::primary_root_offset_range();

        (*self).splice(start..end, offset.to_le_bytes());
    }

    pub fn set_secondary_index_list_size(&mut self, size: u32) {
        let (start, end) = Master::secondary_index_list_size_range();

        (*self).splice(start..end, size.to_le_bytes());
    }

    pub fn set_secondary_index_list_offset(&mut self, offset: u32) {
        let (start, end) = Master::secondary_index_list_offset_range();

        (*self).splice(start..end, offset.to_le_bytes());
    }

    pub fn set_table_columns_size(&mut self, size: u32) {
        let (start, end) = Master::table_columns_size_range();

        (*self).splice(start..end, size.to_le_bytes());
    }

    pub fn set_table_columns_offset(&mut self, offset: u32) {
        let (start, end) = Master::tabel_columns_offset_range();

        (*self).splice(start..end, offset.to_le_bytes());
    }

    pub fn set_free_list_number_items(&mut self, count: u32) {
        let (start, end) = Master::free_list_number_items_range();

        (*self).splice(start..end, (count).to_le_bytes());
    }

    pub fn set_free_list_offset(&mut self, offset: u32) {
        let (start, end) = Master::free_list_offset_range();

        (*self).splice(start..end, (offset).to_le_bytes());
    }

    pub fn set_reclaim_list_number_items(&mut self, count: u32) {
        let (start, end) = Master::reclaim_list_number_items_range();
        
        (*self).splice(start..end, (count).to_le_bytes());
    }

    pub fn set_reclaim_list_offset(&mut self, offset: u32) {
        let (start, end) = Master::reclaim_list_offset_range();

        (*self).splice(start..end, (offset).to_le_bytes());
    }

    pub fn primary_root_size(&self) -> u32 {
        let (start, end) = Master::primary_root_size_range();

        utils::bytes_to_u32(&(*self)[start..end])
    }

    pub fn primary_root_offset(&self) -> u32 {
        let (start, end) = Master::primary_root_offset_range();

        utils::bytes_to_u32(&(*self)[start..end])
    }

    pub fn secondary_index_list_size(&self) -> u32 {
        let (start, end) = Master::secondary_index_list_size_range();

        bytes_to_u32(&(*self)[start..end])
    }

    pub fn secondary_index_list_offset(&self) -> u32 {
        let (start, end) = Master::secondary_index_list_offset_range();

        bytes_to_u32(&(*self)[start..end])
    }

    pub fn table_columns_size(&self) -> u32 {
        let (start, end) = Master::table_columns_size_range();

        bytes_to_u32(&(*self)[start..end])
    }

    pub fn table_columns_offset(&self) -> u32 {
        let (start, end) = Master::tabel_columns_offset_range();

        bytes_to_u32(&(*self)[start..end])
    }

    pub fn free_list_number_items(&self) -> u32 {
        let (start, end) = Master::free_list_number_items_range();

        bytes_to_u32(&(*self)[start..end])
    }

    pub fn free_list_offset(&self) -> u32 {
        let (start, end) = Master::free_list_offset_range();

        bytes_to_u32(&(*self)[start..end])
    }

    pub fn reclaim_list_number_items(&self) -> u32 {
        let (start, end) = Master::reclaim_list_number_items_range();

        bytes_to_u32(&(*self)[start..end])
    }
    
    pub fn reclaim_list_offset(&self) -> u32 {
        let (start, end) = Master::reclaim_list_offset_range();

        bytes_to_u32(&(*self)[start..end])
    }

    // TOO: remove hard-coded 92
    pub fn secondary_index_list_len(&self) -> usize {
        let mut size = self.secondary_index_list_size() as usize;

        let padding = 92 - (size % 92);
        if padding < 92 {
            size += padding;
        }

        size
    }

    // TOO: remove hard-coded 92
    pub fn table_columns_len(&self) -> usize {
        let mut size = self.table_columns_size() as usize;

        let padding = 92 - (size % 92);
        if padding < 92 {
            size += padding;
        }

        size
    }

    // TODO: remove hard-coded 92
    pub fn free_list_len(&self) -> usize {
        let mut size = self.free_list_number_items() as usize * FREE_ITEM_SIZE;

        let padding = 92 - (size % 92);
        if padding < 92 {
            size += padding;
        }

        size
    }

    // TODO: remove hard-coded 92
    pub fn reclaim_list_len(&self) -> usize {
        // let mut num_items = self.free_list_number_items() as usize;
        let mut size = self.reclaim_list_number_items() as usize * FREE_ITEM_SIZE;
        
        let padding = 92 - (size % 92);
        if padding < 92 {
            size += padding;
        }

        size
    }
 
    pub fn data(&self) -> &[u8] {
        &(*self)
    }

    fn primary_root_size_range() -> (usize, usize) {
        (PRIMARY_ROOT_SIZE_OFFSET, PRIMARY_ROOT_SIZE_OFFSET + PRIMARY_ROOT_SIZE_SIZE)
    }

    fn primary_root_offset_range() -> (usize, usize) {
        (PRIMARY_ROOT_OFFSET_OFFSET, PRIMARY_ROOT_OFFSET_OFFSET + PRIMARY_ROOT_OFFSET_SIZE)
    }

    fn secondary_index_list_size_range() -> (usize, usize) {
        (SECONDARY_INDEX_LIST_SIZE_OFFSET, SECONDARY_INDEX_LIST_SIZE_OFFSET + SECONDARY_INDEX_LIST_SIZE_SIZE)
    }

    fn secondary_index_list_offset_range() -> (usize, usize) {
        (SECONDARY_INDEX_LIST_OFFSET_OFFSET, SECONDARY_INDEX_LIST_OFFSET_OFFSET + SECONDARY_INDEX_LIST_OFFSET_SIZE)
    }

    fn table_columns_size_range() -> (usize, usize) {
        (TABLE_COLUMNS_LIST_SIZE_OFFSET, TABLE_COLUMNS_LIST_SIZE_OFFSET + TABLE_COLUMNS_LIST_SIZE_SIZE)
    }

    fn tabel_columns_offset_range() -> (usize, usize) {
        (TABLE_COLUMNS_LIST_OFFSET_OFFSET, TABLE_COLUMNS_LIST_OFFSET_OFFSET + TABLE_COLUMNS_LIST_OFFSET_SIZE)
    }

    fn free_list_number_items_range() -> (usize, usize) {
        (FREE_LIST_NUMBER_ITEMS_OFFSET, FREE_LIST_NUMBER_ITEMS_OFFSET + FREE_LIST_NUMBER_ITEMS_SIZE)
    } 

    fn free_list_offset_range() -> (usize, usize) {
        (FREE_LIST_OFFSET_OFFSET, FREE_LIST_OFFSET_OFFSET + FREE_LIST_OFFSET_SIZE)
    }

    fn reclaim_list_number_items_range() -> (usize, usize) {
        (RECLAIM_LIST_NUMBER_ITEMS_OFFSET, RECLAIM_LIST_NUMBER_ITEMS_OFFSET + RECLAIM_LIST_NUMBER_ITEMS_SIZE)
    }

    fn reclaim_list_offset_range() -> (usize, usize) {
        (RECLAIM_LIST_OFFSET_OFFSET, RECLAIM_LIST_OFFSET_OFFSET + RECLAIM_LIST_OFFSET_SIZE)
    }
}

impl Deref for Master {
    type Target = Vec<u8>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Master {
    fn deref_mut(&mut self) -> &mut Self::Target {
            &mut self.0
    }
}