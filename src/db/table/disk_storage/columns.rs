
use std::{io::Read, ops::{Deref, DerefMut}, str};

use crate::db::shared::{constants::{column::{COLUMN_META_DATA_SIZE, COLUMN_NAME_SIZE_OFFSET, COLUMN_NAME_SIZE_SIZE, COLUMN_TYPE_OFFSET, COLUMN_TYPE_SIZE}, columns::{self, COLUMNS_META_DATA_SIZE, COLUMNS_NUMBER_COLUMNS_OFFSET, COLUMNS_NUMBER_COLUMNS_SIZE, COLUMNS_SIZE_OFFSET, COLUMNS_SIZE_SIZE}}, enums::ColumnType, utils::bytes_to_u32};

#[derive(Debug)]
pub struct Columns(Vec<u8>);

impl Columns {
    pub fn new() -> Columns {
        let bytes = vec![]
            .into_iter()
            .chain((COLUMNS_META_DATA_SIZE as u32).to_le_bytes())        
            .chain([0; COLUMNS_NUMBER_COLUMNS_SIZE])
            .collect();

        let mut columns = Columns(bytes);
        columns.add_column("head", ColumnType::Meta);
        columns.add_column("key", ColumnType::Integer);

       columns
    }

    pub fn from_bytes(mut bytes: Vec<u8>) -> Columns {
        let (start, end) = Columns::columns_size_range();
        let size = bytes_to_u32(&(&bytes)[start..end]) as usize;
        bytes.drain(size..);

        Columns(bytes)
    }

    pub fn add_column(&mut self, column_name: &str, column_type: ColumnType) -> &mut Self {
        let column_name_bytes = column_name.as_bytes();
        let size = column_name_bytes.len();

        let bytes: Vec<u8> = vec![]
            .into_iter()
            .chain((size as u32).to_le_bytes())
            .chain((column_type as u32).to_le_bytes())
            .chain(column_name_bytes.to_vec())
            .collect();
        (*self).extend(bytes);

        self.increment_columns_size(size + COLUMN_META_DATA_SIZE);
        self.increment_number_columns();

        self
    }

    pub fn column(&self, i: usize) -> (String, ColumnType) {
        let (start, end) = Columns::number_columns_range();
        let num_columns = bytes_to_u32(&(*self)[start..end]) as usize;
        if i > num_columns {
            return (String::from(""), ColumnType::from_u32(0));
        }

        let mut start = COLUMNS_META_DATA_SIZE;
        for _ in 0..i {
            let (column_name_size_start, column_name_size_end) = Columns::column_name_size_range();
            let column_name_size_start = start + column_name_size_start;
            let column_name_size_end = column_name_size_start + column_name_size_end;

            start += COLUMN_META_DATA_SIZE + bytes_to_u32(&(*self)[column_name_size_start..column_name_size_end]) as usize;
        }

        let (column_name_size_start, column_name_size_end) = Columns::column_name_size_range();
        let column_name_size_start = start + column_name_size_start;
        let column_name_size_end = column_name_size_start + column_name_size_end;

        let (column_type_start, column_type_end) = Columns::column_type_range();
        let column_name_start = start + COLUMNS_META_DATA_SIZE;
        let column_name_end = column_name_start + bytes_to_u32(&(*self)[column_name_size_start..column_name_size_end]) as usize;

        let column_name = if let Ok(column_name) = str::from_utf8(&(*self)[column_name_start..column_name_end]) {
            column_name
        } else {
            ""
        };
        let column_type = &(*self)[start + column_type_start..start + column_type_end];

        (column_name.to_string(), ColumnType::from_bytes(column_type))
    }

    pub fn num_columns(&self) -> usize {
        let (start, end) = Columns::number_columns_range();

        bytes_to_u32(&(*self)[start..end]) as usize
    }

    pub fn columns_len(&self) -> usize {
        let (start, end) = Columns::columns_size_range();

        bytes_to_u32(&(*self)[start..end]) as usize
    }

    pub fn data(&self) -> &[u8] {
       &(*self)
    }

    fn increment_columns_size(&mut self, size: usize) {
        let (start, end) = Columns::column_name_size_range();
        let mut columns_size = bytes_to_u32(&(*self)[start..end]) as usize;
        columns_size += size;
        
        (*self).splice(start..end, (columns_size as u32).to_le_bytes());
    }

    fn increment_number_columns(&mut self) {
        let (start, end) = Columns::number_columns_range();
        let mut number_columns = bytes_to_u32(&(*self)[start..end]);
        number_columns += 1;

        (*self).splice(start..end, number_columns.to_le_bytes());
    }   

    fn columns_size_range() -> (usize, usize) {
        (COLUMNS_SIZE_OFFSET, COLUMNS_SIZE_OFFSET + COLUMNS_SIZE_SIZE)
    }

    fn number_columns_range() -> (usize, usize) {
        (COLUMNS_NUMBER_COLUMNS_OFFSET, COLUMNS_NUMBER_COLUMNS_OFFSET + COLUMNS_NUMBER_COLUMNS_SIZE)
    }

    fn column_name_size_range() -> (usize, usize) {
        (COLUMN_NAME_SIZE_OFFSET, COLUMN_NAME_SIZE_OFFSET + COLUMN_NAME_SIZE_SIZE)
    }

    fn column_type_range() -> (usize, usize) {
        (COLUMN_TYPE_OFFSET, COLUMN_TYPE_OFFSET + COLUMN_TYPE_SIZE)
    }
}

impl Deref for Columns {
    type Target = Vec<u8>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Columns {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}