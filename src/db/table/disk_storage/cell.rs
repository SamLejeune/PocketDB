use std::num;
use std::{io::Read, ops::{Deref, DerefMut}, str};
use crate::db::shared::{constants::cell::{CELL_DATA_SIZE, CELL_DATA_SIZE_OFFSET, CELL_DATA_TYPE_OFFSET, CELL_DATA_TYPE_SIZE, CELL_IS_HEAD_OFFSET, CELL_IS_HEAD_SIZE, CELL_META_DATA_SIZE}, enums::DataType, utils::{self, bytes_to_u32}};

#[derive(Debug)]
pub enum CellType {
    Body = 0,
    Head = 1,
    Padding = 2,
}

#[derive(Debug)]
pub enum CellDataType {
    Meta = 0,
    Integer = 1,
    Text = 2,
    Bool = 3,
}

impl From<u8> for CellDataType {
    fn from(value: u8) -> Self {
        match value {
            0 => CellDataType::Meta,
            1 => CellDataType::Integer,
            2 => CellDataType::Text,
            3 => CellDataType::Bool,
            _ => panic!("Invalid value for CellDataType: {}", value),
        }
    }
}

impl From<DataType> for CellDataType {
    fn from(value: DataType) -> Self {
        match value {
            DataType::Integer(_) => CellDataType::Integer,
            DataType::Text(_) => CellDataType::Text,
            DataType::Meta => CellDataType::Meta,
            DataType::Bool(_) => CellDataType::Bool
        }
    }
}

#[derive(Debug)]
pub struct Cell(Vec<u8>);

impl Cell {
    pub fn new_head(size: usize) -> Cell {
        let is_head = CellType::Head as u8;
        let data_type = CellDataType::Meta as u8; 
        let data_size = ((CELL_DATA_SIZE + CELL_IS_HEAD_SIZE + CELL_DATA_TYPE_SIZE + 4) as u32).to_le_bytes();
        let data = data_size.to_vec()
            .into_iter()
            .chain([is_head])
            .chain([data_type])
            .chain((size as u32).to_le_bytes())
            .collect();

        Cell(data)
    }

    pub fn new_body(data: DataType) -> Cell {
        let bytes = data.as_bytes();
        let data_type = CellDataType::from(data);
        let is_body = CellType::Body as u8;
        let data_size = ((CELL_DATA_SIZE + CELL_IS_HEAD_SIZE + CELL_DATA_TYPE_SIZE + bytes.len()) as u32).to_le_bytes();
        let data = data_size.to_vec()
            .into_iter()
            .chain([is_body])
            .chain([data_type as u8])
            .chain(bytes.to_vec())
            .collect();

        Cell(data)
    }

    pub fn new_padding(size: usize) -> Cell {
        let is_padding = CellType::Padding as u8;
        let data_type = CellDataType::Meta as u8; 
        let data_size = (size as u32).to_le_bytes();
        let data = data_size.to_vec()
            .into_iter()
            .chain([is_padding])
            .chain([data_type])
            .chain(vec![0u8; (size as i32 - CELL_META_DATA_SIZE as i32).abs() as usize])
            .collect();

        Cell(data)
    }

    pub fn from_bytes(bytes: &[u8]) -> Cell {
        Cell(bytes.to_vec())
    }

    // pub fn from_split(&mut self, split_at: usize) -> Cell {
    //     let (data_start, data_end) = self.data_range();
    //     let pre_split_data_size = (*self)[data_start..data_end].len();

    //     self.set_is_split(CellIsSplit::True);
    //     self.set_size(pre_split_data_size - split_at);
        
    //     let split_data = (*self).split_off(split_at);

    //     Cell::new_body(&split_data)
    // }

    // pub fn from_bytes(bytes: &[u8]) -> Cell {
    //     let len = (bytes.len() as u32).to_le_bytes();
    //     let data = vec![]
    //         .into_iter()
    //         .chain(len)
    //         .chain(bytes.to_vec())
    //         .collect();

    //     Cell(data)
    // }

    pub fn size(&self) -> usize {
        (*self).len()
    }

    pub fn is_head(&self) -> CellType {
        let (start, end) = Cell::is_head_range();

        if utils::bytes_to_u32(&self[start..end]) == 1 {
            CellType::Head
        } else {
            CellType::Body
        }
    }

    // pub fn data_type(&self) -> DataType {
    //     let (start, end) = Cell::data_type_range();

    //     if self[start..end][0] == 1 {
    //         DataType::Integer()
    //     } else if self[start..end][0] == 2 {
    //         DataType::Text()
    //     } else {
    //         DataType::Meta
    //     }
    // }

    // pub fn data(&self) -> &[u8] {
    //     let (start, end) = self.data_range();

    //     &(*self)[start..end]
    // }

    pub fn to_typed_data(&self) -> DataType {
        let (data_type_start, data_type_end) = Cell::data_type_range();
        let (data_start, data_end) = self.data_range();

        let cell_data_type = CellDataType::from(self[data_type_start..data_type_end][0]);
        match cell_data_type {
            CellDataType::Integer => DataType::to_integer(&(*self)[data_start..data_end]),
            CellDataType::Text => DataType::to_text(&(*self)[data_start..data_end]),
            CellDataType::Bool => DataType::to_bool(&(*self)[data_start..data_end]),
            CellDataType::Meta => DataType::Meta
        }
    }

    pub fn set_size(&mut self, data_size: usize) {
        let (start, end) = Cell::data_size_range();
        (*self).splice(start..end, (data_size as u32).to_le_bytes());
    }

    pub fn data(&self) -> &[u8] {
        let (start, end) = self.data_range();
        
        &(*self)[start..end]
    }

    fn is_head_range() -> (usize, usize) {
        (CELL_IS_HEAD_OFFSET, CELL_IS_HEAD_OFFSET + CELL_IS_HEAD_SIZE)
    }

    fn data_type_range() -> (usize, usize) {
        (CELL_DATA_TYPE_OFFSET, CELL_DATA_TYPE_OFFSET + CELL_DATA_TYPE_SIZE)
    }

    fn data_size_range() -> (usize, usize) {
        (CELL_DATA_SIZE_OFFSET, CELL_DATA_SIZE_OFFSET + CELL_DATA_SIZE)
    }

    fn data_range(&self) -> (usize, usize) {
        let (size_start, size_end) = Cell::data_size_range();
        let data_size = utils::bytes_to_u32(&(*self)[size_start..size_end]) as usize;

        (CELL_DATA_TYPE_OFFSET + CELL_DATA_TYPE_SIZE, data_size)
    }   
}

impl Deref for Cell {
    type Target = Vec<u8>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Cell {
    fn deref_mut(&mut self) -> &mut Self::Target {
            &mut self.0
    }
}