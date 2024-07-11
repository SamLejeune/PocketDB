use std::str;
use super::utils::bytes_to_u32;

#[derive(Debug)]
pub enum ColumnType {
    Meta = 0,
    Integer = 1,
    Text = 2,
    Bool = 3,
}

impl ColumnType {
   pub fn matches(&self, data_type: &DataType) -> bool {
        match (self, data_type) {
            (ColumnType::Meta, DataType::Meta) => true,
            (ColumnType::Text, DataType::Text(_)) => true,
            (ColumnType::Integer, DataType::Integer(_)) => true,
            (ColumnType::Bool, DataType::Bool(_)) => true,
            (_, _) => false
        }
   }

   pub fn from_bytes(column_type: &[u8]) -> ColumnType {
        match column_type[0] {
            1 => ColumnType::Integer,
            2 => ColumnType::Text,
            3 => ColumnType::Bool,
            _ => ColumnType::Meta
        }
   }

   pub fn from_u32(column_type: u32) -> ColumnType {
        match column_type {
            1 => ColumnType::Integer,
            2 => ColumnType::Text,
            3 => ColumnType::Bool,
            _ => ColumnType::Meta
        }
   }
}

#[derive(Debug)]
pub enum DataType {
    Meta,
    Integer(u32),
    Text(String),
    Bool(bool),
}

impl DataType {
    pub fn to_integer(bytes: &[u8]) -> DataType {
        DataType::Integer(bytes_to_u32(bytes))
    }

    pub fn to_text(bytes: &[u8]) -> DataType {
        let data = if let Ok(data) = str::from_utf8(bytes) {
            data
        } else {
            ""
        };
        
        DataType::Text(data.to_string())
    }

    pub fn to_bool(bytes: &[u8]) -> DataType {
        if bytes[0] == 1 {
            DataType::Bool(true)
        } else {
            DataType::Bool(false)
        }
    }

    pub fn as_bytes(&self) -> Vec<u8> {
        match self {
            DataType::Integer(int) => int.to_le_bytes().to_vec(),
            DataType::Text(s) => s.as_bytes().to_vec(),
            DataType::Bool(b) => if *b { vec![1] } else { vec![0] },
            DataType::Meta => vec![0]
        }
    }
}