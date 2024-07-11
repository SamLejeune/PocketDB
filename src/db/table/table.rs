use std::{collections::HashMap, ops::Deref};

use crate::db::shared::enums::ColumnType;

use super::disk_storage::{columns::Columns, row::Row};

// #[derive(Debug)]
// pub struct Column((String, ColumnType));

// impl Column {
//     pub fn new(name: String, column_type: ColumnType) -> Column {
//         Column((name, column_type))
//     }

//     pub fn name(&self) -> &str {
//         let Column((name, _)) = &(*self);

//         name
//     }

//     pub fn column_type(&self) -> &ColumnType {
//         let Column((_, column_type)) = &(*self);

//         column_type
//     }
// }

// impl Deref for Column {
//     type Target = (String, ColumnType);

//     fn deref(&self) -> &Self::Target {
//         &self.0
//     }
// }

#[derive(Debug)]
pub struct Table {
  table: HashMap<u32, Row>,
  columns: Columns
}

impl Table {
    pub fn new() -> Table {
        Table { table: HashMap::new(), columns: Columns::new() }
    }

    pub fn from_bytes(bytes: Vec<u8>) -> Table {
        let columns = Columns::from_bytes(bytes);

        Table { table: HashMap::new(), columns }
    }

    pub fn add_column(&mut self, column_name: &str, column_type: ColumnType) {
        self.columns.add_column(column_name, column_type);
    //     if !self.columns
    //         .iter()
    //         .any(|Column((name, _))| name == column.name()) 
    //     {
    //         self.columns.push(column);
    //     }
    // } 
    }

    pub fn insert_row(&mut self, row_offset: u32, row: Row) {
        self.table.insert(row_offset, row);
    }

    pub fn delete_row(&mut self, row_offset: u32) -> Option<Row> {
        self.table.remove(&row_offset)
    }

    pub fn row(&self, row_offset: u32) -> Option<&Row> {
        self.table.get(&row_offset)
    }

    pub fn row_mut(&mut self, row_offset: u32) -> Option<&mut Row> {
        self.table.get_mut(&row_offset)
    }

    pub fn column(&self, i: usize) -> (String, ColumnType) {
        self.columns.column(i)
    }

    pub fn num_columns(&self) -> usize {
        self.columns.num_columns()
    }

    pub fn columns_len(&self) -> usize {
        self.columns.columns_len()
    }

    pub fn columns_data(&self) -> &[u8] {
        &self.columns.data()
    }
}

