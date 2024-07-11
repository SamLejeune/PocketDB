use std::io::Read;

use crate::db::shared::{constants::{cell::{CELL_DATA_SIZE, CELL_DATA_SIZE_OFFSET}, row::{ROW_DATA_SIZE, ROW_DATA_SIZE_OFFSET, ROW_HEAD_CELL_SIZE}}, enums::DataType, utils};

use super::cell::Cell;

#[derive(Debug)]
pub struct Row {
    cells: Vec<Cell>,
}

impl Row {
    pub fn from_cells(mut cells: Vec<Cell>, primary_key: u32) -> Row {
        cells.insert(0, Cell::new_body(DataType::Integer(primary_key)));

        let size = cells
            .iter()
            .fold(vec![], |mut data, cell| {
                data.extend((*cell).to_vec());
                data
            })
            .len()
        + ROW_HEAD_CELL_SIZE;
        
        // TODO: remove hard-coded 92
        let size_padding = 92 - (size % 92);
        if size_padding < 92 {
            cells.push(Cell::new_padding(size_padding));
        }

        Row { cells: vec![Cell::new_head(size + size_padding)]// 
                .into_iter()
                .chain(cells)
                .collect()
        }
    }

    pub fn from_bytes(bytes: Vec<u8>) -> Row {
        let head_size = utils::bytes_to_u32(&bytes[CELL_DATA_SIZE_OFFSET..CELL_DATA_SIZE_OFFSET + CELL_DATA_SIZE]) as usize;
        let head = Cell::from_bytes(&bytes[0..head_size]);

        let mut row = Row { cells: vec![head] };

        let mut i = head_size;
        while i < bytes.len() && i <= row.size() - head_size {
            let cell_size = utils::bytes_to_u32(&bytes[i..i + CELL_DATA_SIZE]) as usize;
            row.cells_mut().push(Cell::from_bytes(&bytes[i..i + cell_size]));
            i += cell_size;
        }

        row 
    }

    // pub fn from_split(&mut self, split_at: usize) -> Row {
    //     let split_cells = self.cells.split_off(split_at);
    //     let split_row_size = Row::calc_size(&split_cells);

    //     self.set_size(Row::calc_size(&self.cells));

    //     Row { cells: split_cells }
    // }

    // pub fn from_cell_split(&mut self, split_row_at: usize, split_cell_at: usize) -> Row {
    //     let split_cells = self.cells.split_off(split_row_at + 1);
         
    //     if let Some(mut cell) = self.cells.pop() {
    //         let split_cell = cell.from_split(split_cell_at);
    //         let split_cells = vec![split_cell]
    //             .into_iter()
    //             .chain(split_cells)
    //             .collect();

    //         self.cells.push(cell);
    //         self.set_size(Row::calc_size(&self.cells));

    //         Row::from_cells(split_cells)
    //     } else {
    //         Row::from_cells(split_cells)
    //     }
    // }

    // pub fn from_cells(cells: Vec<Cell>) -> Row {
    //     let mut row = Row::new();
    //     row.set_size(Row::calc_size(&cells));
    //     row.cells.extend(cells);

    //     row
    // }
    
    pub fn append_cell(&mut self, cell: Cell) {
        self.increment_size((*cell).len());
        self.cells.push(cell);
    }

    pub fn size(&self) -> usize {
        let (size_start, size_end) = Row::size_range();

        if let Some(head) = self.head() {
            return utils::bytes_to_u32(&(*head)[size_start..size_end]) as usize
        }  

        0 
    }

    pub fn primary_key_bytes(&self) -> &[u8] {
        if let Some(primary_key_cell) = self.cells.get(1) {
            return primary_key_cell.data();
        }

        &[0]
    }

    pub fn cell_data(&self, i: usize) -> &[u8] {
        if let Some(cell) = self.cells.get(i) {
            return cell.data()
        }

        &[0]
    }

    pub fn cells(&self) -> &Vec<Cell> {
        &self.cells
    }

    pub fn cells_mut(&mut self) -> &mut Vec<Cell> {
        &mut self.cells
    }

    pub fn cell_mut(&mut self, at: usize) -> Option<&mut Cell> {
        self.cells.get_mut(at)
    }

    pub fn num_cells(&self) -> usize {
        self.cells.len()
    }

    pub fn data(&self) -> Vec<u8> {
        self.cells()
            .iter()
            .fold(vec![], |mut data, cell| {
                data.extend((*cell).to_vec());
                data
            })
    }

    pub fn set_size(&mut self, size: usize) {
        let (size_start, size_end) = Row::size_range();

        if let Some(head) = self.head_mut() {
            (*head).splice(size_start..size_end + ROW_DATA_SIZE, size.to_le_bytes());
        }  
    }

    pub fn increment_size(&mut self, add_size: usize) {
        let (size_start, size_end) = Row::size_range();

        if let Some(head) = self.head_mut() {
            let size = utils::bytes_to_u32(&(*head)[size_start..size_end]) + add_size as u32;
            (*head).splice(size_start..size_end, size.to_le_bytes());
        }
    }

    fn head(&self) -> Option<&Cell> {
        self.cells.first()
    }

    fn head_mut(&mut self) -> Option<&mut Cell> {
        self.cells.first_mut()
    }

    fn size_range() -> (usize, usize) {
        (ROW_DATA_SIZE_OFFSET, ROW_DATA_SIZE_OFFSET + ROW_DATA_SIZE)
    }

    fn calc_size(cells: &Vec<Cell>) -> usize {
        cells.iter()
            .map(|cell| (*cell).len())
            .sum()
    }
}