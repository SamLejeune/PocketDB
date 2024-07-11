use rand::{rngs::ThreadRng, seq::index, Rng};

use super::{b_tree::{btree::BTree, disk_storage::node::NodeIndexType}, file_stystem::pager::Pager, secondary_index::secondary_index::SecondaryIndex, shared::enums::{ColumnType, DataType}, table::{disk_storage::{cell::Cell, row::Row}, table::Table}};

#[derive(Debug)]
pub struct PocketDB {
    primary_index_tree: BTree,
    secondary_indexes: SecondaryIndex,
	table: Table,
	pager: Pager,
    rng: ThreadRng
}

impl PocketDB {
	pub fn new() -> PocketDB {
		let mut pager = Pager::new();

		PocketDB { 
            primary_index_tree: PocketDB::new_primary_index_tree(&mut pager), 
            secondary_indexes: PocketDB::new_secondary_indexes(&mut pager),
            table: PocketDB::new_table(&mut pager),
            pager,
            rng: rand::thread_rng()
        }
	}

    pub fn add_column(&mut self, column_name: &str, column_type: ColumnType) -> &mut Self { 
        self.table.add_column(column_name, column_type);
        self.flush_table();

        self
    }

    pub fn add_indexed_column(&mut self, column_name: &str, column_type: ColumnType) -> &mut Self {
        self.add_column(column_name, column_type);
        
        let indexed_column = self.table.num_columns() - 1;
        self.secondary_indexes.add_secondary_index(indexed_column, &mut self.pager);

        self
    }

	pub fn insert(&mut self, key: u32, mut row: Row) -> Result<(), String> {
        let num_cols = self.table.num_columns();
        let num_cells = row.num_cells();

        if num_cols != num_cells - 1 {
            return Err(format!("Expected row length {} but received length {}", num_cells - 1, num_cells));
        }

        let type_matches = row.cells()
            .iter()
            .enumerate()
            .all(|(i, cell)| {
                if i >= num_cols { return true; }

                let (_, column_type) = self.table.column(i);
                column_type.matches(&cell.to_typed_data())
            });

        if !type_matches {
            return Err(format!("Invalid row data types provided"));
        }

        let row_size = row.data().len();
        let row_offset = self.pager.add_to_write_buffer(&row.data(), None);
		self.table.insert_row(row_offset, row);

        // let primary_key = self.rng.gen_range(1..=1000) as u32;
		self.primary_index_tree.insert(key, (row_offset, row_size), &mut self.pager, &mut self.table);
        for secondary_tree in self.secondary_indexes.secondary_index_trees_mut().iter_mut() {
            secondary_tree.insert(row_offset, (row_offset, row_size), &mut self.pager, &mut self.table);
        }
        
        self.flush_trees();
       
        Ok(())
	}

    pub fn search_by_primary_index(&mut self, key: DataType) -> Option<Vec<Vec<DataType>>> {        
		if let Some (rows) = self.primary_index_tree.search(key.as_bytes(), &mut self.pager, &mut self.table) {
            let data: Vec<Vec<DataType>> = rows.iter()
                .map(|r| r.cells()
                    .iter()
                    .map(|c| c.to_typed_data())
                    .collect()
                ).collect();

			return Some(data);
		}
        
        None
	}

    pub fn search_by_secondary_index(&mut self, key: DataType, column_name: &str) -> Option<Vec<Vec<DataType>>> {
        let indexed_column = self.indexed_column_from_column_name(column_name);
        let secondary_tree = self.secondary_indexes
            .secondary_index_trees_mut()
            .iter_mut()
            .find(|tree| {
                if let Some(i_col) = tree.indexed_column() {
                    if indexed_column == i_col {
                        return true;
                    }
                }
                false
            });

        if let Some(secondary_tree) = secondary_tree {
            if let Some (rows) = secondary_tree.search(key.as_bytes(), &mut self.pager, &mut self.table) {
                let data: Vec<Vec<DataType>> = rows.iter()
                    .map(|r| r.cells()
                        .iter()
                        .map(|c| c.to_typed_data())
                        .collect()
                    ).collect();
    
                return Some(data);
            }
        }

        None
	}

	pub fn delete_by_primary_index(&mut self, key: DataType) {
		let deleted_row = self.primary_index_tree.delete(key.as_bytes(), &mut self.pager, &mut self.table);

        if let Some(deleted_row) = deleted_row {
            for secondary_tree in self.secondary_indexes.secondary_index_trees_mut().iter_mut() {
                if let Some(indexed_column) = secondary_tree.indexed_column() {
                    let key = deleted_row[0].cell_data(indexed_column).to_vec();
                    secondary_tree.delete(key, &mut self.pager, &mut self.table);
                }
            }
        }

        self.flush_trees();
	}

    pub fn delete_by_secondary_index(&mut self, key: DataType, column_name: &str) {
        let indexed_column = self.indexed_column_from_column_name(column_name);
        let secondary_tree = self.secondary_indexes
            .secondary_index_trees_mut()
            .iter_mut()
            .find(|tree| {
                if let Some(i_col) = tree.indexed_column() {
                    if indexed_column == i_col {
                        return true;
                    }
                }
                false
            });

        if let Some(secondary_tree) = secondary_tree {
            let deleted_rows = secondary_tree.delete(key.as_bytes(), &mut self.pager, &mut self.table);
            if let Some(deleted_rows) = deleted_rows {
                for row in &deleted_rows {
                    let primary_key = row.primary_key_bytes().to_vec();
                    self.primary_index_tree.delete(primary_key, &mut self.pager, &mut self.table);

                    for secondary_tree in self.secondary_indexes.secondary_index_trees_mut().iter_mut() {
                        if let Some(i_col) = secondary_tree.indexed_column() {
                            if i_col == indexed_column { continue; }

                            let key = row.cell_data(i_col).to_vec();
                            secondary_tree.delete(key, &mut self.pager, &mut self.table);
                        }
                    }
                }
            }
        }

        self.flush_trees();
    }

    fn indexed_column_from_column_name(&self, column_name: &str) -> usize {
        let mut indexed_column = 0;
        for i in 0..self.table.num_columns() {
            let (name, _) = self.table.column(i);
            if column_name == name {
                indexed_column = i;
                break;
            }
        }

        indexed_column
    }

    fn flush_table(&mut self) {
        let table_columns_size = self.table.columns_data().len();
        let table_columns_offset = self.pager.add_to_write_buffer(
            self.table.columns_data(), 
            self.pager.table_columns_offset_table_columns_size()
        );

        self.pager.flush_table_columns((table_columns_offset, table_columns_size));
    }

    fn flush_trees(&mut self) {
        let (primary_root_offset, primary_root_size) = if let Some (primary_root) = self.primary_index_tree.root() {
            let prev_root_offset_and_size = if let Some(primary_root) = self.pager.primary_root_offset_primary_root_size() {
                let (primary_root_offset, primary_root_size) = primary_root;
                Some((primary_root_offset, primary_root_size))
            } else {
                None
            };

            (self.pager.add_to_write_buffer(primary_root.data(), prev_root_offset_and_size), primary_root.data().len())
        } else {
            (0, 0)
        };

        let secondary_index_offsets: Vec<u32> = self.secondary_indexes
            .secondary_index_trees()
            .iter()
            .enumerate()
            .filter_map(|(i, secondary_tree)|  {
                if let Some(secondary_tree_root) = secondary_tree.root() {
                    let data = secondary_tree_root.data();
                    let prev_offset_prev_size = self.secondary_indexes.secondary_index_item(i);

                    return Some(self.pager.add_to_write_buffer(data, prev_offset_prev_size));
                }
                None
            })
            .collect();
        
        for (i, offset) in secondary_index_offsets.iter().enumerate() {
            self.secondary_indexes.set_secondary_index_item_offset(*offset, i);
        }

        let secondary_index_list_size = self.secondary_indexes.secondary_index_data().len();
        let secondary_index_list_offset = self.pager.add_to_write_buffer(
            self.secondary_indexes.secondary_index_data(), 
            self.pager.secondary_index_offset_secondary_index_size()
        );

        self.pager.flush_write_buffer_trees(
            (primary_root_offset, primary_root_size), 
            (secondary_index_list_offset, secondary_index_list_size)
        ); 
    }

    fn new_primary_index_tree(pager: &mut Pager) -> BTree {
        if let Some(root) = pager.primary_root_offset_primary_root_size() {
            let (primary_root_offset, _) = root;
            return BTree::new(Some(primary_root_offset), None, None, pager);
        }
        
        BTree::new(None, Some(NodeIndexType::Primary), None, pager)
    }

    fn new_secondary_indexes(pager: &mut Pager) -> SecondaryIndex {
        if let Some(secondary_index_list) = pager.secondary_index_offset_secondary_index_size() {
            let (secondary_index_list_offset, secondary_index_list_size) = secondary_index_list;
            if let Some(bytes) = pager.read_from_file(secondary_index_list_offset as usize, secondary_index_list_size) {
                return SecondaryIndex::from_bytes(bytes, pager);
            }
        }

        SecondaryIndex::new()
    }

    fn new_table(pager: &mut Pager) -> Table {
        if let Some(table) = pager.table_columns_offset_table_columns_size() {
            let (table_columns_offset, table_columns_size) = table;
            if let Some(bytes) = pager.read_from_file(table_columns_offset as usize, table_columns_size) {
                return Table::from_bytes(bytes);
            }
        }

        Table::new()
    }

    pub fn row(key: u32, cells: Vec<Cell>) -> Row {
        Row::from_cells(cells, key)
    }

    pub fn cell(data: DataType) -> Cell {
        Cell::new_body(data)
    }
}