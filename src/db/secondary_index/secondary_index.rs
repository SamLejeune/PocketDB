use crate::db::{b_tree::{btree::BTree, disk_storage::node::NodeIndexType}, file_stystem::pager::Pager};

use super::disk_storage::secondary_index_list::SecondaryIndexList;

#[derive(Debug)]
pub struct SecondaryIndex {
    secondary_index_trees: Vec<BTree>,
    secondary_index_list: SecondaryIndexList,
}

impl SecondaryIndex {
    pub fn new() -> SecondaryIndex {
        SecondaryIndex { secondary_index_trees: vec![], secondary_index_list: SecondaryIndexList::new() }
    }

    pub fn from_bytes(bytes: Vec<u8>, pager: &mut Pager) -> SecondaryIndex {
        let secondary_index_list = SecondaryIndexList::from_bytes(bytes);
        let mut secondary_index_trees = vec![];
        for i in 0..secondary_index_list.num_items() {
            if let Some((secondary_index_offset, indexed_column)) = secondary_index_list.item(i) {
                let secondary_index_tree = BTree::new(Some(secondary_index_offset), Some(NodeIndexType::Secondary), Some(indexed_column), pager);

                secondary_index_trees.push(secondary_index_tree);
            }
        }

        SecondaryIndex { secondary_index_trees, secondary_index_list }
    }

    pub fn add_secondary_index(&mut self, indexed_column: usize, pager: &mut Pager) {
        self.secondary_index_list.add_item(0, indexed_column as u32);

        let tree = BTree::new(None, Some(NodeIndexType::Secondary), Some(indexed_column), pager);
        self.secondary_index_trees.push(tree);
    }

    pub fn set_secondary_index_item_offset(&mut self, offset: u32, i: usize) {
        self.secondary_index_list.set_item_offset(offset, i);
    }

    pub fn secondary_index_tree(&mut self, i: usize) -> Option<&BTree> {
        self.secondary_index_trees.get(i)
    }

    pub fn secondary_index_tree_mut(&mut self, i: usize) -> Option<&mut BTree> {
        self.secondary_index_trees.get_mut(i)
    }

    pub fn secondary_index_trees(&self) -> &Vec<BTree> {
        &self.secondary_index_trees
    }

    pub fn secondary_index_trees_mut(&mut self) -> &mut Vec<BTree> {
        &mut self.secondary_index_trees
    }

    pub fn secondary_index_item(&self, i: usize) -> Option<(u32, usize)> {
        self.secondary_index_list.item(i)
    }   

    pub fn num_secondary_index_items(&self) -> usize {
        self.secondary_index_list.num_items()
    }

    pub fn secondary_index_data(&self) -> &[u8] {
        &self.secondary_index_list.data()
    }
}