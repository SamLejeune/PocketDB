use std::cmp::{max, min};

use crate::db::{b_tree::tree_node::TreeNode, file_stystem::pager::Pager, shared::{constants::{node::NODE_SIZE, node_child::{NODE_MAX_CHILDREN, NODE_MIN_CHILDREN}, node_key::{NODE_MAX_KEYS, NODE_MIN_KEYS}}, enums::DataType}, table::{disk_storage::row::Row, table::Table, disk_storage}};

use super::disk_storage::{node::{Node, NodeIndexType, NodeType}, node_overflow::NodeOverflow};

#[derive(Debug)]
pub struct BTree {
    root: Option<TreeNode>,
    index_type: NodeIndexType,
    indexed_column: Option<usize>,
}

impl BTree {
    pub fn new(root_offset: Option<u32>, index_type: Option<NodeIndexType>, indexed_column: Option<usize>, pager: &mut Pager) -> BTree {
        if let Some(root_offset) = root_offset {
            if let Some(bytes) = pager.read_from_file(root_offset as usize, NODE_SIZE) {
                let root = TreeNode::from_bytes(bytes);
                return BTree { index_type: root.node_index_type(), indexed_column: root.node_indexed_column(), root: Some(root) };
            }
        }

        if let Some(index_type) = index_type {
            return BTree { root: None, index_type, indexed_column };
        }

        BTree { root: None, index_type: NodeIndexType::Primary, indexed_column: None }
    }

    pub fn insert(&mut self, key: u32, row_meta_data: (u32, usize), pager: &mut Pager, table: &mut Table) {  
        if let Some(root) = self.root.take() {
            let (node , _) = BTree::insert_node(root, key, false, row_meta_data, pager, table);

            self.root = Some(node);
        } else {
            let (row_offset, row_size) = row_meta_data;
            let mut root = TreeNode::new(true, NodeType::Leaf, self.index_type, self.indexed_column, key, row_size);
            root.add_node_child(row_offset, row_size, false);

            self.root = Some(root);
        }        
    }

    fn insert_node(node: TreeNode, key: u32, duplicate_key: bool, row_meta_data: (u32, usize), pager: &mut Pager, table: &mut Table) -> (TreeNode, Option<TreeNode>) {
        let key_value = BTree::key_value_from_table(row_meta_data, node.node_indexed_column(), pager, table);
        let (i, dup_key) = BTree::key_index_from_node(&node, &key_value, pager, table);
        let duplicate_key = duplicate_key || dup_key;

        match node.node_type() {
            NodeType::Internal => BTree::insert_internal(node, i, key, duplicate_key, row_meta_data, pager, table),
            NodeType::Leaf => BTree::insert_leaf(node, i, key, duplicate_key, row_meta_data, pager, table),
        }
    }

    fn insert_internal(mut node: TreeNode, i: usize, key: u32, duplicate_key: bool, row_meta_data: (u32, usize), pager: &mut Pager, table: &mut Table) -> (TreeNode, Option<TreeNode>) {
        let num_keys_pre_split = node.keys_len(); 
        let i = if i < node.children_len() || i <= 0 { i } else { node.children_len() - 1 };

        if let None = node.cached_tree_node_child(i) {
            if let Some((child_offset, child_size, _)) = node.child(i) {
                if let Some(bytes) = pager.read_from_file(child_offset as usize, child_size) {
                    node.cache_tree_node_child(TreeNode::from_bytes(bytes), i);
                }
            }
        }

        let (mut left_child, right_child) = BTree::insert_node(
            node.take_cached_tree_node_child(i).unwrap(), key, duplicate_key, row_meta_data, pager, table
        );

        if let Some(mut right_child) = right_child {
            let (promote_key, remote_key_size) = if left_child.keys_len() > right_child.keys_len() {
                (left_child.take_key(left_child.keys_len() - 1))
            } else {
                (right_child.take_key(0))
            }; 

            let left_child_offset = pager.add_to_write_buffer(left_child.data(), node.child_offset_child_size(i));
            let right_child_offset = pager.add_to_write_buffer(right_child.data(), None);

            node.replace_node_child(left_child_offset, left_child.size(), false, i);
            node.cache_tree_node_child(left_child, i);
            
            if node.keys_len() + 1 > NODE_MAX_KEYS {
                if node.is_root() {
                    let (mut left_node, mut right_node) = BTree::insert_balance_root_internal(&mut node, i, promote_key, duplicate_key, row_meta_data, pager, table);

                    if left_node.children_len() < right_node.children_len() {
                        let key_value = BTree::key_value_from_table(row_meta_data, left_node.node_indexed_column(), pager, table);
                        let (splice_at, _) = BTree::key_index_from_node(&left_node, &key_value, pager, table);

                        left_node.splice_tree_node_child(right_child, right_child_offset, false, splice_at);
                    } else {
                        let key_value = BTree::key_value_from_table(row_meta_data, right_node.node_indexed_column(), pager, table);
                        let (splice_at, _) = BTree::key_index_from_node(&right_node, &key_value, pager, table);

                        right_node.splice_tree_node_child(right_child, right_child_offset, false, min(splice_at, right_node.children_len()));
                    }

                    let left_child_offset = pager.add_to_write_buffer(left_node.data(), None);
                    let right_child_offset = pager.add_to_write_buffer(right_node.data(), node.child_offset_child_size(i));

                    node.add_tree_node_child(left_node, left_child_offset, false);
                    node.add_tree_node_child(right_node, right_child_offset, false);
        
                    return (node, None);
                } else { 
                    let (mut left_node, mut right_node) = BTree::insert_balance_internal(node, promote_key, duplicate_key, row_meta_data, pager, table);

                    if left_node.children_len() < right_node.children_len() {
                        let key_value = BTree::key_value_from_table(row_meta_data, left_node.node_indexed_column(), pager, table);
                        let (splice_at, _) = BTree::key_index_from_node(&left_node, &key_value, pager, table);

                        left_node.splice_tree_node_child(right_child, right_child_offset, false, splice_at);
                    } else {
                        let key_value = BTree::key_value_from_table(row_meta_data, right_node.node_indexed_column(), pager, table);
                        let (splice_at, _) = BTree::key_index_from_node(&right_node, &key_value, pager, table);

                        right_node.splice_tree_node_child(right_child, right_child_offset, false, min(splice_at, right_node.children_len()));
                    }
    
                    return (left_node, Some(right_node));
                }
            } else {
                let i = if let None = node.node_indexed_column() {
                    i
                } else {
                    let key_value = BTree::key_value_from_table((promote_key, remote_key_size), node.node_indexed_column(), pager, table);
                    let (i, _) = BTree::key_index_from_node(&node, &key_value, pager, table);
                    i
                };

                node.splice_key(promote_key, remote_key_size, i);
                node.splice_tree_node_child(right_child, right_child_offset, false, i + 1); 

                return (node, None);
            }
        } else {
            let offset = pager.add_to_write_buffer(&left_child.data(), node.child_offset_child_size(i));
            node.replace_node_child(offset, left_child.size(), false, i);
            node.cache_tree_node_child(left_child, i);

            return (node, None);
        }
    }

    fn insert_leaf(mut node: TreeNode, i: usize, key: u32, duplicate_key: bool, row_meta_data: (u32, usize), pager: &mut Pager, table: &mut Table) -> (TreeNode, Option<TreeNode>) {
        if node.keys_len() + 1 > NODE_MAX_KEYS && !duplicate_key {
            if node.is_root() {
                let (left_node, right_node) = BTree::insert_balance_root_leaf(&mut node, i, key, duplicate_key, row_meta_data, pager, table);

                let left_child_offset = pager.add_to_write_buffer(left_node.data(), None);
                let right_child_offset = pager.add_to_write_buffer(right_node.data(), None);

                node.add_tree_node_child(left_node, left_child_offset, false);
                node.add_tree_node_child(right_node, right_child_offset, false);

                return (node, None); 
            } else {
                let (left_node, right_node) = BTree::insert_balance_leaf(node, i, key, duplicate_key, row_meta_data, pager, table);

                return (left_node, Some(right_node));
            }
        } else {
            let key_value = BTree::key_value_from_table(row_meta_data, node.node_indexed_column(), pager, table);
            let (splice_at, dup_key) = BTree::key_index_from_node(&node, &key_value, pager, table);
            BTree::insert_row_to_leaf(&mut node, splice_at, key, duplicate_key || dup_key, row_meta_data, pager, table);

            return (node, None);
        }
    }

    fn insert_balance_internal(node: TreeNode, key: u32, duplicate_key: bool, row_meta_data: (u32, usize), pager: &mut Pager, table: &mut Table) -> (TreeNode, TreeNode) {
        let (_, row_size) = row_meta_data;
        let (split_keys_at, split_children_at) = (NODE_MIN_KEYS, NODE_MIN_CHILDREN);

        let (mut left_node, mut right_node) = node.take_node_and_split(split_keys_at, split_children_at);
        left_node.set_node_type(NodeType::Internal);
        right_node.set_node_type(NodeType::Internal);

        let key_value = BTree::key_value_from_table(row_meta_data, right_node.node_indexed_column(), pager, table);
        let node_key_value = BTree::key_value_from_node(&right_node, 0, pager, table);

        if key_value.cmp(&node_key_value).is_gt()  {
            let (splice_at, dup_key) = BTree::key_index_from_node(&right_node, &key_value, pager, table);
            right_node.splice_key(key, row_size, splice_at);
        } else {
            let (splice_at, dup_key) = BTree::key_index_from_node(&left_node, &key_value, pager, table);
            left_node.splice_key(key, row_size, splice_at);
        }

        (left_node, right_node)
    }

    fn insert_balance_leaf(node: TreeNode, i: usize, key: u32, duplicate_key: bool, row_meta_data: (u32, usize), pager: &mut Pager, table: &mut Table) -> (TreeNode, TreeNode) {
        let (split_keys_at, split_children_at) = (
            if i <= NODE_MIN_KEYS { NODE_MIN_KEYS } else { NODE_MIN_KEYS + 1 },
            if i <= NODE_MIN_KEYS { NODE_MIN_CHILDREN - 1  } else { NODE_MIN_CHILDREN },
        );

        let (mut left_node, mut right_node) = node.take_node_and_split(split_keys_at, split_children_at);
        left_node.set_node_type(NodeType::Leaf);
        right_node.set_node_type(NodeType::Leaf);

        let key_value = BTree::key_value_from_table(row_meta_data, right_node.node_indexed_column(), pager, table);
        let node_key_value = BTree::key_value_from_node(&right_node, 0, pager, table);

        if key_value.cmp(&node_key_value).is_gt() || right_node.keys_len() < NODE_MIN_KEYS {
            let (splice_at, dup_key) = BTree::key_index_from_node(&right_node, &key_value, pager, table);
            BTree::insert_row_to_leaf(&mut right_node, splice_at, key, duplicate_key || dup_key, row_meta_data, pager, table);
        } else {
            let (splice_at, dup_key) = BTree::key_index_from_node(&left_node, &key_value, pager, table);
            BTree::insert_row_to_leaf(&mut left_node, splice_at, key, duplicate_key || dup_key, row_meta_data, pager, table);
        }

        (left_node, right_node)
    }

    fn insert_balance_root_internal(node: &mut TreeNode, i: usize, key: u32, duplicate_key: bool, row_meta_data: (u32, usize), pager: &mut Pager, table: &mut Table) -> (TreeNode, TreeNode) {
        let is_mid = i == node.keys_len() / 2;
        let (_, row_size) = row_meta_data;

        if is_mid {    
            let (left_node, right_node) = node.split_node_at_midpoint();
            node.append_key(key, row_size);

            return (left_node, right_node);
        } else {
            let (mut left_node, mut right_node) = node.split_off_left_node_right_node();

            let key_value = BTree::key_value_from_table(row_meta_data, left_node.node_indexed_column(), pager, table);
            let node_key_value = BTree::key_value_from_node(&left_node, left_node.keys_len() - 1, pager, table);

            if key_value.cmp(&node_key_value).is_lt() {
                let (splice_at, dup_key) = BTree::key_index_from_node(&left_node, &key_value, pager, table);
                left_node.splice_key(key, row_size, splice_at);
            } else {
                let (splice_at, dup_key) = BTree::key_index_from_node(&right_node, &key_value, pager, table);
                right_node.splice_key(key, row_size, splice_at);
            }

            return (left_node, right_node);
        }
    }

    fn insert_balance_root_leaf(node: &mut TreeNode, i: usize, key: u32, duplicate_key: bool, row_meta_data: (u32, usize), pager: &mut Pager, table: &mut Table) -> (TreeNode, TreeNode) {
        let is_mid = i == node.keys_len() / 2;
        let (_, row_size) = row_meta_data;

        node.set_node_type(NodeType::Internal);

        if is_mid {
            let (mut left_node, mut right_node) = node.split_node_at_midpoint();
            left_node.set_node_type(NodeType::Leaf);
            right_node.set_node_type(NodeType::Leaf);

            node.append_key(key, row_size);
     
            let key_value = BTree::key_value_from_table(row_meta_data, left_node.node_indexed_column(), pager, table);
            let (splice_at, dup_key) = BTree::key_index_from_node(&left_node, &key_value, pager, table);
            BTree::insert_row_to_leaf(&mut left_node, splice_at, key, duplicate_key || dup_key, row_meta_data, pager, table);

            return (left_node, right_node);
        } else {
            let (mut left_node, mut right_node) = node.split_off_left_node_right_node();
            left_node.set_node_type(NodeType::Leaf);
            right_node.set_node_type(NodeType::Leaf);

            let key_value = BTree::key_value_from_table(row_meta_data, left_node.node_indexed_column(), pager, table);
            let node_key_value = BTree::key_value_from_node(&left_node, left_node.keys_len() - 1, pager, table);

            if key_value.cmp(&node_key_value).is_lt() {
                // TODO: can I remove key_index_from_node cause I get this from the calling function?
                let (splice_at, dup_key) = BTree::key_index_from_node(&left_node, &key_value, pager, table);
                BTree::insert_row_to_leaf(&mut left_node, splice_at, key, duplicate_key || dup_key, row_meta_data, pager, table);
            } else {
                let (splice_at, dup_key) = BTree::key_index_from_node(&right_node, &key_value, pager, table);
                BTree::insert_row_to_leaf(&mut right_node, splice_at, key, duplicate_key || dup_key, row_meta_data, pager, table);
            }

            return (left_node, right_node);
        }
    }

    fn insert_row_to_leaf(node: &mut TreeNode, i: usize, key: u32, duplicate_key: bool, row_meta_data: (u32, usize), pager: &mut Pager, table: &mut Table) {
        let (row_offset, row_size) = row_meta_data;

        if !duplicate_key {
            node.splice_key(key, row_size, i); 
            node.splice_node_child(row_offset, row_size, false, i);

            if let Some(_) = node.overflow_children(i) {
                if let Some(overflow_child) = node.take_cached_node_overflow_child(i) {
                    node.cache_node_overflow_child(overflow_child, i + 1);
                }
            }
        } else {
            if let Some((child_offset, child_size, is_overflowing)) = node.child(i) {
                if !is_overflowing {
                    node.add_node_overflow_child(row_offset, row_size, i);

                    let (row_offset, row_size) = node.take_node_child(i);
                    node.add_node_overflow_child(row_offset, row_size, i);

                    if let Some(child_overflow_data) = node.overflow_data(i) {
                        let child_overflow_offset = pager.add_to_write_buffer(child_overflow_data, None);
                        node.splice_node_child(child_overflow_offset, child_overflow_data.len(), true, i);
                    }
                } else {
                    if let None = node.cached_node_overflow_child(i) {
                        if let Some(bytes) = pager.read_from_file(child_offset as usize, child_size) {
                            node.cache_node_overflow_child(NodeOverflow::from_bytes(bytes), i);
                        }
                    }

                    node.add_node_overflow_child(row_offset, row_size, i);
                    if let Some(child_overflow_data) = node.overflow_data(i) {
                        let child_overflow_offset = pager.add_to_write_buffer(child_overflow_data, Some((child_offset, child_size)));
                        node.replace_node_child(child_overflow_offset, child_overflow_data.len(), true, i);
                    }
                }
            } else {
                node.add_node_overflow_child(row_offset, row_size, i);
                if let Some(child_overflow_data) = node.overflow_data(i) {
                    let child_overflow_offset = pager.add_to_write_buffer(child_overflow_data, None);
                    node.splice_node_child(child_overflow_offset, child_overflow_data.len(), true, i);
                }
            }
        }
    }

    pub fn search<'a>(&'a mut self, key: Vec<u8>, pager: &mut Pager, table: &'a mut Table) -> Option<Vec<&'a Row>> {
        if let Some(root) = &mut self.root {
            BTree::search_node(root, key, pager, table)
        } else {
            None
        }
    }

    fn search_node<'a>(node: &'a mut TreeNode, key: Vec<u8>, pager: &mut Pager, table: &'a mut Table) -> Option<Vec<&'a Row>> {
        let (i, _) = BTree::key_index_from_node(&node, &key, pager, table);

        match node.node_type() {
            NodeType::Internal => BTree::search_internal(node, key, i, pager, table),
            NodeType::Leaf => BTree::search_leaf(node, key, i, pager, table)
        }
    }

    fn search_internal<'a>(node: &'a mut TreeNode, key: Vec<u8>, i: usize, pager: &mut Pager, table: &'a mut Table) -> Option<Vec<&'a Row>> {
        if let None = node.cached_tree_node_child(i) {
            if let Some((child_offset, child_size, _)) = node.child(i) {
                if let Some(bytes) = pager.read_from_file(child_offset as usize, child_size) {
                    node.cache_tree_node_child(TreeNode::from_bytes(bytes), i);
                }
            }
        }

        if let Some(child_node) = node.chached_mut_tree_node_child(i) {
            if let Some(row) = BTree::search_node(child_node, key, pager, table) {
                return Some(row);
            } 
        }

        None
    }

    fn search_leaf<'a>(node: &'a mut TreeNode, key: Vec<u8>, i: usize, pager: &mut Pager, table: &'a mut Table) -> Option<Vec<&'a Row>> {
        if let Some((child_offset, child_size, is_overflowing)) = node.child(i) {
            if !is_overflowing {
                let row_meta_data = (child_offset, child_size);
                if key.cmp(&BTree::key_value_from_table(row_meta_data, node.node_indexed_column(), pager, table)).is_eq() {
                    if let Some(row) = table.row(child_offset) {
                        return Some(vec![row]);
                    }                
                }
            } else {
                if let None = node.cached_node_overflow_child(i) {
                    if let Some(bytes) = pager.read_from_file(child_offset as usize, child_size) {
                        node.cache_node_overflow_child(NodeOverflow::from_bytes(bytes), i);
                    }
                }

                if let Some(overflow_child) = node.overflow_children(i) {
                    for (overflow_child_offset, overflow_child_size) in overflow_child.items() {
                        let row_meta_data = (overflow_child_offset, overflow_child_size);
                        if !key.cmp(&BTree::key_value_from_table(row_meta_data, node.node_indexed_column(), pager, table)).is_eq() {
                            return None;
                        }
                    }
                }

                if let Some(overflow_child) = node.overflow_children(i) {
                    let rows: Vec<&Row> = overflow_child.items()
                        .iter()
                        .filter_map(|(overflow_child_offset, _)| table.row(*overflow_child_offset))
                        .collect();

                    return Some(rows);
                }
            }
        }

        None
    }

    pub fn delete(&mut self, key: Vec<u8>, pager: &mut Pager, table: &mut Table) -> Option<Vec<Row>> {
        if let Some(root) = self.root.take() {
            let (root, deleted_rows) = BTree::delete_node(root, key, pager, table); 
            self.root = Some(root);

            return deleted_rows;
        }

        None
    }

    fn delete_node(mut node: TreeNode, key: Vec<u8>, pager: &mut Pager, table: &mut Table) -> (TreeNode, Option<Vec<Row>>) {
        let (i, _) = BTree::key_index_from_node(&node, &key, pager, table);
        let i = if i < node.children_len() || i <= 0 { i } else { node.children_len() - 1 };

        match node.node_type() {
            NodeType::Internal => BTree::delete_internal(node, key, i, pager, table),
            NodeType::Leaf => BTree::delete_leaf(node, key, i, pager, table)
        }
    }

    fn delete_internal(mut node: TreeNode, key: Vec<u8>, i: usize, pager: &mut Pager, table: &mut Table) -> (TreeNode, Option<Vec<Row>>) {
        // * Load child in if not in cache *
        if let None = node.cached_tree_node_child(i) {
            if let Some((child_offset, child_size, _)) = node.child(i) {
                if let Some(bytes) = pager.read_from_file(child_offset as usize, child_size) {
                    node.cache_tree_node_child(TreeNode::from_bytes(bytes), i);
                }
            }
        }

        // * If key match found... *
        let node_key_value = BTree::key_value_from_node(&node, i, pager, table);
        let deleted_rows = if i < NODE_MAX_KEYS && key.cmp(&node_key_value).is_eq() {
            // * Traverse down to find row to remove at before removing key *
            let deleted_rows = if let Some(child) = node.take_cached_tree_node_child(i) {
                let (child, deleted_rows) = BTree::delete_node(child, key, pager, table);
                node.cache_tree_node_child(child, i);

                deleted_rows
            } else {
                None
            };
            
            // * Remove key if rows removed and balance *
            if let Some(_) = deleted_rows {
                node.remove_key(i);
                BTree::delete_balance(&mut node, i, pager, table);
            }

            deleted_rows
        } else {
            // * Traverse down to find row to remove at *
            if let Some(child) = node.take_cached_tree_node_child(i) {
                let (child, deleted_rows) = BTree::delete_node(child, key, pager, table);
                node.cache_tree_node_child(child, i);

                deleted_rows
            } else {
                None
            }
        };

        if let Some(_) = deleted_rows {
            // * Balance tree if intenral node falls beneath children threshold *
            if let Some(child) = node.cached_tree_node_child(i) {
                if child.keys_len() < NODE_MIN_KEYS {
                    BTree::delete_balance(&mut node, i, pager, table);
                }
            }
        }

        // * Add modified node to file and replace pointer * 
        if let Some(child) = node.cached_tree_node_child(i) {
            let offset = pager.add_to_write_buffer(child.data(), node.child_offset_child_size(i));
            node.replace_node_child(offset, child.size(), false, i);
        }

        (node, deleted_rows)
    }

    fn delete_leaf(mut node: TreeNode, key: Vec<u8>, i: usize, pager: &mut Pager, table: &mut Table) -> (TreeNode, Option<Vec<Row>>) {
        let deleted_rows = if !node.child_is_overflowing(i) {
            if let Some(row) = node.child_offset_child_size(i) {
                let (row_offset, row_size) = row;
                if key.cmp(&BTree::key_value_from_table(row, node.node_indexed_column(), pager, table)).is_eq() {
                    if let Some(row) = table.delete_row(row_offset) {
                        pager.mark_free(row_size, row_offset);
                        Some(vec![row])
                    } else {
                        None
                    }
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            if let Some(overflow_children) = node.overflow_children(i) {
                let deleted_rows: Vec<Row> = overflow_children
                    .items()
                    .iter()
                    .filter_map(|row| {
                        if key.cmp(&BTree::key_value_from_table(*row, node.node_indexed_column(), pager, table)).is_eq() {
                            let (row_offset, row_size) = row;
                            if let Some(row) = table.delete_row(*row_offset) {
                                pager.mark_free(*row_size, *row_offset);
                                Some(row)
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    })
                    .collect();
                
                if let Some((overflow_child_offset, overflow_child_size, _)) = node.child(i) {
                    node.remove_node_overflow_child(i);
                    pager.mark_free(overflow_child_size, overflow_child_offset);
                }

                Some(deleted_rows)
            } else {
                None
            }
        };

        // * If rows deleted remove child pointer and key if it exists on node  *
        if let Some(_) = deleted_rows {
            node.remove_node_child(i);
            let node_key_value = BTree::key_value_from_node(&node, i, pager, table);
            if i < NODE_MAX_KEYS && key.cmp(&node_key_value).is_eq() {
                node.remove_key(i);
            }
        }

        (node, deleted_rows)
    }

    fn delete_balance(parent_node: &mut TreeNode, i: usize, pager: &mut Pager, table: &mut Table) -> Option<()> {
        let (mut left_child, mut right_child) = BTree::node_take_left_right_children(parent_node, i, pager)?;
        let (left_index, right_index) = parent_node.left_right_children_of_index(i);
        let mut left_keys = left_child.keys_len();
        let mut right_keys = right_child.keys_len();

        if left_keys >= NODE_MIN_KEYS && right_keys >= NODE_MIN_KEYS {
            if left_keys >= right_keys {
                let (promote_key, remote_key_size) = left_child.key(left_keys - 1);
                parent_node.splice_key(promote_key, remote_key_size, left_index);

                let delete_key = BTree::key_value_from_node(&parent_node, left_index, pager, table);
                match left_child.node_type() {
                    NodeType::Internal => {
                        let (child, _) = BTree::delete_node(left_child, delete_key, pager, table);
                        left_child = child;
                    },                    
                    NodeType::Leaf => left_child.remove_key(left_keys - 1)
                }
                
                left_keys = left_child.keys_len();
            } else {
                let (promote_key, remote_key_size) = right_child.key(0);
                parent_node.splice_key(promote_key, remote_key_size, right_index - 1);

                let delete_key = BTree::key_value_from_node(&parent_node, left_index, pager, table);
                match right_child.node_type() {
                    NodeType::Internal => {
                        let (child, _) = BTree::delete_node(right_child, delete_key, pager, table);
                        right_child = child;
                    },
                    NodeType::Leaf => {
                        left_child.remove_key(0)
                        // let (row_offset, row_size) = right_child.take_node_child(0);
                        // left_child.add_node_child(row_offset, row_size, false);
                    }
                }

                right_keys = right_child.keys_len();
            }
        }

        if parent_node.is_root() && parent_node.keys_len() < NODE_MIN_KEYS && (
            (left_keys < NODE_MIN_KEYS && right_keys <= NODE_MIN_KEYS) || (left_keys <= NODE_MIN_KEYS && right_keys < NODE_MIN_KEYS)
        ) {
            BTree::merge_left_right_child_to_parent(parent_node, left_child, right_child, pager, i);
        } else if left_keys < NODE_MIN_KEYS && right_keys > NODE_MIN_KEYS {
            BTree::demote_key_move_left_child_to_right(parent_node, left_child, right_child, pager, i);
        } else if left_keys > NODE_MIN_KEYS && right_keys < NODE_MIN_KEYS {
            BTree::demote_key_move_right_child_to_left(parent_node, left_child, right_child, pager, i);
        } else if (left_keys >= NODE_MIN_KEYS && right_keys < NODE_MIN_KEYS) || (left_keys < NODE_MIN_KEYS && right_keys >= NODE_MIN_KEYS) {
            BTree::demote_key_merge_left_to_right(parent_node, left_child, right_child, pager, i);
        } else {
            parent_node.cache_tree_node_child(left_child, left_index);
            parent_node.cache_tree_node_child(right_child, right_index);
        }
        
        None
    }

    fn merge_left_right_child_to_parent(parent_node: &mut TreeNode, mut left_child: TreeNode, mut right_child: TreeNode, pager: &mut Pager, i: usize) {
        let (left_index, right_index) = parent_node.left_right_children_of_index(i);

        if let Some(left_offset) = parent_node.child_offset(left_index) {
            pager.mark_free(left_child.data().len(), left_offset)
        }
        if let Some(right_offset) = parent_node.child_offset(right_index) {
            pager.mark_free(right_child.data().len(), right_offset);
        }  
        
        parent_node.clear_children();
        parent_node.prepend_merge(&mut left_child);
        parent_node.append_merge(&mut right_child);

        match left_child.node_type() {
            NodeType::Leaf if parent_node.is_root() => parent_node.set_node_type(NodeType::Leaf),
            NodeType::Internal => (),
            NodeType::Leaf => ()
        }
    }

    fn demote_key_move_left_child_to_right(parent_node: &mut TreeNode, mut left_child: TreeNode, mut right_child: TreeNode, pager: &mut Pager, i: usize) -> Option<()> {
        let (left_index, right_index) = parent_node.left_right_children_of_index(i);

        let (demote_key, remote_key_size) = parent_node.take_key(i); //left_index
        left_child.append_key(demote_key, remote_key_size);

        let (promote_key, remote_key_size) = right_child.take_key(0);
        parent_node.splice_key(promote_key, remote_key_size, left_index); // right index

        if right_child.has_children() {
            let move_child_index = 0;

            match right_child.node_type() {
                NodeType::Internal => {
                    let move_child = BTree::node_take_child(&mut right_child, move_child_index, pager)?;
                    let move_child_offset = right_child.child_offset(move_child_index)?;
            
                    left_child.add_tree_node_child(move_child, move_child_offset, false);
                    right_child.remove_tree_node_child(move_child_index);
                },
                NodeType::Leaf => {
                    let (move_child_offset, move_child_size) = right_child.take_node_child(move_child_index);
                    left_child.add_node_child(move_child_offset, move_child_size, false);

                    if let Some(overflow_child) = right_child.take_cached_node_overflow_child(i) {
                        // left_child.cache_node_overflow_child(overflow_child, left_child.overflow_children_len());
                        left_child.cache_node_overflow_child(overflow_child, left_child.keys_len() - 1);
                    }
                }
            }
        }

        if let Some((left_offset, left_size)) = parent_node.child_offset_child_size(left_index) {
            let offset = pager.add_to_write_buffer(left_child.data(), Some((left_offset, left_size)));
            parent_node.replace_node_child(offset, left_child.size(), false, left_index);
        }
        if let Some((right_offset, right_size)) = parent_node.child_offset_child_size(right_index) {
            let offset = pager.add_to_write_buffer(right_child.data(), Some((right_offset, right_size)));
            parent_node.replace_node_child(offset, right_child.size(), false, right_index);
        }
        
        parent_node.cache_tree_node_child(left_child, left_index);
        parent_node.cache_tree_node_child(right_child, right_index);
        None
    }

    fn demote_key_move_right_child_to_left(parent_node: &mut TreeNode, mut left_child: TreeNode, mut right_child: TreeNode, pager: &mut Pager, i: usize) -> Option<()> {
        let (left_index, right_index) = parent_node.left_right_children_of_index(i);

        let (demote_key, remote_key_size) = parent_node.take_key(i); //right_index
        right_child.prepend_key(demote_key, remote_key_size);

        let (promote_key, remote_key_size) = left_child.take_key(left_child.keys_len() - 1);
        parent_node.splice_key(promote_key, remote_key_size, right_index); //left_index

        if left_child.has_children() {
            let move_child_index = left_child.children_len() - 1;

            match left_child.node_type() {
                NodeType::Internal => {
                    let move_child = BTree::node_take_child(&mut left_child, move_child_index, pager)?;
                    let move_child_offset = left_child.child_offset(move_child_index)?;
        
                    right_child.splice_tree_node_child(move_child, move_child_offset, false, 0);
                    left_child.remove_tree_node_child(move_child_index);
                },
                NodeType::Leaf => {
                    let (move_child_offset, move_child_size) = left_child.take_node_child(move_child_index);
                    right_child.add_node_child(move_child_offset, move_child_size, false);

                    if let Some(overflow_child) = left_child.take_cached_node_overflow_child(i) {
                        right_child.cache_node_overflow_child(overflow_child, right_child.overflow_children_len());
                    }
                }
            }

            let move_child = BTree::node_take_child(&mut left_child, move_child_index, pager)?;
            let move_child_offset = left_child.child_offset(move_child_index)?;

            right_child.splice_tree_node_child(move_child, move_child_offset, false, 0);
            left_child.remove_tree_node_child(move_child_index);
        }
        
        if let Some((left_offset, left_size)) = parent_node.child_offset_child_size(left_index) {
            let offset = pager.add_to_write_buffer(left_child.data(), Some((left_offset, left_size)));
            parent_node.replace_node_child(offset, left_child.size(), false, left_index);
        }
        if let Some((right_offset, right_size)) = parent_node.child_offset_child_size(right_index) {
            let offset = pager.add_to_write_buffer(right_child.data(), Some((right_offset, right_size)));
            parent_node.replace_node_child(offset, right_child.size(), false, right_index);
        }

        parent_node.cache_tree_node_child(left_child, left_index);
        parent_node.cache_tree_node_child(right_child, right_index);

        None
    }

    fn demote_key_merge_left_to_right(parent_node: &mut TreeNode, mut left_child: TreeNode, mut right_child: TreeNode, pager: &mut Pager, i: usize) {
        let (left_index, right_index) = parent_node.left_right_children_of_index(i);

        let (demote_key, remote_key_size) = parent_node.take_key(left_index);
        left_child.append_key(demote_key, remote_key_size);
        left_child.append_merge(&mut right_child);

        if let Some((left_offset, left_size)) = parent_node.child_offset_child_size(left_index) {
            let offset = pager.add_to_write_buffer(left_child.data(), Some((left_offset, left_size)));
            parent_node.replace_node_child(offset, left_child.size(), false, left_index);
        }
        if let Some((right_offset, right_size)) = parent_node.child_offset_child_size(right_index) {
            pager.mark_free(right_size, right_offset);
            parent_node.remove_tree_node_child(right_index);
        }

        parent_node.cache_tree_node_child(left_child, left_index);
    }

    fn key_index_from_node(node: &TreeNode, key_value: &Vec<u8>, pager: &mut Pager, table: &mut Table) -> (usize, bool) {
        let mut i = 0;

        while i < node.keys_len() && key_value.cmp(&BTree::key_value_from_node(node, i, pager, table)).is_gt() {
            i += 1;
        }
        
        let duplicate_key = if i < node.keys_len() {
            key_value.cmp(&BTree::key_value_from_node(node, i, pager, table)).is_eq()
        } else {
            false
        };

        (i, duplicate_key)
    }

    fn key_value_from_node(node: &TreeNode, i: usize, pager: &mut Pager, table: &mut Table) -> Vec<u8> {
        match node.node_index_type() {
            NodeIndexType::Primary => node.key_value(i).to_le_bytes().to_vec(),
            NodeIndexType::Secondary => BTree::key_value_from_table( node.key(i), node.node_indexed_column(), pager, table)
        }
    } 

    fn key_value_from_table(row_meta_data: (u32, usize), indexed_column: Option<usize>, pager: &mut Pager, table: &mut Table) -> Vec<u8> {
        let (row_offset, row_size) = row_meta_data;

        if let None = table.row(row_offset) {
            if let Some(bytes) = pager.read_from_file(row_offset as usize, row_size) {
                table.insert_row(row_offset, Row::from_bytes(bytes));
            }
        }

        if let Some(row) = table.row(row_offset) {
            if let Some(indexed_column) = indexed_column {
                return row.cell_data(indexed_column).to_vec()
            } else {
                return row.primary_key_bytes().to_vec();
            }
        }

        vec![]
    }

    fn node_take_child(parent_node: &mut TreeNode, i: usize, pager: &mut Pager) -> Option<TreeNode> {
        if let Some(child) = parent_node.take_cached_tree_node_child(i) {
            Some(child)
        } else {
            let (child_offset, child_size, _) = parent_node.child(i).unwrap();
            if let Some(bytes) = pager.read_from_file(child_offset as usize, child_size) {
                Some(TreeNode::from_bytes(bytes))
            } else {
                None
            }
        }
    }

    fn node_take_left_right_children(parent_node: &mut TreeNode, i: usize, pager: &mut Pager) -> Option<(TreeNode, TreeNode)> {
        let (left_index, right_index) = parent_node.left_right_children_of_index(i);

        let (left_child, right_child) = if let Some((left_child, right_child)) = parent_node.take_cached_left_right_tree_node_child(left_index) {
            let left = if let Some(left) = left_child {
                left
            } else {
                let (child_offset, child_size, _) = parent_node.child(left_index)?;
                let bytes = pager.read_from_file(child_offset as usize, child_size)?;

                TreeNode::from_bytes(bytes)
            };

            let right = if let Some(right) = right_child {
                right
            } else {
                let (child_offset, child_size, _) = parent_node.child(right_index)?;
                let bytes = pager.read_from_file(child_offset as usize, child_size)?;

                TreeNode::from_bytes(bytes)
            };

            (left, right)
        } else {
            let (left_offset, left_size, _) = parent_node.child(left_index)?;
            let (right_offset, right_size, _) = parent_node.child(right_index)?;

            let left_bytes = pager.read_from_file(left_offset as usize, left_size)?;
            let right_bytes = pager.read_from_file(right_offset as usize, right_size)?;

            (TreeNode::from_bytes(left_bytes), TreeNode::from_bytes(right_bytes))
        };

        Some((left_child, right_child))
    }

    pub fn root(&self) -> Option<&TreeNode> {
        if let Some(root) = &self.root {
            return Some(root)
        }
        None
    }

    pub fn indexed_column(&self) -> Option<usize> {
        self.indexed_column
    }
}