use std::{cmp::{max, min}, ops::{Deref, DerefMut}};

use crate::db::shared::{constants::{node::{self, NODE_CHILDREN_OFFSET, NODE_CHILDREN_SIZE, NODE_INDEXED_COLUMN_OFFSET, NODE_INDEXED_COLUMN_SIZE, NODE_INDEX_TYPE_OFFSET, NODE_INDEX_TYPE_SIZE, NODE_IS_ROOT_OFFSET, NODE_IS_ROOT_SIZE, NODE_KEYS_OFFSET, NODE_KEYS_SIZE, NODE_NUMBER_CHILDREN_OFFSET, NODE_NUMBER_CHILDREN_SIZE, NODE_NUMBER_KEYS_OFFSET, NODE_NUMBER_KEYS_SIZE, NODE_SIZE, NODE_TYPE_OFFSET, NODE_TYPE_SIZE}, node_child::{NODE_CHILD_CHILD_SIZE, NODE_CHILD_OFFSET_SIZE, NODE_CHILD_OVERFLOWING_SIZE, NODE_CHILD_SIZE, NODE_MAX_CHILDREN, NODE_MIN_CHILDREN}, node_key::{NODE_KEY_REMOTE_ITEM_SIZE, NODE_KEY_SIZE, NODE_KEY_VALUE_SIZE, NODE_MAX_KEYS, NODE_MIN_KEYS}}, utils::bytes_to_u32};

#[derive(Debug)]
pub enum NodeType {
    Internal = 0,
    Leaf = 1,
}

#[derive(Clone, Copy, Debug)]
pub enum NodeIndexType {
    Primary = 0,
    Secondary = 1,
}

#[derive(Debug)]
pub struct Node(Vec<u8>);

impl Node {
    
    pub fn new(is_root: bool, node_type: NodeType, node_index_type: NodeIndexType, node_indexed_column: Option<usize>, key: u32, remote_key_size: usize) -> Node {
        let is_root: u8 = if is_root { 1 } else { 0 };
        let indexed_column = if let Some(node_indexed_column) = node_indexed_column { node_indexed_column as u32 } else { 0u32 };
        let num_keys: u32 = 1;
        let num_children: u32 = 0;

        let bytes = vec![is_root, node_type as u8, node_index_type as u8]
            .into_iter()
            .chain(indexed_column.to_le_bytes())// TODO: add param for indexed column
            .chain(num_keys.to_le_bytes())
            .chain((remote_key_size as u32).to_le_bytes()) // THIS IS NEW
            .chain(key.to_le_bytes())
            .chain([0u8; NODE_KEYS_SIZE - NODE_KEY_SIZE])
            .chain(num_children.to_le_bytes())
            .chain([0u8; NODE_CHILDREN_SIZE])
            .collect();

            Node(bytes)
    }

    pub fn from_bytes(bytes: &[u8]) -> Node {
        Node (bytes[0..NODE_SIZE].to_vec())
    }

    pub fn from_split_new(mut node: Node, split_keys_at: usize, split_children_at: usize) -> (Node, Node) {
        let split_num_keys = node.num_keys() - split_keys_at;
        let split_num_children = node.num_children() - split_children_at;

        let mut split_keys = node.shift_keys(split_keys_at, split_num_keys);
        split_keys.resize(NODE_MAX_KEYS * NODE_KEY_SIZE, 0);

        let mut split_children = if split_num_children > 0 {
            node.shift_children(split_children_at, split_num_children)
        } else {
            vec![]
        };
        split_children.resize(NODE_MAX_CHILDREN * NODE_CHILD_SIZE, 0);

        let is_root = 0u8;
        let node_type = node.node_type() as u8;
        let node_index_type = node.node_index_type() as u8;
        let indexed_column = node.node_indexed_column() as u32;
        let num_keys = (split_num_keys as u32).to_le_bytes();
        let bytes = vec![is_root, node_type, node_index_type]
            .into_iter()
            .chain(indexed_column.to_le_bytes())// TODO: add param for indexed column
            .chain(num_keys)
            .chain(split_keys)
            .chain(if split_num_children > 0 { (split_num_children as u32).to_le_bytes() } else { 0u32.to_le_bytes() })
            .chain(split_children)         
            .collect();

        (node, Node(bytes))
    }

    pub fn from_split_range(&mut self, i: usize, len: usize) -> Node {
        let mut split_keys = self.shift_keys(i, len);
        split_keys.resize(NODE_MAX_KEYS * NODE_KEY_SIZE, 0);

        let split_num_children = if self.num_children() > len { len + 1 } else { len };
        let mut split_children = self.shift_children(if self.is_root() { 0 } else { i }, split_num_children);
        split_children.resize(NODE_MAX_CHILDREN * NODE_CHILD_SIZE, 0);

        let is_root = 0u8;
        let node_type = self.node_type() as u8;
        let node_index_type = self.node_index_type() as u8;
        let indexed_column = self.node_indexed_column() as u32;
        let num_keys = (len as u32).to_le_bytes();
        let bytes = vec![is_root, node_type, node_index_type]
            .into_iter()
            .chain(indexed_column.to_le_bytes())// TODO: add param for indexed column
            .chain(num_keys)
            .chain(split_keys)
            .chain((split_num_children as u32).to_le_bytes())
            .chain(split_children)          
            .collect();

        Node(bytes)
    }

    pub fn append_merge(merge_to: &mut Node, merge_from: &mut Node) {
        for (i, key) in merge_from.keys().chunks(NODE_KEY_SIZE).enumerate() {
            if i >= merge_from.num_keys() { break; }

            merge_to.append_key_bytes(key.to_vec());
        }

        for (i, child) in merge_from.children().chunks(NODE_CHILD_SIZE).enumerate() {
            if i >= merge_from.num_children() { break; }

            merge_to.append_child_bytes(child.to_vec());
        }
    }

    pub fn prepend_merge(merge_to: &mut Node, merge_from: &mut Node) {
        let mut reverse_keys = merge_from
            .keys()
            .chunks(NODE_KEY_SIZE)
            .fold(vec![], |mut keys, key| {
                keys.push(key.to_vec());
                keys
            })
            .split_at(merge_from.num_keys())
            .0
            .to_vec();
        reverse_keys.reverse();
        for key in reverse_keys {
            merge_to.prepend_key_bytes(key);
        }

        if merge_from.num_children() > 0 {
            let mut reverse_children = merge_from
                .children()
                .chunks(NODE_CHILD_SIZE)
                .fold(vec![], |mut children, child| {
                    children.push(child.to_vec());
                    children
                })
                .split_at(merge_from.num_children())
                .0
                .to_vec();
            reverse_children.reverse();
            for child in reverse_children {
                merge_to.prepend_child_bytes(child);
            }
        }
    }

    pub fn append_key(&mut self, key: u32, remote_key_item_size: usize) {
        let keys_offset = self.num_keys() * NODE_KEY_SIZE;
        let (start, _) = Node::keys_range();

        let start = start + keys_offset;
        let end = start + NODE_KEY_SIZE;

        let key: Vec<u8> = (remote_key_item_size as u32).to_le_bytes()
            .into_iter()
            .chain(key.to_le_bytes())
            .collect();

        (*self).splice(start..end, key);
        self.increment_key();
    }

    fn append_key_bytes(&mut self, key: Vec<u8>) {
        let keys_offset = self.num_keys() * NODE_KEY_SIZE;
        let (start, _) = Node::keys_range();

        let start = start + keys_offset;
        let end = start + NODE_KEY_SIZE;

        (*self).splice(start..end, key);
        self.increment_key();
    }

    pub fn prepend_key(&mut self, key: u32, remote_key_item_size: usize) {
        let keys_offset = self.num_keys() * NODE_KEY_SIZE;
        let (start, _) = Node::keys_range();
        let end = start + keys_offset;

        let mut keys: Vec<u8> = (remote_key_item_size as u32).to_le_bytes()
            .into_iter()
            .chain(key.to_le_bytes())
            .collect();
        keys.extend(&(*self)[start..end]);

        (*self).splice(start..end + NODE_KEY_SIZE, keys);
        self.increment_key();
    }

    fn prepend_key_bytes(&mut self, mut key: Vec<u8>) {
        let keys_offset = self.num_keys() * NODE_KEY_SIZE;

        let (start, _) = Node::keys_range();
        let end = start + keys_offset;

        key.extend(&(*self)[start..end]);

        (*self).splice(start..end + NODE_KEY_SIZE, key);
        self.increment_key();
    }

    pub fn splice_key(&mut self, key: u32, remote_key_item_size: usize, i: usize) {
        let i = min(i, NODE_MAX_KEYS - 1);
        let key_offset = i * NODE_KEY_SIZE;
        let (start, end) = Node::keys_range();

        // let mut keys = (*self)[start..start + key_offset].to_vec();
        let mut bytes = (*self)[start..start + key_offset].to_vec();
        let key: Vec<u8> = (remote_key_item_size as u32).to_le_bytes()
            .into_iter()
            .chain(key.to_le_bytes())
            .collect();
        bytes.extend(key);
        bytes.extend(&(*self)[start + key_offset..end - NODE_KEY_SIZE]);

        (*self).splice(start..end, bytes);
        self.increment_key();
    }

    pub fn append_child(&mut self, child_offset: u32, size: usize, is_overflowing: bool) {
        let children_offset = self.num_children() * NODE_CHILD_SIZE;
        let (start, _) = Node::children_range();

        let start = start + children_offset;
        let end = start + NODE_CHILD_SIZE;

        let child: Vec<u8> = 
            (size as u32).to_le_bytes()
            .into_iter()
            .chain(child_offset.to_le_bytes())
            .chain(if is_overflowing { [1u8] } else { [0u8] })
            .collect();

        (*self).splice(start..end, child);
        self.increment_children();
    }

     fn append_child_bytes(&mut self, child: Vec<u8>) {
        let children_offset = self.num_children() * NODE_CHILD_SIZE;
        let (start, _) = Node::children_range();

        let start = start + children_offset;
        let end = start + NODE_CHILD_SIZE;

        (*self).splice(start..end, child);
        self.increment_children();
    }

    pub fn prepend_child(&mut self, child_offset: u32, size: usize, is_overflowing: bool) {
        let children_offset = self.num_children() * NODE_CHILD_SIZE;

        let (start, _) = Node::children_range();
        let end = start + children_offset;

        let mut child: Vec<u8> = (size as u32).to_le_bytes()
            .into_iter()
            .chain(child_offset.to_le_bytes())
            .chain(if is_overflowing { [1u8] } else { [0u8] })
            .collect();
        child.extend(&(*self)[start..end]);

        (*self).splice(start..end + NODE_CHILD_SIZE, child);
        self.increment_children();
    }

    // TODO: fix how this takes child: u32
    pub fn replace_child(&mut self, child_offset: u32, size: usize, is_overflowing: bool, i: usize) {
        let children_offset = i * NODE_CHILD_SIZE;

        let (start, _) = Node::children_range();
        let start = start + children_offset;
        let end = start + NODE_CHILD_SIZE;
        let child: Vec<u8> = (size as u32).to_le_bytes()
            .into_iter()
            .chain(child_offset.to_le_bytes())
            .chain(if is_overflowing { [1u8] } else { [0u8] })
            .collect();

        (*self).splice(start..end, child);
    }

    pub fn splice_child(&mut self, child_offset: u32, size: usize, is_overflowing: bool, i: usize) {
        let i = min(i, NODE_MAX_CHILDREN - 1); // TODO: maybe remove? (it's working in splice_keys)
        let children_offset = i * NODE_CHILD_SIZE;
        let (start, end) = Node::children_range();

        let mut bytes = (*self)[start..start + children_offset].to_vec();
        let child: Vec<u8> = 
            (size as u32).to_le_bytes()
            .into_iter()
            .chain(child_offset.to_le_bytes())
            .chain(if is_overflowing { [1u8] } else { [0u8] })
            .collect();
        bytes.extend(child);
        bytes.extend(&(*self)[start + children_offset..end - NODE_CHILD_SIZE]);

        (*self).splice(start..end, bytes);
        self.increment_children();
    }

    fn prepend_child_bytes(&mut self, mut child: Vec<u8>) {
        let children_offset = self.num_children() * NODE_CHILD_SIZE;

        let (start, _) = Node::children_range();
        let end = start + children_offset;

        child.extend(&(*self)[start..end]);

        (*self).splice(start..end + NODE_CHILD_SIZE, child);
        self.increment_children();
    }

    pub fn clear_children(&mut self) {
        let (start, end) = Node::children_range();
        let children = vec![0; NODE_CHILD_SIZE * NODE_MAX_CHILDREN];

        (*self).splice(start..end, children);
        self.set_num_children(0);
    }

    pub fn shift_keys(&mut self, i: usize, num_keys: usize) -> Vec<u8> {
        let key_offset_start = i * NODE_KEY_SIZE;
        let (start, end) = Node::keys_range();
        let start = start + key_offset_start;
        let shift_end = start + (num_keys * NODE_KEY_SIZE);

        let mut shift_keys = Vec::new();
        let mut offset = shift_end;
        for i in start..end {
            if i < shift_end {
                shift_keys.push((*self)[i]);
            }

            (*self)[i] = (*self)[offset];
            if offset < end {
                (*self)[offset] = 0;
            } else {
                (*self)[i] = 0;
            }

            offset += 1;
        }

        self.set_num_keys((self.num_keys() - num_keys) as u32);
        // self.set_num_keys(if self.num_keys()  > num_keys { (self.num_keys() - num_keys) as u32 } else { 0 });

        shift_keys
    }

    pub fn shift_children(&mut self, i: usize, num_children: usize) -> Vec<u8> {
        let children_offset_start = i * NODE_CHILD_SIZE;
        let (start, end) = Node::children_range();
        let start = start + children_offset_start;
        let shift_end = start + (num_children * NODE_CHILD_SIZE) as usize;

        let mut shift_children = Vec::new();
        let mut offset = shift_end;
        for i in start..end {
            if i < shift_end {
                shift_children.push((*self)[i]);
            }

            if offset < end {
                (*self)[i] = (*self)[offset];
                (*self)[offset] = 0;
            } else {
                (*self)[i] = 0;
            }

            offset += 1;
        }

        self.set_num_children(if self.num_children() > num_children { (self.num_children() - num_children) as u32 } else { 0 });

        shift_children
    }

    pub fn take_key(&mut self, i: usize) -> (u32, usize) {
        let key = self.shift_keys(i, 1);
        let remote_key_item_size = bytes_to_u32(&key[0..NODE_KEY_REMOTE_ITEM_SIZE]) as usize;
        let key_value = bytes_to_u32(&key[NODE_KEY_REMOTE_ITEM_SIZE..NODE_KEY_REMOTE_ITEM_SIZE + NODE_KEY_VALUE_SIZE]);

        (key_value, remote_key_item_size)
    }

    pub fn take_child(&mut self, i: usize) -> (u32, usize) {
        let child = self.shift_children(i, 1);
        let child_size = bytes_to_u32(&child[0..NODE_CHILD_CHILD_SIZE]) as usize;
        let child_offset = bytes_to_u32(&child[NODE_CHILD_CHILD_SIZE..NODE_CHILD_CHILD_SIZE + NODE_CHILD_OFFSET_SIZE]);

        (child_offset, child_size)
    }

    pub fn increment_key(&mut self) {
        let mut num_keys = self.num_keys();
        num_keys += 1;

        self.set_num_keys(num_keys as u32);
    }

    pub fn decrement_key(&mut self) {
        let mut num_keys = self.num_keys();
        num_keys -= 1;

        self.set_num_keys(num_keys as u32);
    }

    pub fn increment_children(&mut self) {
        let mut num_children = self.num_children();
        num_children += 1;

        self.set_num_children(num_children as u32);
    }

    pub fn decrement_children(&mut self) {
        let mut num_children = self.num_children();
        num_children -= 1;

        self.set_num_children(num_children as u32);
    }

    pub fn is_root(&self) -> bool {
        let (start, end) = Node::is_root_range();

        bytes_to_u32(&(*self)[start..end]) == 1
    }

    pub fn node_type(&self) -> NodeType {
        let (start, end) = Node::node_type_range();
        if (*self)[start..end][0] == 0 {
            NodeType::Internal
        } else {
            NodeType::Leaf
        }
    }

    pub fn node_index_type(&self) -> NodeIndexType {
        let (start, end) = Node::node_index_type_range();

        if bytes_to_u32(&(*self)[start..end]) == 0 {
            NodeIndexType::Primary
        } else {
            NodeIndexType::Secondary
        }
    }

    pub fn node_indexed_column(&self) -> usize {
        let (start, end) = Node::node_indexed_column_range();

        bytes_to_u32(&(*self)[start..end]) as usize
    }

     pub fn num_keys(&self) -> usize {
        let (start, end) = Node::number_keys_range();

        bytes_to_u32(&(*self)[start..end]) as usize
    }

    pub fn num_children(&self) -> usize {
        let (start, end) = Node::number_children_range();

        bytes_to_u32(&(*self)[start..end]) as usize
    }

    pub fn keys(&self) -> &[u8] {
        let (start, end) = Node::keys_range();

        &(*self)[start..end]
    }

    pub fn key_value_as_u32(&self, i: usize) -> u32 {
        let (start, _) = Node::keys_range();
        let key_offset = NODE_KEY_SIZE * i;
        let start = start + key_offset + NODE_KEY_REMOTE_ITEM_SIZE;
        let end = start + NODE_KEY_VALUE_SIZE;

        // bytes_to_u32(&(*self)[start + key_offset..start + key_offset + NODE_KEY_SIZE])
        bytes_to_u32(&(*self)[start..end])
    }

    pub fn key_remote_item_size(&self, i: usize) -> usize {
        let (start, _) = Node::keys_range();
        let key_offset = NODE_KEY_SIZE * i;
        let start = start + key_offset;
        let end = start + NODE_KEY_REMOTE_ITEM_SIZE;

        bytes_to_u32(&(*self)[start..end]) as usize
    }

    pub fn children(&self) -> &[u8] {
        let (start, end) = Node::children_range();

        &(*self)[start..end]
    }

    pub fn child_size_as_usize(&self, i: usize) -> usize {
        let (start, _) = Node::children_range();
        let children_offset = NODE_CHILD_SIZE * i;
        let start = start + children_offset;
        let end = start + NODE_CHILD_CHILD_SIZE;

        bytes_to_u32(&(*self)[start..end]) as usize
    }

    pub fn child_offset_as_u32(&self, i: usize) -> u32 {
        let (start, _) = Node::children_range();
        let children_offset = NODE_CHILD_SIZE * i;
        let start = start + children_offset + NODE_CHILD_CHILD_SIZE;
        let end = start + NODE_CHILD_OFFSET_SIZE;

        bytes_to_u32(&(*self)[start..end])
    }

    pub fn child_is_overflowing(&self, i: usize) -> bool {
        let (start, _) = Node::children_range();
        let children_offset = NODE_CHILD_SIZE * i;
        let start = start + children_offset + NODE_CHILD_CHILD_SIZE + NODE_CHILD_OFFSET_SIZE;
        let end = start + NODE_CHILD_OVERFLOWING_SIZE;

        (*self)[start..end][0] == 1
    }

    pub fn children_as_u32(&self) -> Option<Vec<u32>> {
        if self.num_children() > 0 {
            let children = self.children()
                .to_vec()
                .chunks(NODE_CHILD_SIZE)
                .map(|c| bytes_to_u32(&c[NODE_CHILD_CHILD_SIZE..NODE_CHILD_CHILD_SIZE + NODE_CHILD_OFFSET_SIZE]))
                .collect();
            Some(children)
        } else {
            None
        }
    }

    // pub fn overflow_children_size(&self) -> usize {
    //     let (start, end) = Node::overflow_children_size_range();

    //     bytes_to_u32(&(*self)[start..end]) as usize
    // }

    // pub fn overflow_children_offset(&self) -> u32 {
    //     let (start, end) = Node::overflow_children_offset_range();

    //     bytes_to_u32(&(*self)[start..end])
    // }

    pub fn data(&self) -> &[u8] {
        &(*self)
    }

    fn set_num_keys(&mut self, num_keys: u32) {
        let (start, end) = Node::number_keys_range();

        (*self).splice(start..end, num_keys.to_le_bytes());
    }

    fn set_num_children(&mut self, num_children: u32) {
        let (start, end) = Node::number_children_range();

        (*self).splice(start..end, num_children.to_le_bytes());
    }

    pub fn set_node_type(&mut self, node_type: NodeType) {
        let (start, end) = Node::node_type_range();

        (*self).splice(start..end, (node_type as u8).to_le_bytes());
    }

    // pub fn set_is_overflowing(&mut self, is_overflowing: bool) {
    //     let (start, end) = Node::is_overflowing_range();
    //     let is_overflowing = if is_overflowing { 1u8 } else { 0u8 };

    //     (*self)[start..end][0] = is_overflowing; 
    // }

    // pub fn set_overflow_children_size(&mut self, size: usize) {
    //     let (start, end) = Node::overflow_children_size_range();

    //     (*self).splice(start..end, (size as u32).to_le_bytes());
    // }

    // pub fn set_overflow_children_offset(&mut self, offset: u32) {
    //     let (start, end) = Node::overflow_children_offset_range();

    //     (*self).splice(start..end, offset.to_le_bytes());
    // }

    fn is_root_range() -> (usize, usize) {
        (NODE_IS_ROOT_OFFSET, NODE_IS_ROOT_OFFSET + NODE_IS_ROOT_SIZE)
    }

    fn node_type_range() -> (usize, usize) {
        (NODE_TYPE_OFFSET, NODE_TYPE_OFFSET + NODE_TYPE_SIZE)
    }

    fn node_index_type_range() -> (usize, usize) {
        (NODE_INDEX_TYPE_OFFSET, NODE_INDEX_TYPE_OFFSET + NODE_INDEX_TYPE_SIZE)
    }

    fn node_indexed_column_range() -> (usize, usize) {
        (NODE_INDEXED_COLUMN_OFFSET, NODE_INDEXED_COLUMN_OFFSET + NODE_INDEXED_COLUMN_SIZE)
    }

    fn number_keys_range() -> (usize, usize) {        
        (NODE_NUMBER_KEYS_OFFSET, NODE_NUMBER_KEYS_OFFSET + NODE_NUMBER_KEYS_SIZE)
    }

    fn number_children_range() -> (usize, usize) {
        (NODE_NUMBER_CHILDREN_OFFSET, NODE_NUMBER_CHILDREN_OFFSET + NODE_NUMBER_CHILDREN_SIZE)
    }

    fn keys_range() -> (usize, usize) {
        (NODE_KEYS_OFFSET, NODE_KEYS_OFFSET + NODE_KEYS_SIZE)
    }

    fn children_range() -> (usize, usize) {
        (NODE_CHILDREN_OFFSET, NODE_CHILDREN_OFFSET + NODE_CHILDREN_SIZE)
    }
}

impl Deref for Node {
    type Target = Vec<u8>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Node {
    fn deref_mut(&mut self) -> &mut Self::Target {
            &mut self.0
    }
}

/* 
I need to create new methods for accessing child pointers (and keys for that matter). The child_offset() method in tree_node is using children_as_u32(),
which is expensive. It should be using child_offset_as_u32() (a new method I'll create -> also create key_value_as_u32). Once these are created, I shouldn't need
to use children_as_u32() in tree_node, and should be able to refactor calls to children() in tree. Basically, in order to begin enhancing the node to
include both a pointer to the child AND the size of the child, I need to eliminate current instances where I'm accessing all the children. Because
children will no longer just be pointers, I can't simply access this slice of data and index into it to get a child pointer  

> I need to be able to pass size to all instance where I'm inserting a child (from any direction)
> I need to be able to get the child offset from within the new child structure (offset + size)
> I need to be able to get the size from within the new child structure
> I need to update all SLAB allocation to be 92 instead of 92
> I need to update the read_from_file to also take the number of bytes to read
> I need to update the CHILD_SIZE contant to be the sum of the offset size and the child size
 */