use std::cmp;

use crate::db::shared::constants::node_child::{NODE_MAX_CHILDREN, NODE_MIN_CHILDREN};

use super::disk_storage::{node::{Node, NodeIndexType, NodeType}, node_overflow::NodeOverflow};

#[derive(Debug)]
pub struct TreeNode {
    disk_node: Node,
    pub cached_children: Option<Vec<Option<Box<TreeNode>>>>,
    cached_overflow_children: Option<Vec<Option<NodeOverflow>>>,
}

impl TreeNode {
    pub fn new(is_root: bool, node_type: NodeType, node_index_type: NodeIndexType, node_indexed_column: Option<usize>, key: u32, remote_key_size: usize) -> TreeNode {
        TreeNode {
            disk_node: Node::new(is_root, node_type, node_index_type, node_indexed_column, key, remote_key_size),
            cached_children: None,
            cached_overflow_children: None,
        }
    }

    pub fn from_bytes(bytes: Vec<u8>) -> TreeNode {
        TreeNode {
            disk_node: Node::from_bytes(&bytes),
            cached_children: None,
            cached_overflow_children: None,
        }
    }

    pub fn cache_tree_node_child(&mut self, tree_child: TreeNode, i: usize) {
        if let Some(children) = &mut self.cached_children {
            if children.len() < NODE_MAX_CHILDREN {
                children.insert(i, Some(Box::new(tree_child)));
            } else {
                children[i] = Some(Box::new(tree_child));
            }
        } else {
            let mut cache = Vec::with_capacity(NODE_MAX_CHILDREN);
            cache.extend((0..NODE_MAX_CHILDREN).map(|_| None));
            cache.insert(i, Some(Box::new(tree_child)));

            self.cached_children = Some(cache);
        }
    }

    pub fn cached_tree_node_child(&self, i: usize) -> Option<&TreeNode> {
        if let Some(children) = &self.cached_children {
            if let Some(Some(node)) = children.get(i) {
                return Some(&**node);
            }
        }
        None
    }

    pub fn chached_mut_tree_node_child(&mut self, i: usize) -> Option<&mut TreeNode> {
        if let Some(children) = &mut self.cached_children {
            if let Some(Some(node)) = children.get_mut(i) {
                return Some(&mut **node);
            }
        }
        None
    }

    pub fn take_cached_tree_node_child(&mut self, i: usize) -> Option<TreeNode> {
        if let Some(children) = &mut self.cached_children {
            if let Some(child) = children.remove(i) {
               return Some(*child);
            }
        } 
        None
    }

    pub fn take_cached_left_right_tree_node_child(&mut self, i: usize) -> Option<(Option<TreeNode>, Option<TreeNode>)> {
        if let Some(children) = &mut self.cached_children {
            let left = if let Some(left) = children.remove(i) {
                Some(*left)
            } else {
                None
            };

            let right = if let Some(right) = children.remove(i) {
                Some(*right)
            } else {
                None
            };

            return Some((left, right));
        }
        None
    }

    pub fn add_tree_node_child(&mut self, tree_child: TreeNode, offset: u32, is_overflowing: bool) {
        self.disk_node.append_child(offset, tree_child.data().len(), is_overflowing);

        if let Some(children) = &mut self.cached_children {
            children.push(Some(Box::new(tree_child)));
        } else {
            let mut cache = Vec::with_capacity(NODE_MAX_CHILDREN);
            cache.push(Some(Box::new(tree_child)));

            self.cached_children = Some(cache);
        }
    }

    pub fn splice_tree_node_child(&mut self, tree_child: TreeNode, offset: u32, is_overflowing: bool, i: usize) {
        self.disk_node.splice_child(offset, tree_child.data().len(), is_overflowing, i);

        if let Some(children) = &mut self.cached_children {
            children.insert(i, Some(Box::new(tree_child)));
        } else {
            let mut cache = Vec::with_capacity(NODE_MAX_CHILDREN);
            for _ in 1..NODE_MAX_CHILDREN + 1 {
                cache.push(None);
            }
            cache.insert(i, Some(Box::new(tree_child)));

            self.cached_children = Some(cache);
        }
    }

    pub fn remove_tree_node_child(&mut self, remove_at: usize) {
        self.disk_node.shift_children(remove_at, 1);

        let disk_children_len = self.children_len();
        let cached_children_len = if let Some(cached_children) = &self.cached_children {
            cached_children.len()
        } else {
            0
        };

        if disk_children_len < cached_children_len {
            if let Some(children) = &mut self.cached_children {
                children.remove(remove_at);
            }
        }
    }

    pub fn cache_node_overflow_child(&mut self, overflow_child: NodeOverflow, i: usize) {
        if let Some(overflow_children) = &mut self.cached_overflow_children {
            if overflow_children.len() < NODE_MAX_CHILDREN {
                overflow_children.insert(i, Some(overflow_child));
            } else {
                overflow_children.splice(i..i + 1, [Some(overflow_child)]);
            }
        } else {
            let mut cache = Vec::with_capacity(NODE_MAX_CHILDREN);
            cache.extend((0..NODE_MAX_CHILDREN).map(|_| None));
            cache.splice(i..i + 1, vec![Some(overflow_child)]);
            // cache.insert(i, Some(overflow_child));

            self.cached_overflow_children = Some(cache);
        }
    }

    pub fn cached_node_overflow_child(&self, i: usize) -> Option<&NodeOverflow> {
        if let Some(overflow_children) = &self.cached_overflow_children {
            if let Some(Some(overflow_child)) = overflow_children.get(i) {
                return Some(overflow_child);
            }
        }
        None
    }

    pub fn take_cached_node_overflow_child(&mut self, i: usize) -> Option<NodeOverflow> {
        if let Some(overflow_children) = &mut self.cached_overflow_children {
            if let Some(overflow_child) = overflow_children.remove(i) {
                overflow_children.insert(i, None);
                return Some(overflow_child);
            }
        }
        None
    }

    pub fn remove_node_overflow_child(&mut self, i: usize) {
        if let Some(overflow_children) = &mut self.cached_overflow_children {
            overflow_children.remove(i);
            overflow_children.insert(i, None);
        }
    }

    pub fn prepend_merge(&mut self, merge_with_node: &mut TreeNode) {
        Node::prepend_merge(&mut self.disk_node, &mut merge_with_node.disk_node);

        if let Some(merge_with_children) = &mut merge_with_node.cached_children {
            for (i, child) in merge_with_children.drain(..).enumerate() {
                if let Some(child) = child {
                    self.cache_tree_node_child(*child, i);
                }
            }
        }

        if let Some(overflow_children) = &mut merge_with_node.cached_overflow_children {
            for (i, overflow_child) in overflow_children.drain(..).enumerate() {
                if let Some(overflow_child) = overflow_child {
                    self.cache_node_overflow_child(overflow_child, i);
                }
            }
        }
    }

    pub fn append_merge(&mut self, merge_with_node: &mut TreeNode) {
        Node::append_merge(&mut self.disk_node, &mut merge_with_node.disk_node);

        let merge_to_len = if let Some(cached_children) = &self.cached_children {
            cached_children.len()
        } else {
            0
        };

        if let Some(merge_with_children) = &mut merge_with_node.cached_children {
            let mut j = merge_to_len;
            for child in merge_with_children.drain(..) {
                if let Some(child) = child {
                    self.cache_tree_node_child(*child, j);
                    j += 1;
                }
            }
        }

        if let Some(overflow_children) = &mut merge_with_node.cached_overflow_children {
            let mut j = merge_to_len;
            for overflow_child in overflow_children.drain(..) {
                if let Some(overflow_child) = overflow_child {
                    self.cache_node_overflow_child(overflow_child, j);
                    j += 1;
                }
            }
        }
    }

    pub fn take_node_and_split(mut self, split_keys_at: usize, split_children_at: usize) -> (TreeNode, TreeNode) {
        let (left_data, right_data) = Node::from_split_new(self.disk_node, split_keys_at, split_children_at);

        let right_children = if let Some(children) = &mut self.cached_children {
            Some(children.split_off(split_children_at))
        } else {
            None
        };

        let (left_overflow, right_overflow) = if let Some(overflow_children) = self.cached_overflow_children.take() {
            let (left_overflow, right_overflow) = TreeNode::split_overflow_children_at(overflow_children, split_keys_at);

            (Some(left_overflow), Some(right_overflow))
        } else {
            (None, None)
        }; 

        (
            TreeNode { disk_node: left_data, cached_children: self.cached_children.take(), cached_overflow_children: left_overflow },
            TreeNode { disk_node: right_data, cached_children: right_children, cached_overflow_children: right_overflow }
        )
    }

    pub fn split_off_left_node_right_node(&mut self) -> (TreeNode, TreeNode) {
        let mid = self.disk_node.num_keys() / 2;
        let left_data = self.disk_node.from_split_range(0, mid);
        let right_data = self.disk_node.from_split_range(1, self.disk_node.num_keys() - 1);

        let right_children = if let Some(children) = &mut self.cached_children {
            if children.len() >= NODE_MIN_CHILDREN {
                Some(children.split_off(NODE_MIN_CHILDREN))
            } else {
                None
            }
        } else {
            None
        };

        let (left_overflow, right_overflow) = if let Some(overflow_children) = self.cached_overflow_children.take() {
            let (left_overflow, right_overflow) = TreeNode::split_overflow_children_at(overflow_children, NODE_MIN_CHILDREN);
            (Some(left_overflow), Some(right_overflow))
        } else {
            (None, None)
        };

        let left_node = TreeNode { 
            disk_node: left_data, 
            cached_children: self.cached_children.take(), 
            cached_overflow_children: left_overflow 
        };
        let right_node = TreeNode { 
            disk_node: right_data, 
            cached_children: right_children, 
            cached_overflow_children: right_overflow 
        };

        (left_node, right_node)
    }

    pub fn split_node_at_midpoint(&mut self) -> (TreeNode, TreeNode) {
        let (left_node, right_node) = self.disk_node.from_cleave();
        
        let right_children = if let Some(children) = &mut self.cached_children {
            if children.len() >= NODE_MIN_CHILDREN {
                let right_children = children.split_off(NODE_MIN_CHILDREN);
                Some(right_children)
            } else {
                None
            }
        } else {
            None
        };
        let (left_overflow, right_overflow) = if let Some(overflow_children) = self.cached_overflow_children.take() {
            let (left_overflow, right_overflow) = TreeNode::split_overflow_children_at(overflow_children, NODE_MIN_CHILDREN);
            (Some(left_overflow), Some(right_overflow))
        } else {
            (None, None)
        };

        let left_node = TreeNode { 
            disk_node: left_node, 
            cached_children: self.cached_children.take(), 
            cached_overflow_children: left_overflow 
        };
        let right_node = TreeNode { 
            disk_node: right_node, 
            cached_children: right_children, 
            cached_overflow_children: right_overflow 
        };

        (left_node, right_node)
    }

    // TODO: how can I improve this overall (naming, logic, ...)
    // pub fn split_node_at(&mut self, split_at: usize) -> (TreeNode, TreeNode) {
    //     let left_data = if split_at <= 0 {
    //         self.disk_node.from_split_range(0, 1)
    //     } else {
    //         self.disk_node.from_split_range(0, 2)
    //     };

    //     let right_data = if split_at <= 0 {
    //         self.disk_node.from_split_range(1, 2)
    //     } else {
    //         self.disk_node.from_split_range(1, 1)
    //     };
    //     println!("{:?}", self.disk_node);
    //     let right_children = if let Some(children) = &mut self.cached_children {
    //         if children.len() >= split_at {
    //             Some(children.split_off(split_at))
    //         } else {
    //             None
    //         }
    //     } else {
    //         None
    //     };

    //     let (left_overflow, right_overflow) = if let Some(overflow_children) = self.cached_overflow_children.take() {
    //         let (left_overflow, right_overflow) = TreeNode::split_overflow_children_at(overflow_children, split_at);
    //         (Some(left_overflow), Some(right_overflow))
    //     } else {
    //         (None, None)
    //     };

    //     let left_node = TreeNode { 
    //         disk_node: left_data, 
    //         cached_children: self.cached_children.take(), 
    //         cached_overflow_children: left_overflow 
    //     };
    //     let right_node = TreeNode { 
    //         disk_node: right_data, 
    //         cached_children: right_children, 
    //         cached_overflow_children: right_overflow 
    //     };

    //     (left_node, right_node)
    // }

    pub fn append_key(&mut self, key: u32, remote_key_size: usize) {
        self.disk_node.append_key(key, remote_key_size);
    }

    pub fn prepend_key(&mut self, key: u32, remote_key_size: usize) {
        self.disk_node.prepend_key(key, remote_key_size);
    }

    pub fn splice_key(&mut self, key: u32, remote_key_size: usize, i: usize) {
        self.disk_node.splice_key(key, remote_key_size, i);
    }

    pub fn remove_key(&mut self, remove_at: usize) {
        self.disk_node.shift_keys(remove_at, 1);
    }

    pub fn take_key(&mut self, i: usize) -> (u32, usize) {
        self.disk_node.take_key(i)
    }

    pub fn key(&self, i: usize) -> (u32, usize) {
        let key_value =  self.disk_node.key_value_as_u32(i);
        let remote_key_item_size = self.disk_node.key_remote_item_size(i);

        (key_value, remote_key_item_size)
    }

    pub fn key_value(&self, i: usize) -> u32 {
        self.disk_node.key_value_as_u32(i)
    }

    pub fn keys_len(&self) -> usize {
        self.disk_node.num_keys()
    }

    pub fn add_node_child(&mut self, child_offset: u32, size: usize, is_overflowing: bool) {
        self.disk_node.append_child(child_offset, size, is_overflowing);
    }

    pub fn splice_node_child(&mut self, child_offset: u32, size: usize, is_overflowing: bool, i: usize) {
        self.disk_node.splice_child(child_offset, size, is_overflowing, i,);
    }

    pub fn replace_node_child(&mut self, child_offset: u32, size: usize, is_overflowing: bool, i: usize) {
        self.disk_node.replace_child(child_offset, size, is_overflowing, i);
    }

    pub fn remove_node_child(&mut self, i: usize) {
        self.disk_node.shift_children(i, 1);
    }

    pub fn take_node_child(&mut self, i: usize) -> (u32, usize) {
        self.disk_node.take_child(i)
    }

    pub fn clear_children(&mut self) {
        self.disk_node.clear_children();
    }

    pub fn child(&self, i: usize) -> Option<(u32, usize, bool)> {
        let child_offset = self.disk_node.child_offset_as_u32(i);
        let child_size = self.disk_node.child_size_as_usize(i);
        let child_is_overflowing = self.disk_node.child_is_overflowing(i);

        if child_offset > 0 && child_size > 0 {
            Some((child_offset, child_size, child_is_overflowing))
        } else {    
            None
        }
    }

    pub fn child_offset_child_size(&self, i: usize) -> Option<(u32, usize)> {
        let child_offset = self.disk_node.child_offset_as_u32(i);
        let child_size = self.disk_node.child_size_as_usize(i);

        if child_offset > 0 && child_size > 0 {
            Some((child_offset, child_size))
        } else {    
            None
        }
    }

    pub fn child_offset(&self, i: usize) -> Option<u32> {
        let child_offset = self.disk_node.child_offset_as_u32(i);

        if child_offset > 0 {
            Some(child_offset)
        } else {
            None
        }
    }

    pub fn child_size(&self, i: usize) -> Option<usize> {
        let child_size = self.disk_node.child_size_as_usize(i);

        if child_size > 0 {
            Some(child_size)
        } else {
            None
        }
    }

    // pub fn is_overflowing(&self) -> bool {
    //     self.disk_node
    // }

    pub fn child_is_overflowing(&self, i: usize) -> bool {
        self.disk_node.child_is_overflowing(i)
    }

    // pub fn children_len(&self) -> usize {
    //     // TODO: why am I checking the length of the cached children?
    //     if let Some(children) = &self.cached_children {
    //         children.len()
    //     } else {
    //         0
    //     }
    // }

    pub fn children_len(&self) -> usize {
        self.disk_node.num_children()
    }

    pub fn add_node_overflow_child(&mut self, child_offset: u32, size: usize, i: usize ) {   
        if let Some (overflow_children) = &mut self.cached_overflow_children {
            // if let Some(overflow_child) = overflow_children.get_mut(i) {
            //     if let Some(overflow_child) = overflow_child {
            //         overflow_child.add_item(child_offset, size);
            //     } else {
            //         overflow_children[i] = Some(NodeOverflow::new(child_offset, size));
            //     }
            // } else {
            //     overflow_children.push(Some(NodeOverflow::new(child_offset, size)));
            // }

            if let Some(overflow_child) = &mut overflow_children[i] {
                overflow_child.add_item(child_offset, size);
            } else {
                overflow_children[i] = Some(NodeOverflow::new(child_offset, size));
            }
        } else {
            let mut cache = Vec::with_capacity(NODE_MAX_CHILDREN);
            cache.extend((0..NODE_MAX_CHILDREN).map(|_| None));
            cache.splice(i..i + 1, vec![Some(NodeOverflow::new(child_offset, size))]);

            self.cached_overflow_children = Some(cache);
        }
    }

    pub fn overflow_child(&self, i: usize) -> Option<&NodeOverflow> {
        if let Some(overflow_children) = &self.cached_overflow_children {
            if let Some(overflow_child) = overflow_children.get(i) {
                if let Some(overflow_child) = overflow_child {
                    return Some(overflow_child);
                }
            }
        }
        None
    }

    pub fn overflow_children(&self, i: usize) -> Option<&NodeOverflow> {
        if let Some(overflow_children) = &self.cached_overflow_children {
            if let Some(overflow_child) = &overflow_children[i] {
                return Some(overflow_child);
            }
        }
        None
    }

    pub fn overflow_child_len(&self, i: usize) -> usize {
        if let Some(overflow_children) = &self.cached_overflow_children {
            if let Some(overflow_child) = &overflow_children[i] {
                return overflow_child.num_items()
            }
        }
        0
    }

    pub fn overflow_children_len(&self) -> usize {
        if let Some(overflow_children) = &self.cached_overflow_children {
            if let Some(i) = overflow_children.into_iter().position(|c| c.is_none()) {
                return i; 
            }
        }
        0
    }

    pub fn left_right_children_of_index(&self, i: usize) -> (usize, usize) {
        if i < self.disk_node.num_children() - 1  {
            (i, i + 1)
        } else {
            (i - 1, i)
        }
    }

    pub fn set_node_type(&mut self, node_type: NodeType) {
        self.disk_node.set_node_type(node_type);
    }

    pub fn has_children(&self) -> bool {
        self.disk_node.num_children() > 0
    }

    pub fn is_root(&self) -> bool {
        self.disk_node.is_root()
    }

    pub fn node_type(&self) -> NodeType {
        self.disk_node.node_type()
    }

    pub fn node_index_type(&self) -> NodeIndexType {
        self.disk_node.node_index_type()
    }

    pub fn node_indexed_column(&self) -> Option<usize> {
        match self.node_index_type() {
            NodeIndexType::Primary => None,
            NodeIndexType::Secondary => Some(self.disk_node.node_indexed_column())
        }
    }

    pub fn data(&self) -> &[u8] {
        &self.disk_node.data()
    }

    pub fn size(&self) -> usize {
        self.disk_node.data().len()
    }

    pub fn overflow_data(&self, i: usize) -> Option<&[u8]> {
        if let Some(overflow_children) = &self.cached_overflow_children {
            if let Some(overflow_child) = &overflow_children[i] {
                return Some(&overflow_child.data());
            }
        }
        None
    }

    fn split_overflow_children_at(mut overflow_children: Vec<Option<NodeOverflow>>, split_at: usize) -> (Vec<Option<NodeOverflow>>, Vec<Option<NodeOverflow>>) {
        let mut right_overflow_children = overflow_children.split_off(split_at);
        right_overflow_children.extend((0..NODE_MAX_CHILDREN - right_overflow_children.len()).map(|_| None));

        let mut left_overflow_children = overflow_children;
        left_overflow_children.extend((0..NODE_MAX_CHILDREN - left_overflow_children.len()).map(|_| None));

        (left_overflow_children, right_overflow_children)        
    }
}