#[derive(Debug)]
pub struct WriteBufferItem {
    bytes: Vec<u8>,
    size: usize,
    offset: usize,
}

impl WriteBufferItem {
    pub fn new(bytes: &[u8], size: usize, offset: usize) -> WriteBufferItem {
        WriteBufferItem { bytes: bytes.to_vec(), size, offset }
    }

    pub fn add_bytes(&mut self, bytes: &[u8]) {
        self.size += bytes.len();
        self.bytes.extend(bytes);
    }

    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub fn size(&self) -> usize {
        self.size
    }

    pub fn offset(&self) -> usize {
        self.offset
    }
}

// TODO: delete this
#[derive(Debug)]
pub struct WriteBuffer {
    items: Vec<WriteBufferItem>,
    size: usize,
    offset: usize,
}

impl WriteBuffer {
    pub fn new() -> WriteBuffer {
        WriteBuffer { items: vec![], size: 0, offset: 0 }
    }

    pub fn from_write_buffer_item(item: WriteBufferItem, offset: usize) -> WriteBuffer {
        let size = item.size();
        WriteBuffer { items: vec![item], size, offset }
    }

    pub fn add_item(&mut self, item: WriteBufferItem) {
        self.size += item.size();

        self.items.push(item);
    }

    pub fn remove_item(&mut self) -> Option<WriteBufferItem> {
        if let Some(last_item) = self.items.last() {
            self.size -= last_item.size;
        }

        self.items.pop()
    }

    pub fn clear_items(&mut self) {
      self.items = vec![];
      self.size = 0;
    }

    pub fn items(&self) -> &Vec<WriteBufferItem> {
        &self.items
    }

    pub fn items_mut(&mut self) -> &mut Vec<WriteBufferItem> {
        &mut self.items
    }

    pub fn root_offset(&self) -> usize {
      if let Some(root) = self.items.last() {
        self.size + self.offset - root.size()
      } else {
        0
      }
    }

    pub fn offset(&self) -> usize {
        self.offset
    }

    pub fn size(&self) -> usize {
        self.size
    }

    pub fn data(&self) -> Vec<u8> {
        self.items.iter()
            .fold(vec![], |mut data, item| {
                data.extend(item.bytes().to_vec());
                data
            })
    }
}

// use crate::db::{b_tree::node::tree_node::TreeNode, page::table::row::Row};

// #[derive(Debug)]
// pub enum WriteBufferItem<'a> {
//     TreeNode(&'a mut TreeNode),
//     Row(&'a mut Row),
// }

// #[derive(Debug)]
// pub struct WriteBuffer<'a> {
//     items: Vec<WriteBufferItem<'a>>,
//     size: usize,
// }

// impl<'a> WriteBuffer<'a> {
//     pub fn new() -> WriteBuffer<'a> {
//         WriteBuffer { items: vec![], size: 0 }
//     }

//     pub fn add_item(&mut self, item: WriteBufferItem) {
//         // self.size += item.size();
//         match item {
//             WriteBufferItem::TreeNode(node) => self.size +=  node.data().len(),
//             WriteBufferItem::Row(row) => self.size += row.data().len(),
//         }

//         self.items.push(item);
//     }

//     // pub fn remove_item(&mut self) -> Option<WriteBufferItem> {
//     //     if let Some(last_item) = self.items.last() {
//     //         self.size -= last_item.size;
//     //     }

//     //     self.items.pop()
//     // }

//     pub fn clear_items(&mut self) {
//       self.items = vec![];
//       self.size = 0;
//     }

//     pub fn items(&self) -> &Vec<WriteBufferItem> {
//         &self.items
//     }

//     pub fn items_mut(&mut self) -> &mut Vec<WriteBufferItem> {
//         &mut self.items
//     }

//     pub fn root_offset(&mut self) -> usize {
//       if let Some(root) = self.items.last() {
//                 // self.size - root.size
//         match root {
//             WriteBufferItem::TreeNode(root) =>  self.size - root.data().len(),
//             WriteBufferItem::Row(_) => 0,
//         }
//       } else {
//         0
//       }
//     }

//     pub fn size(&self) -> usize {
//         self.size
//     }
// }