use std::io::Read;

use crate::db::{file_stystem::{file_handler::FileHandler, write_buffer::WriteBufferItem}, meta::disk_storage::{free_list::{FreeList, FreeListItem}, master::Master}, shared::constants::{self, free_list_item::FREE_ITEM_SIZE, params::ELEMENT_SIZE}};

#[derive(Debug)]
pub struct Pager {
    pub master: Master,
    file_handler: FileHandler,
    free_list: FreeList,
    write_buffers: Vec<WriteBufferItem>,
    eof_buffer: Option<usize>,
}

impl Pager {
    pub fn new() -> Pager {
        let mut file_handler: FileHandler = FileHandler::new();

        if let Some(master) = Pager::master_from_file(&mut file_handler) {
            if let Some(free_list) = Pager::free_list_from_file(&mut file_handler, &master) {
                return Pager { master, file_handler, write_buffers: vec![], free_list, eof_buffer: None };
            }

            Pager { master, file_handler, write_buffers: vec![], free_list: FreeList::new(), eof_buffer: None }
        } else {
            let master = Master::new();            
            let mut pager = Pager { master, file_handler, write_buffers: vec![], free_list: FreeList::new(), eof_buffer: None };
            pager.master_to_file();

            pager
        }
    }

    pub fn read_from_file(&mut self, offset: usize, size: usize) -> Option<Vec<u8>> {
        if let Err(_) = self.file_handler.seek_reader(offset) {
            println!("Failed to seek");
            return None;
        }

        let mut bytes: Vec<u8> = vec![0; size];
        if let Ok(bytes_read) = self.file_handler.read(&mut bytes) {
            if bytes_read > 0 {
                Some(bytes)
            } else {
                None
            }
        } else {
            None
        }
    }

    pub fn add_to_write_buffer(&mut self, bytes: &[u8], prev_offset_prev_size: Option<(u32, usize)>) -> u32 {
        if let Some((prev_offset, prev_size)) = prev_offset_prev_size {
            self.mark_free(prev_size, prev_offset);
        }

        let offset = if let Some(reclaim_offset) = self.free_list.reclaim_from_free_list(bytes.len()) {
            if let Some(write_buffer) = self.write_buffers.last_mut() {
                if write_buffer.offset() + write_buffer.size() == reclaim_offset {   
                    write_buffer.add_bytes(bytes)                 
                } else {
                    self.write_buffers.push(WriteBufferItem::new(bytes, bytes.len(), reclaim_offset));
                }
            } else {
                self.write_buffers.push(WriteBufferItem::new(bytes, bytes.len(), reclaim_offset));
            }

            reclaim_offset as u32
        } else {
            if let Some(eof_buffer) = self.eof_buffer {
                if let Some(write_buffer) = self.write_buffers.get_mut(eof_buffer) {
                    let offset = write_buffer.offset() + write_buffer.size();
                    write_buffer.add_bytes(bytes);
         
                    offset as u32
                } else {
                    println!("Failed to add to EOF buffer");
                    0
                }
            } else {
                if let Ok(cursor_offset) = self.file_handler.seek_write() {
                    self.write_buffers.push(WriteBufferItem::new(bytes, bytes.len(), cursor_offset as usize));
                    self.eof_buffer = Some(self.write_buffers.len() - 1);

                    cursor_offset
                } else {
                    println!("Failed to create EOF buffer");
                    0
                }
            }
        };

        offset
    }

    pub fn flush_write_buffer_trees(&mut self, primary_root: (u32, usize), secondary_index_list: (u32, usize)) {
        let (primary_root_offset, primary_root_size) = primary_root;
        let (secondary_index_list_offset, secondary_index_list_size) = secondary_index_list;

        for write_buffer in self.write_buffers.iter() {
            if let Err(_) = self.file_handler.seek_overwrite(write_buffer.offset()) {
                println!("Failed to write");
            }

            if let Err(_) = self.file_handler.write(&write_buffer.bytes().to_vec()) {
                println!("Failed to write");
            }
        }

        self.master.set_primary_root_offset(primary_root_offset);
        self.master.set_primary_root_size(primary_root_size as u32);
        self.master.set_secondary_index_list_offset(secondary_index_list_offset);
        self.master.set_secondary_index_list_size(secondary_index_list_size as u32);

        self.free_list.condense_free_list();
        self.free_list_to_file();
        self.master_to_file();

        self.write_buffers = vec![];
        self.eof_buffer = None;
    }

    pub fn flush_table_columns(&mut self, table_columns: (u32, usize)) {
        let (table_columns_offset, table_columns_size) = table_columns;

        for write_buffer in self.write_buffers.iter() {
            if let Err(_) = self.file_handler.seek_overwrite(write_buffer.offset()) {
                println!("Failed to write");
            }

            if let Err(_) = self.file_handler.write(&write_buffer.bytes().to_vec()) {
                println!("Failed to write");
            }
        }

        self.master.set_table_columns_offset(table_columns_offset);
        self.master.set_table_columns_size(table_columns_size as u32);

        self.free_list.condense_free_list();
        self.free_list_to_file();
        self.master_to_file();

        self.write_buffers = vec![];
        self.eof_buffer = None;
    }

    pub fn mark_free(&mut self, size: usize, offset: u32) {
        self.free_list.add_to_free_list(FreeListItem::new(offset as usize, size));
    }

    fn free_list_buffer(&self) -> Vec<u8> {
        let free_list_size = self.free_list.free_list_len();
        
        let mut free_list_buffer = vec![];
        free_list_buffer.extend((free_list_size as u32).to_le_bytes());
        free_list_buffer.extend(self.free_list.free_list_data());

        // TODO: remove hard-coded ELEMENT_SIZE
        let padding = ELEMENT_SIZE - (free_list_buffer.len() % ELEMENT_SIZE);
        if padding < ELEMENT_SIZE {
            free_list_buffer.extend(vec![0u8; padding]);
        }

        free_list_buffer
    }

    fn reclaim_list_buffer(&self) -> Vec<u8> {
        let reclaim_list_size = self.free_list.reclaim_list_len();
       
        let mut reclaim_list_buffer = vec![];
        reclaim_list_buffer.extend((reclaim_list_size as u32).to_le_bytes());
        reclaim_list_buffer.extend(self.free_list.recliam_list_data());

        // TODO: remove hard-coded ELEMENT_SIZE
        let padding = ELEMENT_SIZE - (reclaim_list_buffer.len() % ELEMENT_SIZE);
        if padding < ELEMENT_SIZE {
            reclaim_list_buffer.extend(vec![0u8; padding]);
        }

        reclaim_list_buffer
    }

    pub fn primary_root_offset_primary_root_size(&self) -> Option<(u32, usize)> {
        let primary_root_offset = self.master.primary_root_offset();
        let primary_root_size = self.master.primary_root_size();

        if primary_root_offset > 0 {
            Some((primary_root_offset, primary_root_size as usize))
        } else {
            None
        }
    }

    pub fn secondary_index_offset_secondary_index_size(&self) -> Option<(u32, usize)> {
        let secondary_index_list_offset = self.master.secondary_index_list_offset();
        let secondary_index_list_size = self.master.secondary_index_list_size();

        if secondary_index_list_offset > 0 && secondary_index_list_size > 0 {
            Some((secondary_index_list_offset, secondary_index_list_size as usize))
        } else {
            None
        }
    }


    pub fn table_columns_offset_table_columns_size(&self) -> Option<(u32, usize)> {
        let table_columns_offset = self.master.table_columns_offset();
        let table_columns_size = self.master.table_columns_size();

        if table_columns_offset > 0 && table_columns_size > 0 {
            Some((table_columns_offset, table_columns_size as usize))
        } else {
            None
        }
    }

    fn free_list_offset(&self) -> Option<u32> {
        let free_list_offset = self.master.free_list_offset();

        if free_list_offset > 0 {
            Some(free_list_offset)
        } else {
            None
        }
    }

    fn reclaim_list_offset(&self) -> Option<u32> {
        let reclaim_list_offset = self.master.reclaim_list_offset();

        if reclaim_list_offset > 0 {
            Some(reclaim_list_offset)
        } else {
            None
        }
    }
    
    fn master_to_file(&mut self) -> u32 {
        if let Err(_) = self.file_handler.seek_overwrite(0) {
            println!("Failed to seek");
        }
        if let Err(_) = self.file_handler.write(&self.master.data().to_vec()) {
            println!("Failed to write node");
        }
        0
    }

    fn free_list_to_file(&mut self) {
        if self.free_list.free_list_len() <= 0 && self.free_list.reclaim_list_len() <= 0 {
            return;
        }

        self.mark_free(self.master.free_list_len() , self.master.free_list_offset());
        self.mark_free(self.master.reclaim_list_len(), self.master.reclaim_list_offset());

        let free_list_buffer = self.free_list_buffer();
        let reclaim_list_buffer = self.reclaim_list_buffer();
        let free_list_len = free_list_buffer.len();

        let free_list_offset = if let Ok(offset) = self.file_handler.seek_write() {
            offset
        } else {
            0
        };

        if let Err(_) = self.file_handler.write( &(free_list_buffer.into_iter().chain(reclaim_list_buffer).collect())) {
            println!("Failed to write node");
        }

        self.master.set_free_list_number_items((self.free_list.free_list_len()) as u32);
        self.master.set_reclaim_list_number_items((self.free_list.reclaim_list_len()) as u32);
        self.master.set_free_list_offset(free_list_offset);
        self.master.set_reclaim_list_offset(free_list_offset + free_list_len as u32);
    }

    fn master_from_file(file_handler: &mut FileHandler) -> Option<Master> {
        if let Err(_) = file_handler.seek_reader(0) {
            println!("Failed to seek");
            return None;
        }

        let mut bytes: Vec<u8> = vec![0; constants::master::MASTER_SIZE as usize];
        if let Ok(bytes_read) = file_handler.read(&mut bytes) {
            if bytes_read > 0 { Some(Master::from_bytes(&bytes)) } else {  None }
        } else {
            return None;
        }
    }

    fn free_list_from_file(file_handler: &mut FileHandler, master: &Master) -> Option<FreeList> {
        // TODO: replace hard-coded 4
        let free_list_bytes =  if let Ok(_) = file_handler.seek_reader(master.free_list_offset() as usize + 4) {
            let mut free_list_bytes = vec![0; master.free_list_number_items() as usize * FREE_ITEM_SIZE];
            if let Ok(_) = file_handler.read(&mut free_list_bytes) {
                Some(free_list_bytes)
            } else {
                None
            }
        } else {
            println!("Failed to seek");
            None
        };

        // TODO: replace hard-coded 4
        let reclaim_list_bytes =  if let Ok(_) = file_handler.seek_reader(master.reclaim_list_offset() as usize + 4) {
            let mut reclaim_list_bytes = vec![0; master.reclaim_list_number_items() as usize * FREE_ITEM_SIZE];
            if let Ok(_) = file_handler.read(&mut reclaim_list_bytes) {
                Some(reclaim_list_bytes)
            } else {
                None
            }
        } else {
            println!("Failed to seek");
            None
        };

        if let (Some(free_list_bytes), Some(reclaim_list_bytes)) = (free_list_bytes, reclaim_list_bytes) {
            return Some(FreeList::from_bytes(free_list_bytes, reclaim_list_bytes));
        }

        None
    } 

    // pub fn add_to_write_buffer(&mut self, bytes: &[u8], prev_offset: Option<u32>) -> u32 {
    //     if let Some(prev_offset) = prev_offset {
    //         self.mark_free(bytes.len(), prev_offset);
    //     }
        
    //     let offset = if let Some(reclaim_offset) = self.free_list.reclaim_from_free_list(bytes.len()) {
    //         if let Some(write_buffer) = self.write_buffers.last_mut() {
    //             if write_buffer.offset() + write_buffer.size() == reclaim_offset {   
    //                 write_buffer.add_bytes(bytes)                 
    //                 // write_buffer.add_item(WriteBufferItem::new(&bytes, bytes.len()));
    //             } else {
    //                 self.write_buffers.push(WriteBufferItem::new(bytes, bytes.len(), reclaim_offset));
    //                 // self.write_buffers.push(
    //                 //     WriteBuffer::from_write_buffer_item(WriteBufferItem::new(&bytes, bytes.len()), reclaim_offset)
    //                 // );
    //             }
    //         } else {
    //             self.write_buffers.push(WriteBufferItem::new(bytes, bytes.len(), reclaim_offset));
    //             // self.write_buffers.push(
    //             //     WriteBuffer::from_write_buffer_item(WriteBufferItem::new(&bytes, bytes.len()), reclaim_offset)
    //             // );
    //         }

    //         reclaim_offset as u32
    //     } else {
    //         if let Some(eof_buffer) = self.eof_buffer {
    //             if let Some(write_buffer) = self.write_buffers.get_mut(eof_buffer) {
    //                 let offset = write_buffer.offset() + write_buffer.size();
    //                 write_buffer.add_bytes(bytes);
    //                 // write_buffer.add_item(WriteBufferItem::new(&bytes, bytes.len()));

    //                 offset as u32
    //             } else {
    //                 println!("Failed to add to EOF buffer");
    //                 0
    //             }
    //         } else {
    //             if let Ok(cursor_offset) = self.file_handler.seek_write() {
    //                 self.write_buffers.push(WriteBufferItem::new(bytes, bytes.len(), cursor_offset as usize));
    //                 // self.write_buffers.push(
    //                 //     WriteBuffer::from_write_buffer_item(WriteBufferItem::new(&bytes, bytes.len()), cursor_offset as usize)
    //                 // );
    //                 self.eof_buffer = Some(self.write_buffers.len() - 1);

    //                 cursor_offset
    //             } else {
    //                 println!("Failed to create EOF buffer");
    //                 0
    //             }
    //         }

    //         // if let Ok(cursor_offset) = self.file_handler.seek_write() {
    //         //     if let Some(write_buffer) = self.write_buffers.last_mut() {
    //         //         println!("WRITE BUFFER: {:?}", write_buffer);
    //         //         if write_buffer.offset() == cursor_offset as usize {

    //         //             let offset = cursor_offset + write_buffer.size() as u32;
    //         //             write_buffer.add_item(WriteBufferItem::new(&bytes, bytes.len()));

    //         //             offset
    //         //         } else {
    //         //             println!("MISTAKE?");
    //         //             self.write_buffers.push(
    //         //                 WriteBuffer::from_write_buffer_item(WriteBufferItem::new(&bytes, bytes.len()), cursor_offset as usize)
    //         //             );

    //         //             cursor_offset
    //         //         }
    //         //     } else {
    //         //         self.write_buffers.push(
    //         //             WriteBuffer::from_write_buffer_item(WriteBufferItem::new(&bytes, bytes.len()), cursor_offset as usize)
    //         //         );

    //         //         cursor_offset
    //         //     }
    //         // } else {
    //         //     self.write_buffers.push(
    //         //         WriteBuffer::from_write_buffer_item(WriteBufferItem::new(&bytes, bytes.len()), 0)
    //         //     );

    //         //     0
    //         // }
    //     };

    //     offset
    // }
}