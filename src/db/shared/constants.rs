pub mod params {
    pub const ELEMENT_SIZE: usize = 92;
}

pub mod master {
    pub const PRIMARY_ROOT_SIZE_SIZE: usize = 4;
    pub const PRIMARY_ROOT_SIZE_OFFSET: usize = 0;
    pub const PRIMARY_ROOT_OFFSET_SIZE: usize = 4;
    pub const PRIMARY_ROOT_OFFSET_OFFSET: usize = PRIMARY_ROOT_SIZE_OFFSET + PRIMARY_ROOT_SIZE_SIZE;
    pub const SECONDARY_INDEX_LIST_SIZE_SIZE: usize = 4;
    pub const SECONDARY_INDEX_LIST_SIZE_OFFSET: usize = PRIMARY_ROOT_OFFSET_OFFSET + PRIMARY_ROOT_OFFSET_SIZE;
    pub const SECONDARY_INDEX_LIST_OFFSET_SIZE: usize = 4;
    pub const SECONDARY_INDEX_LIST_OFFSET_OFFSET: usize = SECONDARY_INDEX_LIST_SIZE_OFFSET + SECONDARY_INDEX_LIST_SIZE_SIZE;
    pub const TABLE_COLUMNS_LIST_SIZE_SIZE: usize = 4;
    pub const TABLE_COLUMNS_LIST_SIZE_OFFSET: usize = SECONDARY_INDEX_LIST_OFFSET_OFFSET + SECONDARY_INDEX_LIST_OFFSET_SIZE;
    pub const TABLE_COLUMNS_LIST_OFFSET_SIZE: usize = 4;
    pub const TABLE_COLUMNS_LIST_OFFSET_OFFSET: usize = TABLE_COLUMNS_LIST_SIZE_OFFSET + TABLE_COLUMNS_LIST_SIZE_SIZE;
    pub const FREE_LIST_NUMBER_ITEMS_SIZE: usize = 4;
    pub const FREE_LIST_NUMBER_ITEMS_OFFSET: usize = TABLE_COLUMNS_LIST_OFFSET_OFFSET + TABLE_COLUMNS_LIST_OFFSET_SIZE;
    pub const FREE_LIST_OFFSET_SIZE: usize = 4;
    pub const FREE_LIST_OFFSET_OFFSET: usize = FREE_LIST_NUMBER_ITEMS_OFFSET + FREE_LIST_NUMBER_ITEMS_SIZE;
    pub const RECLAIM_LIST_NUMBER_ITEMS_SIZE: usize = 4;
    pub const RECLAIM_LIST_NUMBER_ITEMS_OFFSET: usize = FREE_LIST_OFFSET_OFFSET + FREE_LIST_OFFSET_SIZE;
    pub const RECLAIM_LIST_OFFSET_SIZE: usize = 4;
    pub const RECLAIM_LIST_OFFSET_OFFSET: usize = RECLAIM_LIST_NUMBER_ITEMS_OFFSET + RECLAIM_LIST_NUMBER_ITEMS_SIZE;
    pub const MASTER_SIZE: usize = PRIMARY_ROOT_SIZE_SIZE + PRIMARY_ROOT_OFFSET_SIZE + SECONDARY_INDEX_LIST_SIZE_SIZE + SECONDARY_INDEX_LIST_OFFSET_SIZE + TABLE_COLUMNS_LIST_SIZE_SIZE + TABLE_COLUMNS_LIST_OFFSET_SIZE + FREE_LIST_NUMBER_ITEMS_SIZE + FREE_LIST_OFFSET_SIZE + RECLAIM_LIST_NUMBER_ITEMS_SIZE + RECLAIM_LIST_OFFSET_SIZE;
}

pub mod secondary_index_item {
    pub const SECONDARY_INDEX_ITEM_OFFSET_SIZE: usize = 4;
    pub const SECONDARY_INDEX_ITEM_OFFSET_OFFSET: usize = 0;
    pub const SECONDARY_INDEX_ITEM_COLUMN_INDEX_SIZE: usize = 4;
    pub const SECONDARY_INDEX_ITEM_COLUMN_INDEX_OFFSET: usize = SECONDARY_INDEX_ITEM_OFFSET_OFFSET + SECONDARY_INDEX_ITEM_OFFSET_SIZE;
    pub const SECONDARY_INDEX_ITEM_SIZE: usize = SECONDARY_INDEX_ITEM_OFFSET_SIZE + SECONDARY_INDEX_ITEM_COLUMN_INDEX_SIZE;
}

pub mod secondary_index_list {
    pub const SECONDARY_INDEX_LIST_SIZE_SIZE: usize = 4;
    pub const SECONDARY_INDEX_LIST_SIZE_OFFSET: usize = 0;
    pub const SECONDARY_INDEX_LIST_NUMBER_ITEMS_SIZE: usize = 4;
    pub const SECONDARY_INDEX_LIST_NUMBER_ITEMS_OFFSET: usize = SECONDARY_INDEX_LIST_SIZE_OFFSET + SECONDARY_INDEX_LIST_SIZE_SIZE;
    pub const SECONDARY_INDEX_LIST_META_DATA_SIZE: usize = SECONDARY_INDEX_LIST_SIZE_SIZE + SECONDARY_INDEX_LIST_NUMBER_ITEMS_SIZE;
    pub const SECONDARY_INDEX_LIST_META_DATA_OFFSET: usize = 0;
}

pub mod node_key {
    pub const NODE_KEY_REMOTE_ITEM_SIZE: usize = 4;
    pub const NODE_KEY_VALUE_SIZE: usize = 4;
    pub const NODE_KEY_SIZE: usize = NODE_KEY_REMOTE_ITEM_SIZE + NODE_KEY_VALUE_SIZE;
    pub const NODE_MAX_KEYS: usize = 4;
    pub const NODE_MIN_KEYS: usize = 2;
}

pub mod node_child {
    pub const NODE_CHILD_CHILD_SIZE: usize = 4;
    pub const NODE_CHILD_OFFSET_SIZE: usize = 4;
    pub const NODE_CHILD_OVERFLOWING_SIZE: usize = 1;
    pub const NODE_CHILD_SIZE: usize = NODE_CHILD_CHILD_SIZE + NODE_CHILD_OFFSET_SIZE + NODE_CHILD_OVERFLOWING_SIZE; // 
    pub const NODE_MAX_CHILDREN: usize = 5;
    pub const NODE_MIN_CHILDREN: usize  = 3;
}

pub mod node {
    use super::{node_child::{NODE_CHILD_SIZE, NODE_MAX_CHILDREN}, node_key::{NODE_KEY_SIZE, NODE_MAX_KEYS}};

    pub const NODE_IS_ROOT_SIZE: usize = 1;
    pub const NODE_IS_ROOT_OFFSET: usize = 0;
    pub const NODE_TYPE_SIZE: usize = 1;
    pub const NODE_TYPE_OFFSET: usize = NODE_IS_ROOT_OFFSET + NODE_IS_ROOT_SIZE;
    pub const NODE_INDEX_TYPE_SIZE: usize = 1;
    pub const NODE_INDEX_TYPE_OFFSET: usize = NODE_TYPE_OFFSET + NODE_TYPE_SIZE;
    pub const NODE_INDEXED_COLUMN_SIZE: usize = 4;
    pub const NODE_INDEXED_COLUMN_OFFSET: usize = NODE_INDEX_TYPE_OFFSET + NODE_INDEX_TYPE_SIZE;
    pub const NODE_NUMBER_KEYS_SIZE: usize = 4;
    pub const NODE_NUMBER_KEYS_OFFSET: usize = NODE_INDEXED_COLUMN_OFFSET + NODE_INDEXED_COLUMN_SIZE;
    pub const NODE_KEYS_OFFSET: usize = NODE_NUMBER_KEYS_OFFSET + NODE_NUMBER_KEYS_SIZE;
    pub const NODE_KEYS_SIZE: usize = NODE_KEY_SIZE * NODE_MAX_KEYS;
    pub const NODE_NUMBER_CHILDREN_SIZE: usize = 4;
    pub const NODE_NUMBER_CHILDREN_OFFSET: usize = NODE_KEYS_OFFSET + NODE_KEYS_SIZE;
    pub const NODE_CHILDREN_SIZE: usize = NODE_CHILD_SIZE * NODE_MAX_CHILDREN as usize;
    pub const NODE_CHILDREN_OFFSET: usize = NODE_NUMBER_CHILDREN_OFFSET + NODE_NUMBER_CHILDREN_SIZE;
    pub const NODE_SIZE: usize = NODE_IS_ROOT_SIZE + NODE_TYPE_SIZE + NODE_INDEX_TYPE_SIZE + NODE_INDEXED_COLUMN_SIZE + NODE_NUMBER_KEYS_SIZE + NODE_KEYS_SIZE + NODE_NUMBER_CHILDREN_SIZE + NODE_CHILDREN_SIZE;
}

pub mod node_overflow {
    pub const NODE_OVERFLOW_ITEM_SIZE_SIZE: usize = 4;
    pub const NODE_OVERFLOW_ITEM_SIZE_OFFSET: usize = 0;
    pub const NODE_OVERFLOW_ITEM_OFFSET_SIZE: usize = 4;
    pub const NODE_OVERFLOW_ITEM_OFFSET_OFFSET: usize = NODE_OVERFLOW_ITEM_SIZE_OFFSET + NODE_OVERFLOW_ITEM_SIZE_SIZE;
    pub const NODE_OVERFLOW_ITEM_SIZE: usize = NODE_OVERFLOW_ITEM_SIZE_SIZE + NODE_OVERFLOW_ITEM_OFFSET_SIZE;

    pub const NODE_OVERFLOW_NUMBER_ITEMS_SIZE: usize = 4;
    pub const NODE_OVERFLOW_NUMBER_ITEMS_OFFSET: usize = 0;
    pub const NODE_OVERFLOW_META_DATA_SIZE: usize = NODE_OVERFLOW_NUMBER_ITEMS_SIZE;
}

pub mod cell {
    pub const CELL_DATA_SIZE: usize = 4;
    pub const CELL_DATA_SIZE_OFFSET: usize = 0;
    pub const CELL_IS_HEAD_SIZE: usize = 1;
    pub const CELL_IS_HEAD_OFFSET: usize = CELL_DATA_SIZE_OFFSET + CELL_DATA_SIZE;
    pub const CELL_DATA_TYPE_SIZE: usize = 1;
    pub const CELL_DATA_TYPE_OFFSET: usize = CELL_IS_HEAD_OFFSET + CELL_IS_HEAD_SIZE;
    pub const CELL_META_DATA_SIZE: usize = CELL_DATA_SIZE + CELL_IS_HEAD_SIZE + CELL_DATA_TYPE_SIZE;
}

pub mod row {
    use super::cell::{CELL_DATA_SIZE, CELL_IS_HEAD_SIZE, CELL_DATA_TYPE_SIZE, CELL_DATA_TYPE_OFFSET};
    pub const ROW_DATA_SIZE: usize = 4;
    pub const ROW_DATA_SIZE_OFFSET: usize = CELL_DATA_TYPE_OFFSET + CELL_DATA_TYPE_SIZE;
    pub const ROW_HEAD_CELL_SIZE: usize = CELL_DATA_SIZE + CELL_IS_HEAD_SIZE + CELL_DATA_TYPE_SIZE + ROW_DATA_SIZE;
}

pub mod column {
    pub const COLUMN_NAME_SIZE_SIZE: usize = 4;
    pub const COLUMN_NAME_SIZE_OFFSET: usize = 0;
    pub const COLUMN_TYPE_SIZE: usize = 4;
    pub const COLUMN_TYPE_OFFSET: usize = COLUMN_NAME_SIZE_OFFSET + COLUMN_NAME_SIZE_SIZE;
    pub const COLUMN_META_DATA_SIZE: usize = COLUMN_NAME_SIZE_SIZE + COLUMN_TYPE_SIZE;
}

pub mod columns {
    pub const COLUMNS_SIZE_SIZE: usize = 4;
    pub const COLUMNS_SIZE_OFFSET: usize = 0;
    pub const COLUMNS_NUMBER_COLUMNS_SIZE: usize = 4;
    pub const COLUMNS_NUMBER_COLUMNS_OFFSET: usize = COLUMNS_SIZE_OFFSET + COLUMNS_SIZE_SIZE;
    pub const COLUMNS_META_DATA_SIZE: usize = COLUMNS_SIZE_SIZE + COLUMNS_NUMBER_COLUMNS_SIZE;
}

pub mod free_list_item {
    pub const FREE_ITEM_DATA_OFFSET: usize = 4;
    pub const FREE_ITEM_DATA_OFFSET_OFFSET: usize = 0;
    pub const FREE_ITEM_DATA_SIZE: usize = 4;
    pub const FREE_ITEM_DATA_SIZE_OFFSET: usize = FREE_ITEM_DATA_OFFSET_OFFSET + FREE_ITEM_DATA_OFFSET;
    pub const FREE_ITEM_SIZE: usize = FREE_ITEM_DATA_OFFSET + FREE_ITEM_DATA_SIZE;
}