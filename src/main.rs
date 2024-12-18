mod db;
use db::{db::PocketDB, shared::enums::ColumnType};
use crate::db::shared::enums::DataType;

fn main() {
  let mut db = PocketDB::new();
//   db.add_indexed_column("firstname", ColumnType::Text)
//     .add_column("lastname", ColumnType::Text) 
//     .add_column("title", ColumnType::Text)
//     .add_column("company", ColumnType::Text)
//     .add_column("years", ColumnType::Text)
//     .add_column("ismarried", ColumnType::Bool);

//     db.insert(1, PocketDB::row(
//       1,
//       vec![
//             PocketDB::cell(DataType::Text(String::from("Sammie"))),
//             PocketDB::cell(DataType::Text(String::from("Lejeune"))),
//             PocketDB::cell(DataType::Text(String::from("Software engineer"))),
//             PocketDB::cell(DataType::Text(String::from("Vanguard"))),
//             PocketDB::cell(DataType::Text(String::from("5"))),
//             PocketDB::cell(DataType::Bool(false))
//         ]
//     ));
//     db.insert(2, PocketDB::row(
//         2,
//         vec![
//             PocketDB::cell(DataType::Text(String::from("Alex"))),
//             PocketDB::cell(DataType::Text(String::from("Smith"))),
//             PocketDB::cell(DataType::Text(String::from("Software engineer"))),
//             PocketDB::cell(DataType::Text(String::from("Vanguard"))),
//             PocketDB::cell(DataType::Text(String::from("5"))),
//             PocketDB::cell(DataType::Bool(false))
//         ]
//     ));
//     db.insert(3, PocketDB::row(
//         3,
//         vec![
//             PocketDB::cell(DataType::Text(String::from("Jamie"))),
//             PocketDB::cell(DataType::Text(String::from("Doe"))),
//             PocketDB::cell(DataType::Text(String::from("Software engineer"))),
//             PocketDB::cell(DataType::Text(String::from("Vanguard"))),
//             PocketDB::cell(DataType::Text(String::from("5"))),
//             PocketDB::cell(DataType::Bool(false))
//         ]
//     ));
//     db.insert(4, PocketDB::row(
//         4,
//         vec![
//             PocketDB::cell(DataType::Text(String::from("Chris"))),
//             PocketDB::cell(DataType::Text(String::from("Johnson"))),
//             PocketDB::cell(DataType::Text(String::from("Software engineer"))),
//             PocketDB::cell(DataType::Text(String::from("Vanguard"))),
//             PocketDB::cell(DataType::Text(String::from("5"))),
//             PocketDB::cell(DataType::Bool(false))
//         ]
//     ));
//     db.insert(5, PocketDB::row(
//         5,
//         vec![
//             PocketDB::cell(DataType::Text(String::from("Taylor"))),
//             PocketDB::cell(DataType::Text(String::from("Williams"))),
//             PocketDB::cell(DataType::Text(String::from("Software engineer"))),
//             PocketDB::cell(DataType::Text(String::from("Vanguard"))),
//             PocketDB::cell(DataType::Text(String::from("5"))),
//             PocketDB::cell(DataType::Bool(false))
//         ]
//     ));
//     db.insert(6, PocketDB::row(
//         6,
//         vec![
//             PocketDB::cell(DataType::Text(String::from("Jordan"))),
//             PocketDB::cell(DataType::Text(String::from("Brown"))),
//             PocketDB::cell(DataType::Text(String::from("Software engineer"))),
//             PocketDB::cell(DataType::Text(String::from("Vanguard"))),
//             PocketDB::cell(DataType::Text(String::from("5"))),
//             PocketDB::cell(DataType::Bool(false))
//         ]
//     ));
//     db.insert(7, PocketDB::row(
//         7,
//         vec![
//             PocketDB::cell(DataType::Text(String::from("Casey"))),
//             PocketDB::cell(DataType::Text(String::from("Davis"))),
//             PocketDB::cell(DataType::Text(String::from("Software engineer"))),
//             PocketDB::cell(DataType::Text(String::from("Vanguard"))),
//             PocketDB::cell(DataType::Text(String::from("5"))),
//             PocketDB::cell(DataType::Bool(false))
//         ]
//     ));
//     db.insert(8, PocketDB::row(
//         8,
//         vec![
//             PocketDB::cell(DataType::Text(String::from("Drew"))),
//             PocketDB::cell(DataType::Text(String::from("Martinez"))),
//             PocketDB::cell(DataType::Text(String::from("Software engineer"))),
//             PocketDB::cell(DataType::Text(String::from("Vanguard"))),
//             PocketDB::cell(DataType::Text(String::from("5"))),
//             PocketDB::cell(DataType::Bool(false))
//         ]
//     ));
//     db.insert(9, PocketDB::row(
//         9,
//         vec![
//             PocketDB::cell(DataType::Text(String::from("Morgan"))),
//             PocketDB::cell(DataType::Text(String::from("Garcia"))),
//             PocketDB::cell(DataType::Text(String::from("Software engineer"))),
//             PocketDB::cell(DataType::Text(String::from("Vanguard"))),
//             PocketDB::cell(DataType::Text(String::from("5"))),
//             PocketDB::cell(DataType::Bool(false))
//         ]
//     ));
//     db.insert(10, PocketDB::row(
//         10,
//         vec![
//             PocketDB::cell(DataType::Text(String::from("Casey"))),
//             PocketDB::cell(DataType::Text(String::from("Miller"))),
//             PocketDB::cell(DataType::Text(String::from("Software engineer"))),
//             PocketDB::cell(DataType::Text(String::from("Vanguard"))),
//             PocketDB::cell(DataType::Text(String::from("5"))),
//             PocketDB::cell(DataType::Bool(false))
//         ]
//     ));
//     db.insert(11, PocketDB::row(
//         11,
//         vec![
//             PocketDB::cell(DataType::Text(String::from("Riley"))),
//             PocketDB::cell(DataType::Text(String::from("Wilson"))),
//             PocketDB::cell(DataType::Text(String::from("Software engineer"))),
//             PocketDB::cell(DataType::Text(String::from("Vanguard"))),
//             PocketDB::cell(DataType::Text(String::from("5"))),
//             PocketDB::cell(DataType::Bool(false))
//         ]
//     ));
//     db.insert(12, PocketDB::row(
//         12,
//         vec![
//             PocketDB::cell(DataType::Text(String::from("Reese"))),
//             PocketDB::cell(DataType::Text(String::from("Moore"))),
//             PocketDB::cell(DataType::Text(String::from("Software engineer"))),
//             PocketDB::cell(DataType::Text(String::from("Vanguard"))),
//             PocketDB::cell(DataType::Text(String::from("5"))),
//             PocketDB::cell(DataType::Bool(false))
//         ]
//     ));
//     db.insert(13, PocketDB::row(
//         13,
//         vec![
//             PocketDB::cell(DataType::Text(String::from("Parker"))),
//             PocketDB::cell(DataType::Text(String::from("Taylor"))),
//             PocketDB::cell(DataType::Text(String::from("Software engineer"))),
//             PocketDB::cell(DataType::Text(String::from("Vanguard"))),
//             PocketDB::cell(DataType::Text(String::from("5"))),
//             PocketDB::cell(DataType::Bool(false))
//         ]
//     ));
//     db.insert(14, PocketDB::row(
//         14,
//         vec![
//             PocketDB::cell(DataType::Text(String::from("Quinn"))),
//             PocketDB::cell(DataType::Text(String::from("Anderson"))),
//             PocketDB::cell(DataType::Text(String::from("Software engineer"))),
//             PocketDB::cell(DataType::Text(String::from("Vanguard"))),
//             PocketDB::cell(DataType::Text(String::from("5"))),
//             PocketDB::cell(DataType::Bool(false))
//         ]
//     ));
//     db.insert(15, PocketDB::row(
//         15,
//         vec![
//             PocketDB::cell(DataType::Text(String::from("Avery"))),
//             PocketDB::cell(DataType::Text(String::from("Thomas"))),
//             PocketDB::cell(DataType::Text(String::from("Software engineer"))),
//             PocketDB::cell(DataType::Text(String::from("Vanguard"))),
//             PocketDB::cell(DataType::Text(String::from("5"))),
//             PocketDB::cell(DataType::Bool(false))
//         ]
//     ));
//     db.insert(16, PocketDB::row(
//         16,
//         vec![
//             PocketDB::cell(DataType::Text(String::from("Skyler"))),
//             PocketDB::cell(DataType::Text(String::from("Jackson"))),
//             PocketDB::cell(DataType::Text(String::from("Software engineer"))),
//             PocketDB::cell(DataType::Text(String::from("Vanguard"))),
//             PocketDB::cell(DataType::Text(String::from("5"))),
//             PocketDB::cell(DataType::Bool(false))
//         ]
//     ));
//     db.insert(17, PocketDB::row(
//         17,
//         vec![
//             PocketDB::cell(DataType::Text(String::from("Emerson"))),
//             PocketDB::cell(DataType::Text(String::from("White"))),
//             PocketDB::cell(DataType::Text(String::from("Software engineer"))),
//             PocketDB::cell(DataType::Text(String::from("Vanguard"))),
//             PocketDB::cell(DataType::Text(String::from("5"))),
//             PocketDB::cell(DataType::Bool(false))
//         ]
//     ));
//     db.insert(18, PocketDB::row(
//       18,
//       vec![
//           PocketDB::cell(DataType::Text(String::from("Emerson"))),
//           PocketDB::cell(DataType::Text(String::from("White"))),
//           PocketDB::cell(DataType::Text(String::from("Software engineer"))),
//           PocketDB::cell(DataType::Text(String::from("Vanguard"))),
//           PocketDB::cell(DataType::Text(String::from("5"))),
//           PocketDB::cell(DataType::Bool(false))
//       ]
//   ));
//   db.insert(19, PocketDB::row(
//     19,
//     vec![
//         PocketDB::cell(DataType::Text(String::from("Emerson"))),
//         PocketDB::cell(DataType::Text(String::from("White"))),
//         PocketDB::cell(DataType::Text(String::from("Software engineer"))),
//         PocketDB::cell(DataType::Text(String::from("Vanguard"))),
//         PocketDB::cell(DataType::Text(String::from("5"))),
//         PocketDB::cell(DataType::Bool(false))
//     ]
// ));
//     db.insert(20, PocketDB::row(
//       20,
//       vec![
//           PocketDB::cell(DataType::Text(String::from("Reese"))),
//           PocketDB::cell(DataType::Text(String::from("Moore"))),
//           PocketDB::cell(DataType::Text(String::from("Software engineer"))),
//           PocketDB::cell(DataType::Text(String::from("Vanguard"))),
//           PocketDB::cell(DataType::Text(String::from("5"))),
//           PocketDB::cell(DataType::Bool(false))
//       ]
//     ));
//     db.insert(21, PocketDB::row(
//       21,
//       vec![
//           PocketDB::cell(DataType::Text(String::from("Parker"))),
//           PocketDB::cell(DataType::Text(String::from("Taylor"))),
//           PocketDB::cell(DataType::Text(String::from("Software engineer"))),
//           PocketDB::cell(DataType::Text(String::from("Vanguard"))),
//           PocketDB::cell(DataType::Text(String::from("5"))),
//           PocketDB::cell(DataType::Bool(false))
//       ]
//     ));
//   db.insert(22, PocketDB::row(
//     22,
//     vec![
//         PocketDB::cell(DataType::Text(String::from("Tara"))),
//         PocketDB::cell(DataType::Text(String::from("Feeley"))),
//         PocketDB::cell(DataType::Text(String::from("Software engineer"))),
//         PocketDB::cell(DataType::Text(String::from("Vanguard"))),
//         PocketDB::cell(DataType::Text(String::from("5"))),
//         PocketDB::cell(DataType::Bool(false))
//     ]
//   ));
//   db.insert(23, PocketDB::row(
//     23,
//     vec![
//         PocketDB::cell(DataType::Text(String::from("Bob"))),
//         PocketDB::cell(DataType::Text(String::from("Parry"))),
//         PocketDB::cell(DataType::Text(String::from("Software engineer"))),
//         PocketDB::cell(DataType::Text(String::from("Vanguard"))),
//         PocketDB::cell(DataType::Text(String::from("5"))),
//         PocketDB::cell(DataType::Bool(false))
//     ]
//   ));
//   db.insert(24, PocketDB::row(
//     24,
//     vec![
//         PocketDB::cell(DataType::Text(String::from("Pierce"))),
//         PocketDB::cell(DataType::Text(String::from("Mulligan"))),
//         PocketDB::cell(DataType::Text(String::from("Software engineer"))),
//         PocketDB::cell(DataType::Text(String::from("Vanguard"))),
//         PocketDB::cell(DataType::Text(String::from("5"))),
//         PocketDB::cell(DataType::Bool(false))
//     ]
//   ));
//   db.insert(25, PocketDB::row(
//     25,
//     vec![
//         PocketDB::cell(DataType::Text(String::from("Adam"))),
//         PocketDB::cell(DataType::Text(String::from("Whitaker"))),
//         PocketDB::cell(DataType::Text(String::from("Software engineer"))),
//         PocketDB::cell(DataType::Text(String::from("Vanguard"))),
//         PocketDB::cell(DataType::Text(String::from("5"))),
//         PocketDB::cell(DataType::Bool(false))
//     ]
//   ));
//   db.insert(26, PocketDB::row(
//     26,
//     vec![
//         PocketDB::cell(DataType::Text(String::from("Mike"))),
//         PocketDB::cell(DataType::Text(String::from("Cebrian"))),
//         PocketDB::cell(DataType::Text(String::from("Software engineer"))),
//         PocketDB::cell(DataType::Text(String::from("Vanguard"))),
//         PocketDB::cell(DataType::Text(String::from("5"))),
//         PocketDB::cell(DataType::Bool(false))
//     ]
//   ));
//   db.insert(27, PocketDB::row(
//     27,
//     vec![
//         PocketDB::cell(DataType::Text(String::from("Justin"))),
//         PocketDB::cell(DataType::Text(String::from("Silang"))),
//         PocketDB::cell(DataType::Text(String::from("Software engineer"))),
//         PocketDB::cell(DataType::Text(String::from("Vanguard"))),
//         PocketDB::cell(DataType::Text(String::from("5"))),
//         PocketDB::cell(DataType::Bool(false))
//     ]
//   ));
//   db.insert(28, PocketDB::row(
//     28,
//     vec![
//         PocketDB::cell(DataType::Text(String::from("Maggie"))),
//         PocketDB::cell(DataType::Text(String::from("Lejeune"))),
//         PocketDB::cell(DataType::Text(String::from("Software engineer"))),
//         PocketDB::cell(DataType::Text(String::from("Vanguard"))),
//         PocketDB::cell(DataType::Text(String::from("5"))),
//         PocketDB::cell(DataType::Bool(false))
//     ]
//   ));
//   db.insert(29, PocketDB::row(
//     29,
//     vec![
//         PocketDB::cell(DataType::Text(String::from("Wynne"))),
//         PocketDB::cell(DataType::Text(String::from("Lejeune"))),
//         PocketDB::cell(DataType::Text(String::from("Software engineer"))),
//         PocketDB::cell(DataType::Text(String::from("Vanguard"))),
//         PocketDB::cell(DataType::Text(String::from("5"))),
//         PocketDB::cell(DataType::Bool(false))
//     ]
//   ));
//   db.insert(30, PocketDB::row(
//     30,
//     vec![
//         PocketDB::cell(DataType::Text(String::from("James"))),
//         PocketDB::cell(DataType::Text(String::from("Samuel"))),
//         PocketDB::cell(DataType::Text(String::from("Software engineer"))),
//         PocketDB::cell(DataType::Text(String::from("Vanguard"))),
//         PocketDB::cell(DataType::Text(String::from("5"))),
//         PocketDB::cell(DataType::Bool(false))
//     ]
//   ));
//   db.insert(31, PocketDB::row(
//     31,
//     vec![
//         PocketDB::cell(DataType::Text(String::from("Marley"))),
//         PocketDB::cell(DataType::Text(String::from("Dog"))),
//         PocketDB::cell(DataType::Text(String::from("Software engineer"))),
//         PocketDB::cell(DataType::Text(String::from("Vanguard"))),
//         PocketDB::cell(DataType::Text(String::from("5"))),
//         PocketDB::cell(DataType::Bool(false))
//     ]
//   ));
//   db.insert(32, PocketDB::row(
//     32,
//     vec![
//         PocketDB::cell(DataType::Text(String::from("Dan"))),
//         PocketDB::cell(DataType::Text(String::from("Tamulonis"))),
//         PocketDB::cell(DataType::Text(String::from("Software engineer"))),
//         PocketDB::cell(DataType::Text(String::from("Vanguard"))),
//         PocketDB::cell(DataType::Text(String::from("5"))),
//         PocketDB::cell(DataType::Bool(false))
//     ]
//   ));
//   db.insert(33, PocketDB::row(
//     33,
//     vec![
//         PocketDB::cell(DataType::Text(String::from("Peter"))),
//         PocketDB::cell(DataType::Text(String::from("Parker"))),
//         PocketDB::cell(DataType::Text(String::from("Software engineer"))),
//         PocketDB::cell(DataType::Text(String::from("Vanguard"))),
//         PocketDB::cell(DataType::Text(String::from("5"))),
//         PocketDB::cell(DataType::Bool(false))
//     ]
//   ));
//   db.insert(34, PocketDB::row(
//     34,
//     vec![
//         PocketDB::cell(DataType::Text(String::from("Nancy"))),
//         PocketDB::cell(DataType::Text(String::from("Mullen"))),
//         PocketDB::cell(DataType::Text(String::from("Software engineer"))),
//         PocketDB::cell(DataType::Text(String::from("Vanguard"))),
//         PocketDB::cell(DataType::Text(String::from("5"))),
//         PocketDB::cell(DataType::Bool(false))
//     ]
//   ));
//   db.insert(35, PocketDB::row(
//     35,
//     vec![
//         PocketDB::cell(DataType::Text(String::from("Mike"))),
//         PocketDB::cell(DataType::Text(String::from("Cebrian"))),
//         PocketDB::cell(DataType::Text(String::from("Software engineer"))),
//         PocketDB::cell(DataType::Text(String::from("Vanguard"))),
//         PocketDB::cell(DataType::Text(String::from("5"))),
//         PocketDB::cell(DataType::Bool(false))
//     ]
//   ));
//   db.insert(36, PocketDB::row(
//     36,
//     vec![
//         PocketDB::cell(DataType::Text(String::from("Justin"))),
//         PocketDB::cell(DataType::Text(String::from("Silang"))),
//         PocketDB::cell(DataType::Text(String::from("Software engineer"))),
//         PocketDB::cell(DataType::Text(String::from("Vanguard"))),
//         PocketDB::cell(DataType::Text(String::from("5"))),
//         PocketDB::cell(DataType::Bool(false))
//     ]
//   ));
//   db.insert(37, PocketDB::row(
//     37,
//     vec![
//         PocketDB::cell(DataType::Text(String::from("Maggie"))),
//         PocketDB::cell(DataType::Text(String::from("Lejeune"))),
//         PocketDB::cell(DataType::Text(String::from("Software engineer"))),
//         PocketDB::cell(DataType::Text(String::from("Vanguard"))),
//         PocketDB::cell(DataType::Text(String::from("5"))),
//         PocketDB::cell(DataType::Bool(false))
//     ]
//   ));
//   db.insert(38, PocketDB::row(
//     38,
//     vec![
//         PocketDB::cell(DataType::Text(String::from("Wynne"))),
//         PocketDB::cell(DataType::Text(String::from("Lejeune"))),
//         PocketDB::cell(DataType::Text(String::from("Software engineer"))),
//         PocketDB::cell(DataType::Text(String::from("Vanguard"))),
//         PocketDB::cell(DataType::Text(String::from("5"))),
//         PocketDB::cell(DataType::Bool(false))
//     ]
//   ));
//   db.insert(39, PocketDB::row(
//     39,
//     vec![
//         PocketDB::cell(DataType::Text(String::from("James"))),
//         PocketDB::cell(DataType::Text(String::from("Samuel"))),
//         PocketDB::cell(DataType::Text(String::from("Software engineer"))),
//         PocketDB::cell(DataType::Text(String::from("Vanguard"))),
//         PocketDB::cell(DataType::Text(String::from("5"))),
//         PocketDB::cell(DataType::Bool(false))
//     ]
//   ));
//   db.insert(40, PocketDB::row(
//     40,
//     vec![
//         PocketDB::cell(DataType::Text(String::from("Marley"))),
//         PocketDB::cell(DataType::Text(String::from("Dog"))),
//         PocketDB::cell(DataType::Text(String::from("Software engineer"))),
//         PocketDB::cell(DataType::Text(String::from("Vanguard"))),
//         PocketDB::cell(DataType::Text(String::from("5"))),
//         PocketDB::cell(DataType::Bool(false))
//     ]
//   ));
//   db.insert(41, PocketDB::row(
//     41,
//     vec![
//         PocketDB::cell(DataType::Text(String::from("Dan"))),
//         PocketDB::cell(DataType::Text(String::from("Tamulonis"))),
//         PocketDB::cell(DataType::Text(String::from("Software engineer"))),
//         PocketDB::cell(DataType::Text(String::from("Vanguard"))),
//         PocketDB::cell(DataType::Text(String::from("5"))),
//         PocketDB::cell(DataType::Bool(false))
//     ]
//   ));

  for i in 1..42 {
    println!("{:?}", db.search_by_primary_index(DataType::Integer(i)));
  }
  
  println!("{:?}", db.search_by_secondary_index(DataType::Text(String::from("Maggie")), "firstname"));
  println!("{:?}", db.search_by_secondary_index(DataType::Text(String::from("Parker")), "firstname"));
  println!("{:?}", db.search_by_secondary_index(DataType::Text(String::from("Marley")), "firstname"));
  println!("{:?}", db.search_by_secondary_index(DataType::Text(String::from("Emerson")), "firstname"));
  println!("{:?}", db.search_by_secondary_index(DataType::Text(String::from("Chris")), "firstname"));
  println!("{:?}", db.search_by_secondary_index(DataType::Text(String::from("Justin")), "firstname"));


//   db.insert(42, PocketDB::row(
//     33,
//     vec![
//         PocketDB::cell(DataType::Text(String::from("Peter"))),
//         PocketDB::cell(DataType::Text(String::from("Parker"))),
//         PocketDB::cell(DataType::Text(String::from("Software engineer"))),
//         PocketDB::cell(DataType::Text(String::from("Vanguard"))),
//         PocketDB::cell(DataType::Text(String::from("5"))),
//         PocketDB::cell(DataType::Bool(false))
//     ]
//   ));
//   db.insert(43, PocketDB::row(
//     34,
//     vec![
//         PocketDB::cell(DataType::Text(String::from("Nancy"))),
//         PocketDB::cell(DataType::Text(String::from("Mullen"))),
//         PocketDB::cell(DataType::Text(String::from("Software engineer"))),
//         PocketDB::cell(DataType::Text(String::from("Vanguard"))),
//         PocketDB::cell(DataType::Text(String::from("5"))),
//         PocketDB::cell(DataType::Bool(false))
//     ]
//   ));

//   for i in 0..2000 {
//     db.insert(i, PocketDB::row(
//         i,
//         vec![
//             PocketDB::cell(DataType::Text(String::from("Nancy"))),
//             PocketDB::cell(DataType::Text(String::from("Mullen"))),
//             PocketDB::cell(DataType::Text(String::from("Software engineer"))),
//             PocketDB::cell(DataType::Text(String::from("Vanguard"))),
//             PocketDB::cell(DataType::Text(String::from("5"))),
//             PocketDB::cell(DataType::Bool(false))
//         ]
//       ));
//   }

//   db.delete_by_secondary_index(DataType::Text(String::from("Cebrian")), "lastname");
    // println!("{:?}", db.search_by_primary_index(DataType::Integer(244)));
    
/*
> NORMALIZE ALL DATA BEING SAVED: -> this will improve the garbage collection
  - secondary_index_list
  - table_columns

TODO:
    X > how does search work?
    X    - in the context of an internal node
    X > how does delete work?
        - delete the key + row (key might be on an internal node, I would need to go down to the leaf node first and delete the row, I think)
        - I need to be able to add this stuff to the free list as rows get deleted or ndoes get no longeer poitned at
        - I need to be able to check the primary key once I get down to the level of the row to make sure I'm deleting the correct row
    X > does inserting out of order work?
    > how do I create keys for the db?
  5 > do some refactoring, break up some of these giant functions
  X > go back over what the structure of a row
  X > *** TODO NEXT ***: how do I write rows to a file
  X > come up with a better way to buffer the write into a single big write
        X  - I need to figure out garbage collection is going to work...
            ** Garbage collection will be handled w/ a free list + slab allocation **
        X - when adding a node to the write buffer, I need to optionally say whether it has a previous pointer
        X - when I do have a previous pointer, I need to add it to the free list
        X - when a new item is added to the free list, I need to be able to see if there are any adjacent free spaces so I can create
            larger blocks of contiguous memory
        X - when I'm returning new pointers after adding a node to the write buffer, I need to first check if there's space on the free list?
        X - need to re-work master to contain the free list and the reclaim list (should both be linked lists, and each node ofthe list points to the
            next element in the list?) 
    > come up with a cli interface for interacting w/ the db
    > come up with a page system(?)
  X > replace node_from_file() and row_from_file() w/ new method
    > search needs to be able to check that the row key matches the key being searched for
  X > evaluate the constants (types -- all should be usize? -- and structure), and how they're imported
    > evaluate all imports and remove unused imports
  X > replace row from file w/ read_from_file
  X > when deleting, I don't think I'm adding the offset to the deleted row to the write buffer
  X  > *** need to be able to write and read dynamic sized rows from file ***
        X - this might require some re-archetecting... (PAINFUL re-archetecting)
        X - I think the most efficient way to do this would be to store the size of the child pointer in the parent node...
        X - if I don't store the size of the child node, then when I go to read I don't know how much to read
  X > implement deref on node
  X > make sure all uses of bytes_to_u32 are coming from the shared director (reomve any use from node)
  X > *** ADD SUPPORT FOR SECONDARY INDEXES ***
       X - need to add a new flag to node (is_primary)
       X - secondary keys will be pointers to the row where the value is stored (so if a secondary indemx is a name (string), it would point to the row where the data
          is located )
       X - MAJOR UPDATE AHEAD:
       X - treenodes will only contain pointers to treenodes (STRIP OUT ALL TREENODECHILD STUFF)
       X - we're going to have a new table file which will be a hashtable of rows (key is the offset to the row, value is the row)
       X - leafnodes won't contain in-memory pointers to rows -> once I get to a leaf node, I need to look it up in the hashtable

  NEW DESIGN:
    1) going to have a db struct, this will contain a vector of btree's (for primary and secondary indexes), the pager, and the table
    2) When an insertion happens we FIRST add the row to the write_buffer in the pager
    3) Instead of passing the row to the b_tree, we pass the row_offset we get from the pager
      - this means rows will no longer be children of tree_nodes
      - the btree is ONLY responsible for finding where to put the new key and row_offset
    4) When doing btree operations, we will also pass a mutable reference to the table (this way we can cache any found rows -- particularly for secondary index trees)

    The new insertion signature will look something like this:
    insert(&mut self, key: u32, row_offset: u32, pager: &mut pager, table: &mut Table) {}
  */
}

/* 
xxxxxxxxxxxxxxxxxxxxxx
Logging scripts: 
xxxxxxxxxxxxxxxxxxxxxx

inset() {}
// *********************************
let (row_offset, _)= row_meta_data;
if let Some(row) = table.row(row_offset) {
    if let Some(indexed_col) = root.node_indexed_column() {
        println!("INSERTING: {:?}", row.cells()[indexed_col].to_typed_data());
    } 
}
println!("INSERTING: {}", key);
// *********************************


insert_internal() -> under is_root {}
-----------------------------------------------------------------------------------------------------------------------------
*****************************************************************************************************************************
-----------------------------------------------------------------------------------------------------------------------------
println!("LEFT CHILD: ");
for i in 0..left_child.keys_len() {
    let (row_offset, _) = left_child.key(i);
    if let Some(row) = table.row(row_offset) {
        // if let Some(indexed_col) = left_child.node_indexed_column() {
        //     println!("{:?}", row.cells()[indexed_col].to_typed_data());
        // }
    } else {
        println!("{:?}", left_child.key(i))
    }
}
if let Some(right_child) = &right_child {
    println!("RIGHT CHILD: ");
    for i in 0..right_child.keys_len() {
        let (row_offset, _) = right_child.key(i);
        if let Some(row) = table.row(row_offset) {
            // if let Some(indexed_col) = right_child.node_indexed_column() {
            //     println!("{:?}", row.cells()[indexed_col].to_typed_data());
            // }
        } else {
            println!("{:?}", right_child.key(i))
        }
    }
}

println!("NODE: ");
for i in 0..node.keys_len() {
    let (row_offset, _) = node.key(i);
    if let Some(row) = table.row(row_offset) {
        // if let Some(indexed_col) = node.node_indexed_column() {
        //     println!("{:?}", row.cells()[indexed_col].to_typed_data());
        // }
    } else {
        println!("{:?}", node.key(i))
    }
}
println!("LEFT: ");
for i in 0..left_node.keys_len() {
    let (row_offset, _) = left_node.key(i);
    if let Some(row) = table.row(row_offset) {
        // if let Some(indexed_col) = left_node.node_indexed_column() {
        //     println!("{:?}", row.cells()[indexed_col].to_typed_data());
        // }
    } else {
        println!("{:?}", left_node.key(i))
    }
}
// println!("LEFT CHILDREN:");
// for i in 0..left_node.children_len() {
//     println!("LEFT CHILD OF LEFT NODE: {}", i);
//     if let Some(left_child) = left_node.cached_tree_node_child(i) {
//         for j in 0..left_child.keys_len() {
//             let (row_offset, _) = left_child.key(j);
//             if let Some(row) = table.row(row_offset) {
//                 if let Some(indexed_col) = left_child.node_indexed_column() {
//                     println!("{:?}", row.cells()[indexed_col].to_typed_data());
//                 }
//             }
//         }
//     }
// }
println!("RIGHT: ");
for i in 0..right_node.keys_len() {
    let (row_offset, _) = right_node.key(i);
    if let Some(row) = table.row(row_offset) {
        // if let Some(indexed_col) = right_node.node_indexed_column() {
        //     println!("{:?}", row.cells()[indexed_col].to_typed_data());
        // }
    }  else {
        println!("{:?}", right_node.key(i))
    }
}
// println!("RIGHT CHILDREN:");
// for i in 0..right_node.children_len() {
//     println!("RIGHT CHILD OF RIGHT NODE: {}", i);
//     if let Some(right_child) = right_node.cached_tree_node_child(i) {
//         for j in 0..right_child.keys_len() {
//             let (row_offset, _) = right_child.key(j);
//             if let Some(row) = table.row(row_offset) {
//                 if let Some(indexed_col) = right_child.node_indexed_column() {
//                     println!("{:?}", row.cells()[indexed_col].to_typed_data());
//                 }
//             }
//         }
//     }
// }
// I NEED TO WORK ON INSERTING THE RETURNED RIGHT NODE IN THE PROPER POSITION
println!("{}", left_node.children_len());
println!("{}", right_node.children_len());
println!("{}", i);

for i in 0..right_node.children_len() {
    println!("RIGHT CHILD OF RIGHT NODE: {}", i);
    if let Some(right_child) = right_node.cached_tree_node_child(i) {
        for j in 0..right_child.keys_len() {
            println!("{:?}", right_child.key((j)));                                
        }
    }
}
-----------------------------------------------------------------------------------------------------------------------------
*****************************************************************************************************************************
-----------------------------------------------------------------------------------------------------------------------------

insert_leaf()
-----------------------------------------------------------------------------------------------------------------------------
*****************************************************************************************************************************
-----------------------------------------------------------------------------------------------------------------------------
println!("INSERTING INTO: ");
for i in 0..node.keys_len() {
    let (row_offset, _) = node.key(i);
    if let Some(row) = table.row(row_offset) {
        if let Some(indexed_col) = node.node_indexed_column() {
            println!("{:?}", row.cells()[indexed_col].to_typed_data());
        }
    } else {
        println!("{:?}", node.key(i))
    }
}
-----------------------------------------------------------------------------------------------------------------------------
*****************************************************************************************************************************
-----------------------------------------------------------------------------------------------------------------------------

let (key_value, _) = node.key(0);
if let Some(row) = table.row(key_value) {
    if let Some(indexed_col) = node.node_indexed_column() {
        println!("{:?}", row.cells()[indexed_col].to_typed_data());
    }
}
*/