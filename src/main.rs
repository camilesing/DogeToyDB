mod csv_operation;
mod sql_parse;
mod rocksdb_operation;

use crate::sql_parse::{handle};
use std::{error::Error,};
use crate::rocksdb_operation::create_default_db;

const PATH: &str = "/Users/camile/Work/Rust/DogeToyDB/";

fn main() {
    create_default_db().expect("create default db error ");

    let create_table = "create table test2 (id int NOT NULL PRIMARY KEY, name varchar, sex varchar, comment varchar)";
    let create_result = handle(create_table);
    if create_result.is_err() {
        panic!(
            "parse create table sql error : {:?}",
            create_result.err()
        )
    }
 
    let insert_table = "insert into test2 (id, name, sex, comment)  values (0, 'doge', 'man', 'write-db!');";
    let insert_result = handle(insert_table);
    if insert_result.is_err() {
        panic!(
            "parse insert table sql error : {:?}",
            insert_result.err()
        )
    }
 
    let query_table = "select id, name, sex, comment  from test2 limit 10;";
    let query_result = handle(query_table);
    if query_result.is_err() {
        panic!(
            "parse query table sql error : {:?}",
            query_result.err()
        )
    }
  
    println!("{:?}", query_result.unwrap())
}
