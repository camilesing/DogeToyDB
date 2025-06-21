mod csv_operation;
mod sql_parse;

use crate::sql_parse::{parse_create_table, parse_insert, parse_select};

use crate::csv_operation::{create_file, read_data, write_data};
use std::panic::panic_any;
use std::{error::Error, fs, io, process};

const PATH: &str = "/Users/camile/Work/Rust/DogeToyDB/";

fn main() {
    let create_table = "create table test2 (name varchar, sex varchar, comment varchar)";
    let parse_create_result = parse_create_table(create_table);
    if (parse_create_result.is_err()) {
        panic!(
            "parse create table  sql error : {:?}",
            parse_create_result.err()
        )
    }
    let create_node = parse_create_result.unwrap();
    let create_result = create_file(create_node.table_name, create_node.field_list);
    if create_result.is_err() {
        panic!("create table error : {:?}", create_result.err())
    }
    let insert_table = "insert into test2 values ('doge', 'man', 'write-db!');";
    let parse_insert_result = parse_insert(insert_table);
    if parse_insert_result.is_err() {
        panic!(
            "parse insert table sql error : {:?}",
            parse_insert_result.err()
        )
    }
    let insert_result = write_data(parse_insert_result.unwrap());
    if insert_result.is_err() {
        panic!("insert table error : {:?}", insert_result.err())
    }
    let query_table = "select name , sex from test2 limit 10;";
    let parse_query_result = parse_select(query_table);
    if parse_query_result.is_err() {
        panic!(
            "parse query table sql error : {:?}",
            parse_query_result.err()
        )
    }
    let query_result = read_data(parse_query_result.unwrap());
    if query_result.is_err() {
        panic!("query table error : {:?}", query_result.err())
    }
    println!("{:?}", query_result.unwrap())
}
