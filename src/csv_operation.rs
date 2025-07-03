
use crate::PATH;
use csv::ErrorKind::Io;
use std::any::Any;
use std::error::Error;
use std::fs::File;
use std::io;
use std::ops::Deref;
use std::path::Path;

pub fn create_file(table_name: String, field_list: Vec<String>) -> Result<(), Box<dyn Error>> {
    let (schema_path_str, _) = generate_file_path(table_name);
    let file_path = Path::new(&schema_path_str);

    let mut open_file_result = File::open(&file_path);
    if (open_file_result.is_ok()) {
        return Err("File is exist".into());
    }

    let write_result = csv::Writer::from_path(file_path);
    if write_result.is_err() {
        return Err(write_result.err().unwrap().into());
    }

    let mut write = write_result.unwrap();
    let ref_list: Vec<&str> = field_list.iter().map(|s| s.as_str()).collect();
    write.write_record(ref_list).expect("happened error");

    let flush_result = write.flush();
    if flush_result.is_err() {
        return Err(flush_result.err().unwrap().into());
    }
    Ok(())
}

pub fn write_data(table_name: String, dataList: Vec<String>) -> Result<(), Box<dyn Error>> {
    let (_, path_str) = generate_file_path(table_name);
    let file_path = Path::new(&path_str);

    let write_result = csv::Writer::from_path(file_path);
    if write_result.is_err() {
        return Err(write_result.err().unwrap().into());
    }

    let mut write = write_result.unwrap();

    write.write_record(dataList).expect("happened error");

    let flush_result = write.flush();
    if flush_result.is_err() {
        return Err(flush_result.err().unwrap().into());
    }
    Ok(())
}

pub fn handle_query(
    table_name: String,
    fields: Vec<String>,
    limit: i32,
) -> Result<Vec<Vec<String>>, Box<dyn Error>> {
    let (schema_file_path_str, data_file_path_str) = generate_file_path(table_name);

    let schema_file_path = Path::new(&schema_file_path_str);
    let schema_reader_result = csv::Reader::from_path(schema_file_path);
    if schema_reader_result.is_err() {
        return Err(schema_reader_result.err().unwrap().into());
    }
    let mut schema_reader = schema_reader_result.unwrap();

    let schema = schema_reader.headers()?.clone();
    let indices: Vec<usize> = fields
        .iter()
        .filter_map(|field| schema.iter().position(|h| h == *field))
        .collect();
    println!("indices: {:?}", indices);

    if indices.is_empty() {
        return Err("None of the specified fields found in CSV headers".into());
    }

    let data_file_path = Path::new(&data_file_path_str);
    let data_reader_result =   csv::ReaderBuilder::new().has_headers(false).from_path(data_file_path);
    if data_reader_result.is_err() {
        return Err(data_reader_result.err().unwrap().into());
    }
    let mut data_reader = data_reader_result.unwrap();

    let mut count = 0;
    let mut result_data = Vec::new();

    for result in data_reader.records() {
        if count >= limit && limit > 0 {
            break;
        }

        let record = result?;
        let filtered_record: Vec<String> = indices
            .iter()
            .map(|&i| record.get(i).unwrap_or("").to_string())
            .collect();

        result_data.push(filtered_record);
        count += 1;
    }

    Ok(result_data)
}

fn generate_file_path(table_name: String) -> (String, String) {
    let mut schema_path = PATH.clone().to_owned();
    schema_path.push_str(&*table_name);
    schema_path.push_str("_schema.csv");

    let mut data_path = PATH.clone().to_owned();
    data_path.push_str(&*table_name);
    data_path.push_str(".csv");
    (schema_path, data_path)
}

#[cfg(test)]
mod test {
    use crate::csv_operation::{
        create_file, generate_file_path, handle_query, write_data,
    };
    use std::fs;
    use std::path::Path;
    #[test]
    fn test_create_file() {
        let table_name = "test1".to_string();
        let field_list = vec!["sex".to_string(), "name".to_string(), "age".to_string()];

        // 生成文件路径
        let (_, file_path) = generate_file_path(table_name.clone());

        // 删除可能存在的旧文件
        if Path::new(&file_path).exists() {
            fs::remove_file(&file_path).expect("Failed to delete existing test file");
        }

        // 执行 create_file
        let result = create_file(table_name, field_list);
        assert!(result.is_ok(), "error is {:#?}", result.err());

    }
    #[test]
    fn test_write() {
        let data1 = vec![
            "man".to_string(),
            "doge".to_string(),
            "18".to_string(),
        ];
        let result1 = write_data("test1".to_string(), data1);
        assert!(result1.is_ok());

        let data2 = Vec::from([
            "man".to_string(),
            "doge".to_string(),
            "18".to_string(),
        ]);
        let result2 = write_data("test1".to_string(), data2);
        assert!(result2.is_ok());
    }

    #[test]
    fn test_read() {
        let result = handle_query("test1".to_string(), Vec::from(["name".to_string()]), 100);
        assert!(result.is_ok());
        println!("{:?}", result.unwrap())
    }


}
