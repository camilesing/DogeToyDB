use crate::PATH;
use rocksdb::{IteratorMode, DB};
use std::any::Any;
use std::cmp::PartialEq;
use std::collections::HashMap;
use std::error::Error;

use serde::{Deserialize, Serialize};
use sqlparser::ast::{BinaryOperator, Expr};
use uuid::Uuid;

pub fn create_default_db() -> Result<(), Box<dyn Error>> {
    let path = generate_file_path();

    let db_mapping_db_name = path.to_string() + &*"_db_mapping";
    let db_mapping_db = DB::open_default(db_mapping_db_name).unwrap();
    let db_info = DbInfo {
        id: Uuid::new_v4().to_string(),
        status: "show".to_string(),
    };

    let ser_value = serde_json::to_string(&db_info).expect("serde for SchemaColumnInfo list error");
    db_mapping_db
        .put("default", ser_value)
        .expect("write db info error");
    Ok(())
}
pub fn create_table(
    db_name: String,
    table_name: String,
    field_list: Vec<Field>,
) -> Result<(), Box<dyn Error>> {
    //确保有对应的DB存在
    let db_info_result = get_db_info(db_name, true);
    if db_info_result.is_err() {
        return Err(format!(
            "happened error when get db info: {:?}",
            db_info_result.err()
        )
        .into());
    }
    let db_info = db_info_result?;

    //确保没有相同的表名
    let table_info_result = get_table_info(db_info.clone().unwrap(), table_name.clone(), false);
    if table_info_result.is_err() {
        return Err(format!(
            "happened error when get table info : {:?}",
            table_info_result.err()
        )
        .into());
    }
    //创建table_mapping表
    let path = generate_file_path();
    let table_mapping_db_name =
        path.to_string() + &db_info.clone().unwrap().id + &*"_table_mapping";
    let table_id = Uuid::new_v4().to_string();
    {
        let table_mapping_db = DB::open_default(table_mapping_db_name.clone()).unwrap();
        let new_table_info = TableInfo {
            id: table_id.clone(),
            status: "show".to_string(),
            table_type: TableType::Pk, //默认先pk
        };

        let ser_value =
            serde_json::to_string(&new_table_info).expect("serde for table info list error");
        table_mapping_db
            .put(table_name.clone(), ser_value)
            .expect("write table info error");
    }

    let table_info_result = get_table_info(db_info.clone().unwrap(), table_name.clone(), true);
    if table_info_result.is_err() {
        return Err(format!(
            "happened error when get table info : {:?}",
            table_info_result.err()
        )
        .into());
    }
    let table_info = table_info_result?.unwrap();

    let schema_info_result = get_schema_info(db_info.clone().unwrap(), table_info.clone(), false);
    if schema_info_result.is_err() {
        return Err(format!(
            "happened error when get schema info : {:?}",
            schema_info_result.err()
        )
        .into());
    }
    //创建schema_info表
    let mut has_pk = false;
    let path = generate_file_path();
    let schema_info_db_name = path.to_string() + &db_info.clone().unwrap().id + &*"_schema_info";
    {
        let schema_info_db = DB::open_default(schema_info_db_name).unwrap();
        let value: Vec<SchemaColumnInfo> = field_list
            .into_iter()
            .map(|field| {
                if field.is_pk {
                    has_pk = true;
                }
                SchemaColumnInfo {
                    id: Uuid::new_v4().to_string(),
                    name: field.name,
                    data_type: DataType::from_str(field.data_type).expect("unsupported type"),
                    status: "show".to_string(),
                    is_index: field.is_index,
                    is_pk: field.is_pk,
                }
            })
            .collect();
        let schema_info_key = table_union_key(db_info.clone().unwrap(), table_info.clone());
        let ser_value =
            serde_json::to_string(&value).expect("serde for SchemaColumnInfo list error");
        schema_info_db
            .put(schema_info_key, ser_value)
            .expect("write schema info db error");
    }

    {
        let table_mapping_db = DB::open_default(table_mapping_db_name.clone()).unwrap();
        let new_table_info = TableInfo {
            id: table_id.clone(),
            status: "show".to_string(),
            table_type: if has_pk {
                TableType::Pk
            } else {
                TableType::Append
            },
        };

        let ser_value =
            serde_json::to_string(&new_table_info).expect("serde for table info list error");
        table_mapping_db
            .put(table_name.clone(), ser_value)
            .expect("write table info error");
    }

    Ok(())
}

pub fn write_data(
    db_name: String,
    table_name: String,
    data_list: Vec<InsertValue>,
) -> Result<(), Box<dyn Error>> {
    let db_info_result = get_db_info(db_name, true);
    if db_info_result.is_err() {
        return Err(format!(
            "happened error when get db info : {:?}",
            db_info_result.err()
        )
        .into());
    }
    let db_info = db_info_result?;

    let table_info_result = get_table_info(db_info.clone().unwrap(), table_name.clone(), true);
    if table_info_result.is_err() {
        return Err(format!(
            "happened error when get table info : {:?}",
            table_info_result.err()
        )
        .into());
    }
    let table_info = table_info_result
        .unwrap()
        .expect("cannot get table info from option");

    let table_union_key = table_union_key(db_info.clone().unwrap(), table_info.clone());
    let path = generate_file_path();
    let data_db_name = path.to_string() + &*table_union_key + &*"_data";

    // 1. 检查 data_list 中的字段是否和 schema_info 中的字段完全相同
    let schema_info_result = get_schema_info(db_info.clone().unwrap(), table_info.clone(), true);
    if schema_info_result.is_err() {
        return Err(format!(
            "happened error when get schema info : {:?}",
            schema_info_result.err()
        )
        .into());
    }
    let schema_cols = schema_info_result?.unwrap();
    let schema_field_names: Vec<String> = schema_cols.iter().map(|col| col.name.clone()).collect();

    let data_fields_set: std::collections::HashSet<String> =
        data_list.iter().map(|val| val.name.clone()).collect();
    let schema_fields_set: std::collections::HashSet<String> =
        schema_field_names.iter().map(|s| s.clone()).collect();

    if data_fields_set != schema_fields_set {
        return Err("data_list fields do not match schema fields. please check u input".into());
    }
    // 2. 按照 schema_info 的顺序对 data_list 排序
    let mut data_map: std::collections::HashMap<_, _> =
        data_list.into_iter().map(|v| (v.name.clone(), v)).collect();

    let mut ordered_data = Vec::with_capacity(schema_field_names.len());
    let mut primary_keys = Vec::new();

    for field_name in &schema_field_names {
        let insert_value = data_map
            .remove(field_name)
            .map(|d| d.value)
            .ok_or_else(|| format!("Missing field in data_list: {}", field_name))?;

        if let Some(schema_col) = schema_cols.iter().find(|col| col.name == *field_name) {
            if schema_col.is_pk {
                primary_keys.push(insert_value.clone());
            }
        }

        ordered_data.push(insert_value);
    }
    let data_db = DB::open_default(data_db_name).unwrap();
    let mut primary_key = String::new();
    let mut last_key = 0; // 默认从0开始
    if table_info.table_type == TableType::Append {
        for kv_result in data_db.iterator(IteratorMode::End) {
            let (k, v) = kv_result.expect("cannot get data from rocksdb");
            let key_str = String::from_utf8(Vec::from(k)).expect("Invalid UTF-8 sequence");
            last_key = key_str.parse().expect("Failed to parse key as integer");
            break; // 只需要最后一个键
        }
        primary_key = (last_key + 1).to_string();
    } else if (table_info.table_type == TableType::Pk && primary_keys.len() != 1) {
        return Err("Pk table write data must has pk".to_string().into());
    } else {
        primary_key = primary_keys[0].clone();
    }

    let ser_value = serde_json::to_string(&ordered_data).expect("write data db error");

    data_db
        .put(primary_key, ser_value)
        .expect("write data db error");
    Ok(())
}

pub fn query(
    db_name: String,
    table_name: String,
    fields: Vec<String>,
    expr: Option<Expr>,
    limit: i32,
) -> Result<String, Box<dyn Error>> {
    let db_info_result = get_db_info(db_name, true);
    if db_info_result.is_err() {
        return Err(format!(
            "happened error  when get db mapping info : {:?}",
            db_info_result.err()
        )
        .into());
    }
    let db_info = db_info_result?;

    let table_info_result = get_table_info(db_info.clone().unwrap(), table_name.clone(), true);
    if table_info_result.is_err() {
        return Err(format!(
            "happened error when get table mapping info: {:?}",
            table_info_result.err()
        )
        .into());
    }
    let table_info = table_info_result?.expect("cannot get tableInfo from option");

    let schema_info_result = get_schema_info(db_info.clone().unwrap(), table_info.clone(), true);
    if schema_info_result.is_err() {
        return Err(format!(
            "happened error when get table info: {:?}",
            schema_info_result.err()
        )
        .into());
    }
    let table_union_key = table_union_key(db_info.unwrap(), table_info.clone());
    let path = generate_file_path();
    let data_db_name = path.to_string() + &*table_union_key + &*"_data";

    let schema_cols = schema_info_result?.unwrap();
    let schema_field_names: Vec<String> = schema_cols.iter().map(|col| col.name.clone()).collect();
    // fields 字段必须是 schema_field_names 的子集，不然报错
    let schema_set: std::collections::HashSet<_> = schema_field_names.iter().collect();
    let field_set: std::collections::HashSet<_> = fields.iter().collect();

    let is_subset = field_set.is_subset(&schema_set);

    if !is_subset {
        return Err("fields contains unknown columns".into());
    }

    let mut op_context = OpContext {
        mode: OperationMode::FullScan,
        value: "".to_string(),
        op: BinaryOperator::Eq, //随便来个
    };

    if table_info.table_type == TableType::Pk {
        if expr.is_some() {
            match &expr.unwrap() {
                Expr::BinaryOp { left, op, right } => {
                    // 提取字段名和值
                    let (value, field_value) = match (&**left, &**right) {
                        (Expr::Identifier(ident), Expr::Value(val)) => {
                            Some((ident.value.clone(), extract_value(val)))
                        }
                        (Expr::Value(val), Expr::Identifier(ident)) => {
                            Some((ident.value.clone(), extract_value(val)))
                        }
                        _ => {
                            let field_name =
                                extract_identifier(left).or_else(|| extract_identifier(right));
                            field_name.map(|name| (name, 0)) // 默认值0
                        }
                    }
                    .unwrap_or(("".to_string(), 0));

                    if !value.is_empty() {
                        for col in &schema_cols {
                            if col.name == value {
                                //rocks特性。对于数据会按照顺序排列
                                if col.is_pk && col.data_type == DataType::Int {
                                    if op == &BinaryOperator::Eq {
                                        op_context = OpContext {
                                            mode: OperationMode::PointGet,
                                            value: field_value.to_string(),
                                            op: op.clone(),
                                        };
                                    } else if op == &BinaryOperator::GtEq
                                        || op == &BinaryOperator::LtEq
                                    {
                                        op_context = OpContext {
                                            mode: OperationMode::PointScan,
                                            value: field_value.to_string(),
                                            op: op.clone(),
                                        }
                                    } else {
                                        return Err(
                                            "Unsupported operation for non-integer primary key"
                                                .into(),
                                        );
                                    }
                                } else if col.is_index {
                                    op_context = OpContext {
                                        mode: OperationMode::RangeScan,
                                        value: value.clone(),
                                        op: op.clone(),
                                    };
                                } else {
                                    op_context = OpContext {
                                        mode: OperationMode::FullScan,
                                        value: value.clone(),
                                        op: op.clone(),
                                    };
                                }
                                break;
                            }
                        }
                    }
                }
                _ => return Err("Unknown exception".into()),
            }
        }
    } else if table_info.table_type != TableType::Append {
        return Err("unsupported table type".into());
    }
    let data_db = DB::open_default(data_db_name).unwrap();
    let mut result: Vec<serde_json::Value> = Vec::new();
    let mut count = 0;
    // 创建字段名到index的映射
    let field_name_to_index: HashMap<_, _> = schema_cols
        .iter()
        .enumerate()
        .map(|(i, col)| (col.name.clone(), i))
        .collect();

    // 确定需要返回的index
    let selected_indices: Vec<usize> = if fields.is_empty() {
        // 如果没有指定字段，则返回所有字段
        (0..schema_cols.len()).collect()
    } else {
        fields
            .iter()
            .map(|field| *field_name_to_index.get(field).expect("Field should exist"))
            .collect()
    };

    match op_context.mode {
        OperationMode::PointGet => {
            // 使用主键快速点查
            let key = op_context.value;
            if let Some(value) = data_db.get(key).expect("cannot query data from table") {
                let v = String::from_utf8(value).expect("Invalid UTF-8 sequence");
                let parsed_data: Vec<serde_json::Value> =
                    serde_json::from_str(&v).expect("Failed to parse JSON data");

                // 根据指定字段过滤数据
                let filtered_data: Vec<serde_json::Value> = get_filtered_data(
                    selected_indices.clone(),
                    parsed_data.clone(),
                    schema_cols.clone(),
                );

                result.push(serde_json::Value::Array(filtered_data));
            }
        }
        OperationMode::PointScan => {
            // 范围扫描，从指定键开始
            let start_key = op_context.value;
            let iter = if op_context.op == BinaryOperator::LtEq {
                data_db.iterator(IteratorMode::From(
                    start_key.as_bytes(),
                    rocksdb::Direction::Reverse,
                ))
            } else {
                data_db.iterator(IteratorMode::From(
                    start_key.as_bytes(),
                    rocksdb::Direction::Forward,
                ))
            };

            for item in iter {
                if let Ok((key, value)) = item {
                    if count >= limit {
                        break;
                    }
                    count += 1;

                    let v = String::from_utf8(Vec::from(value)).expect("Invalid UTF-8 sequence");
                    let parsed_data: Vec<serde_json::Value> =
                        serde_json::from_str(&v).expect("Failed to parse JSON data");

                    // 根据指定字段过滤数据
                    let filtered_data: Vec<serde_json::Value> = get_filtered_data(
                        selected_indices.clone(),
                        parsed_data,
                        schema_cols.clone(),
                    );

                    result.push(serde_json::Value::Array(filtered_data));
                } else {
                    return Err(format!("Point scan table happened error: {:?}", item.err()).into());
                }
            }
        }
        OperationMode::RangeScan => {
            // 范围扫描，从头开始
            for kv_result in data_db.iterator(rocksdb::IteratorMode::Start) {
                if kv_result.is_err() {
                    return Err(format!(
                        "Range scan table happened error type: {:?}",
                        kv_result.err()
                    )
                    .into());
                }

                if count >= limit {
                    break;
                }
                count += 1;

                let v = String::from_utf8(Vec::from(kv_result.unwrap().1))
                    .expect("Invalid UTF-8 sequence");
                let parsed_data: Vec<serde_json::Value> =
                    serde_json::from_str(&v).expect("Failed to parse JSON data");

                // 根据指定字段过滤数据
                let filtered_data: Vec<serde_json::Value> =
                    get_filtered_data(selected_indices.clone(), parsed_data, schema_cols.clone());

                result.push(serde_json::Value::Array(filtered_data));
            }
        }

        OperationMode::FullScan => {
            for kv_result in data_db.iterator(rocksdb::IteratorMode::Start) {
                if kv_result.is_err() {
                    return Err(format!(
                        "Full scan table happened error type: {:?}",
                        kv_result.err()
                    )
                    .into());
                } else {
                    if count >= limit {
                        break;
                    }
                    count += 1;
                    let v = String::from_utf8(Vec::from(kv_result.unwrap().1))
                        .expect("Invalid UTF-8 sequence");
                    let parsed_data: Vec<serde_json::Value> =
                        serde_json::from_str(&v).expect("Failed to parse JSON data");

                    // 根据指定字段过滤数据
                    let filtered_data: Vec<serde_json::Value> = selected_indices
                        .iter()
                        .map(|&i| parsed_data[i].clone())
                        .collect();

                    result.push(serde_json::Value::Array(filtered_data));
                }
            }
        }
        _ => return Err(format!("Unsupported type: {:?}", op_context.mode).into()),
    }

    let ser_value = serde_json::to_string(&result).expect("serde for query error");
    Ok(ser_value)
}

fn get_filtered_data(
    selected_indices: Vec<usize>,
    parsed_data: Vec<serde_json::Value>,
    schema_cols: Vec<SchemaColumnInfo>,
) -> Vec<serde_json::Value> {
    selected_indices
        .iter()
        .map(|&i| {
            let value = parsed_data[i].clone();
            convert_data_type(value, &schema_cols[i].data_type)
        })
        .collect()
}

fn extract_identifier(expr: &Expr) -> Option<String> {
    match expr {
        Expr::BinaryOp { left, op: _, right } => {
            if let Expr::Identifier(ident) = &**left {
                return Some(ident.value.clone());
            }
            if let Expr::Identifier(ident) = &**right {
                return Some(ident.value.clone());
            }
            // 递归检查子表达式
            if let Some(name) = extract_identifier(left) {
                return Some(name);
            }
            if let Some(name) = extract_identifier(right) {
                return Some(name);
            }
            None
        }
        Expr::Identifier(ident) => Some(ident.value.clone()),
        _ => None,
    }
}

fn convert_data_type(value: serde_json::Value, data_type: &DataType) -> serde_json::Value {
    match data_type {
        DataType::Int => {
            match &value {
                serde_json::Value::String(s) => {
                    // 尝试将字符串转换为整数
                    match s.parse::<i64>() {
                        Ok(num) => serde_json::Value::Number(num.into()),
                        Err(_) => value, // 转换失败则保持原值
                    }
                }
                _ => value,
            }
        }
        DataType::String => {
            match &value {
                serde_json::Value::Number(n) => {
                    // 将数字转换为字符串
                    serde_json::Value::String(n.to_string())
                }
                _ => value,
            }
        }
    }
}

fn extract_value(value: &sqlparser::ast::Value) -> i32 {
    match value {
        sqlparser::ast::Value::Number(s, _) => s.parse().unwrap_or(0),
        _ => 0,
    }
}

fn get_db_info(db_name: String, expect_is_exist: bool) -> Result<Option<DbInfo>, Box<dyn Error>> {
    let path = generate_file_path();

    let db_mapping_db_name = path.to_string() + &*"_db_mapping";
    let db_mapping_db = DB::open_default(db_mapping_db_name).unwrap();
    let get_db_mapping = db_mapping_db.get(db_name).expect("cannot get DB info ");
    if expect_is_exist {
        if get_db_mapping.is_none() {
            return Err("db info cannot be None".into());
        }
    } else {
        if get_db_mapping.is_some() {
            return Err("db info must is None".into());
        }
        return Ok(None);
    }

    let string_result = String::from_utf8(get_db_mapping.unwrap()).expect("Invalid UTF-8 sequence");
    let db_info: DbInfo = serde_json::from_str(&*string_result).expect("serde db_info fail");
    Ok(Some(db_info))
}

fn get_table_info(
    db_info: DbInfo,
    table_name: String,
    expect_is_exist: bool,
) -> Result<Option<TableInfo>, Box<dyn Error>> {
    let path = generate_file_path();

    let table_mapping_name = path.to_string() + &db_info.id + &*"_table_mapping".to_string();
    let table_mapping_db = DB::open_default(table_mapping_name).unwrap();
    let get_table_mapping = table_mapping_db
        .get(table_name)
        .expect("cannot get table info ");
    if expect_is_exist {
        if get_table_mapping.is_none() {
            return Err("table info cannot be None".into());
        }
    } else {
        if get_table_mapping.is_some() {
            return Err("table info must is None".into());
        }
        return Ok(None);
    }

    let string_result =
        String::from_utf8(get_table_mapping.unwrap()).expect("Invalid UTF-8 sequence");
    let table_info: TableInfo =
        serde_json::from_str(&*string_result).expect("serde table_info fail");
    Ok(Some(table_info))
}

fn get_schema_info(
    db_info: DbInfo,
    table_info: TableInfo,
    expect_is_exist: bool,
) -> Result<Option<Vec<SchemaColumnInfo>>, Box<dyn Error>> {
    let info_key = table_union_key(db_info.clone(), table_info);
    let path = generate_file_path();
    let schema_info_db_name = path.to_string() + &db_info.id + &*"_schema_info";
    let schema_info_db = DB::open_default(schema_info_db_name).unwrap();
    let get_schema_info = schema_info_db
        .get(info_key)
        .expect("cannot get schema info ");
    if expect_is_exist {
        if get_schema_info.is_none() {
            return Err("schema info cannot be None".into());
        }
    } else {
        if get_schema_info.is_some() {
            return Err("schema info must is None".into());
        }
        return Ok(None);
    }
    let string_result =
        String::from_utf8(get_schema_info.unwrap()).expect("Invalid UTF-8 sequence");
    let schema_cols: Vec<SchemaColumnInfo> =
        serde_json::from_str(&*string_result).expect("serde schema_cols fail");
    Ok(Some(schema_cols))
}

fn table_union_key(db_info: DbInfo, table_info: TableInfo) -> String {
    format!("{}_{}", db_info.id, table_info.id)
}

fn generate_file_path() -> String {
    std::env::var("DB_PATH").unwrap_or_else(|_| PATH.clone().to_owned())
}

#[derive(Serialize, Deserialize, Clone)]
struct DbInfo {
    id: String,
    status: String,
}

#[derive(Serialize, Deserialize, Clone)]
struct TableInfo {
    id: String,
    status: String,
    table_type: TableType,
}
#[derive(Serialize, Deserialize, Clone, Debug)]
enum TableType {
    Append,
    Pk,
}

impl TableType {
    pub fn from_str(s: String) -> Result<Self, Box<dyn Error>> {
        match s.as_str() {
            "Append" => Ok(TableType::Append),
            "Pk" => Ok(TableType::Pk),
            _ => Err(format!("Unknown table type: {}", s).into()),
        }
    }
}

impl PartialEq for TableType {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (TableType::Append, TableType::Append) => true,
            (TableType::Pk, TableType::Pk) => true,
            _ => false,
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
struct SchemaColumnInfo {
    id: String,
    name: String,
    data_type: DataType,
    status: String,
    is_pk: bool,
    is_index: bool,
}

#[derive(Serialize, Deserialize, Clone)]
enum DataType {
    Int,
    String,
}

impl DataType {
    pub fn from_str(s: String) -> Result<Self, Box<dyn Error>> {
        match s.as_str() {
            "Int" => Ok(DataType::Int),
            "String" => Ok(DataType::String),
            _ => Err(format!("Unknown data type: {}", s).into()),
        }
    }
}

impl PartialEq for DataType {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (DataType::Int, DataType::Int) => true,
            (DataType::String, DataType::String) => true,
            _ => false,
        }
    }
}
#[derive(Clone)]
pub(crate) struct Field {
    name: String,
    data_type: String,
    is_pk: bool,
    is_index: bool,
}

impl Field {
    pub fn new(name: String, data_type: String, is_pk: bool, is_index: bool) -> Self {
        Self {
            name,
            data_type,
            is_pk,
            is_index,
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
pub struct InsertValue {
    pub(crate) name: String,
    pub(crate) value: String,
}

#[derive(Clone)]
struct OpContext {
    mode: OperationMode,
    value: String,
    op: BinaryOperator,
}

#[derive(Clone, Debug)]
enum OperationMode {
    PointGet,
    PointScan,
    RangeScan,
    FullScan,
}

#[cfg(test)]
mod test {
    use crate::rocksdb_operation::*;
    use serial_test::serial;
    use tempfile::TempDir;

    fn setup_test_env() -> TempDir {
        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        std::env::set_var("DB_PATH", temp_dir.path().to_str().unwrap());
        temp_dir
    }

    #[test]
    #[serial]
    fn test_create_pk_table_success() {
        let _temp_dir = setup_test_env();
        create_default_db().expect("Failed to create default db");

        let fields = vec![
            Field::new("id".to_string(), "Int".to_string(), true, false),
            Field::new("name".to_string(), "String".to_string(), false, false),
        ];

        let result = create_table("default".to_string(), "users".to_string(), fields);
        assert!(
            result.is_ok(),
            "Failed to create PK table: {:?}",
            result.err()
        );

        // 验证表是否创建成功
        let db_info = get_db_info("default".to_string(), true).unwrap().unwrap();
        let table_info = get_table_info(db_info, "users".to_string(), true)
            .unwrap()
            .unwrap();
        assert_eq!(table_info.table_type, TableType::Pk);
    }

    #[test]
    #[serial]
    fn test_create_append_table_success() {
        let _temp_dir = setup_test_env();
        create_default_db().expect("Failed to create default db");

        let fields = vec![
            Field::new("name".to_string(), "String".to_string(), false, false),
            Field::new("age".to_string(), "Int".to_string(), false, false),
        ];

        let result = create_table("default".to_string(), "logs".to_string(), fields);
        assert!(
            result.is_ok(),
            "Failed to create Append table: {:?}",
            result.err()
        );

        // 验证表是否创建成功
        let db_info = get_db_info("default".to_string(), true).unwrap().unwrap();
        let table_info = get_table_info(db_info, "logs".to_string(), true)
            .unwrap()
            .unwrap();
        assert_eq!(table_info.table_type, TableType::Append);
    }

    #[test]
    #[serial]
    fn test_create_table_with_existing_name_should_fail() {
        let _temp_dir = setup_test_env();
        create_default_db().expect("Failed to create default db");

        let fields1 = vec![
            Field::new("id".to_string(), "Int".to_string(), true, false),
            Field::new("name".to_string(), "String".to_string(), false, false),
        ];

        let fields2 = vec![
            Field::new("user_id".to_string(), "Int".to_string(), true, false),
            Field::new("email".to_string(), "String".to_string(), false, false),
        ];

        // 创建第一个表
        let result1 = create_table("default".to_string(), "users".to_string(), fields1);
        assert!(result1.is_ok(), "Failed to create first table");

        // 尝试创建同名表应该失败
        let result2 = create_table("default".to_string(), "users".to_string(), fields2);
        assert!(
            result2.is_err(),
            "Creating table with existing name should fail"
        );
        assert!(result2
            .unwrap_err()
            .to_string()
            .contains("happened error when get table info"));
    }

    #[test]
    #[serial]
    fn test_create_table_with_nonexistent_db_should_fail() {
        let _temp_dir = setup_test_env();

        let fields = vec![Field::new("id".to_string(), "Int".to_string(), true, false)];

        let result = create_table("nonexistent_db".to_string(), "users".to_string(), fields);
        assert!(
            result.is_err(),
            "Creating table with nonexistent db should fail"
        );
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("happened error when get db info"));
    }

    #[test]
    #[serial]
    fn test_write_data_to_pk_table_success() {
        let _temp_dir = setup_test_env();
        create_default_db().expect("Failed to create default db");

        // 创建主键表
        let fields = vec![
            Field::new("id".to_string(), "Int".to_string(), true, false),
            Field::new("name".to_string(), "String".to_string(), false, false),
        ];

        let result = create_table("default".to_string(), "users".to_string(), fields);
        assert!(result.is_ok(), "Failed to create PK table");

        // 写入数据
        let data_list = vec![
            InsertValue {
                name: "id".to_string(),
                value: "1".to_string(),
            },
            InsertValue {
                name: "name".to_string(),
                value: "Alice".to_string(),
            },
        ];

        let write_result = write_data("default".to_string(), "users".to_string(), data_list);
        assert!(
            write_result.is_ok(),
            "Failed to write data to PK table: {:?}",
            write_result.err()
        );

        // 验证数据写入
        let query_result = query(
            "default".to_string(),
            "users".to_string(),
            vec!["id".to_string(), "name".to_string()],
            None,
            10,
        );
        assert!(
            query_result.is_ok(),
            "Failed to query data: {:?}",
            query_result.err()
        );

        let result_str = query_result.unwrap();
        let parsed_data: Vec<serde_json::Value> =
            serde_json::from_str(&result_str).expect("Failed to parse query result");

        assert_eq!(parsed_data.len(), 1);
        if let serde_json::Value::Array(arr) = &parsed_data[0] {
            assert_eq!(arr.len(), 2);
            assert_eq!(arr[0], serde_json::Value::String("1".to_string()));
            assert_eq!(arr[1], serde_json::Value::String("Alice".to_string()));
        } else {
            panic!("Expected array in query result");
        }
    }

    #[test]
    #[serial]
    fn test_write_data_to_append_table_success() {
        let _temp_dir = setup_test_env();
        create_default_db().expect("Failed to create default db");

        // 创建append表
        let fields = vec![
            Field::new("name".to_string(), "String".to_string(), false, false),
            Field::new("age".to_string(), "Int".to_string(), false, false),
        ];

        let result = create_table("default".to_string(), "logs".to_string(), fields);
        assert!(result.is_ok(), "Failed to create Append table");

        // 写入多条数据
        let data_list1 = vec![
            InsertValue {
                name: "name".to_string(),
                value: "Log1".to_string(),
            },
            InsertValue {
                name: "age".to_string(),
                value: "25".to_string(),
            },
        ];

        let data_list2 = vec![
            InsertValue {
                name: "name".to_string(),
                value: "Log2".to_string(),
            },
            InsertValue {
                name: "age".to_string(),
                value: "30".to_string(),
            },
        ];

        let write_result1 = write_data("default".to_string(), "logs".to_string(), data_list1);
        assert!(
            write_result1.is_ok(),
            "{}",
            format!(
                "Failed to write first data to Append table {:?}",
                write_result1.err()
            )
        );

        let write_result2 = write_data("default".to_string(), "logs".to_string(), data_list2);
        assert!(
            write_result2.is_ok(),
            "Failed to write second data to Append table"
        );

        // 验证数据写入
        let query_result = query(
            "default".to_string(),
            "logs".to_string(),
            vec!["name".to_string(), "age".to_string()],
            None,
            10,
        );
        assert!(
            query_result.is_ok(),
            "Failed to query data: {:?}",
            query_result.err()
        );

        let result_str = query_result.unwrap();
        let parsed_data: Vec<serde_json::Value> =
            serde_json::from_str(&result_str).expect("Failed to parse query result");

        assert_eq!(parsed_data.len(), 2);

        // 验证第一条数据
        if let serde_json::Value::Array(arr) = &parsed_data[0] {
            assert_eq!(arr.len(), 2);
            assert_eq!(arr[0], serde_json::Value::String("Log1".to_string()));
            assert_eq!(arr[1], serde_json::Value::String("25".to_string()));
        } else {
            panic!("Expected array in query result");
        }

        // 验证第二条数据
        if let serde_json::Value::Array(arr) = &parsed_data[1] {
            assert_eq!(arr.len(), 2);
            assert_eq!(arr[0], serde_json::Value::String("Log2".to_string()));
            assert_eq!(arr[1], serde_json::Value::String("30".to_string()));
        } else {
            panic!("Expected array in query result");
        }
    }

    #[test]
    #[serial]
    fn test_write_data_with_missing_fields_should_fail() {
        let _temp_dir = setup_test_env();
        create_default_db().expect("Failed to create default db");

        // 创建主键表
        let fields = vec![
            Field::new("id".to_string(), "Int".to_string(), true, false),
            Field::new("name".to_string(), "String".to_string(), false, false),
            Field::new("email".to_string(), "String".to_string(), false, false),
        ];

        let result = create_table("default".to_string(), "users".to_string(), fields);
        assert!(result.is_ok(), "Failed to create PK table");

        // 尝试写入缺少字段的数据
        let incomplete_data_list = vec![
            InsertValue {
                name: "id".to_string(),
                value: "1".to_string(),
            },
            InsertValue {
                name: "name".to_string(),
                value: "Alice".to_string(),
            },
            // 缺少 email 字段
        ];

        let write_result = write_data(
            "default".to_string(),
            "users".to_string(),
            incomplete_data_list,
        );
        assert!(
            write_result.is_err(),
            "Writing data with missing fields should fail"
        );
        assert!(write_result
            .unwrap_err()
            .to_string()
            .contains("data_list fields do not match schema fields"));
    }

    #[test]
    #[serial]
    fn test_write_data_to_pk_table_without_pk_should_fail() {
        let _temp_dir = setup_test_env();
        create_default_db().expect("Failed to create default db");

        // 创建主键表
        let fields = vec![
            Field::new("id".to_string(), "Int".to_string(), true, false),
            Field::new("name".to_string(), "String".to_string(), false, false),
        ];

        let result = create_table("default".to_string(), "users".to_string(), fields);
        assert!(result.is_ok(), "Failed to create PK table");

        // 尝试写入没有主键的数据
        let data_list = vec![
            InsertValue {
                name: "name".to_string(),
                value: "Alice".to_string(),
            },
            // 缺少 id 主键字段
        ];

        let write_result = write_data("default".to_string(), "users".to_string(), data_list);
        assert!(
            write_result.is_err(),
            "Writing data without PK to PK table should fail"
        );
    }

    #[test]
    #[serial]
    fn test_query_pk_table_point_get() {
        let _temp_dir = setup_test_env();
        create_default_db().expect("Failed to create default db");

        // 创建主键表
        let fields = vec![
            Field::new("id".to_string(), "Int".to_string(), true, false),
            Field::new("name".to_string(), "String".to_string(), false, false),
        ];

        create_table("default".to_string(), "users".to_string(), fields)
            .expect("Failed to create table");

        // 插入测试数据
        let data_list = vec![
            InsertValue {
                name: "id".to_string(),
                value: "1".to_string(),
            },
            InsertValue {
                name: "name".to_string(),
                value: "Alice".to_string(),
            },
        ];

        write_data("default".to_string(), "users".to_string(), data_list)
            .expect("Failed to write data");

        // 使用等于条件进行点查
        use sqlparser::ast::*;
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Identifier(Ident::new("id"))),
            op: BinaryOperator::Eq,
            right: Box::new(Expr::Value(Value::Number("1".to_string(), false))),
        };

        let query_result = query(
            "default".to_string(),
            "users".to_string(),
            vec!["id".to_string(), "name".to_string()],
            Some(expr),
            10,
        );

        assert!(query_result.is_ok());
        let result_str = query_result.unwrap();
        let parsed_data: Vec<serde_json::Value> =
            serde_json::from_str(&result_str).expect("Failed to parse query result");

        assert_eq!(parsed_data.len(), 1);
        if let serde_json::Value::Array(arr) = &parsed_data[0] {
            assert_eq!(arr.len(), 2);
            assert_eq!(arr[0], serde_json::Value::Number(1i32.into()));
            assert_eq!(arr[1], serde_json::Value::String("Alice".to_string()));
        }
    }

    #[test]
    #[serial]
    fn test_query_pk_table_point_scan() {
        let _temp_dir = setup_test_env();
        create_default_db().expect("Failed to create default db");

        // 创建主键表
        let fields = vec![
            Field::new("id".to_string(), "Int".to_string(), true, false),
            Field::new("name".to_string(), "String".to_string(), false, false),
        ];

        create_table("default".to_string(), "users".to_string(), fields)
            .expect("Failed to create table");

        // 插入测试数据
        for i in 1..=5 {
            let data_list = vec![
                InsertValue {
                    name: "id".to_string(),
                    value: i.to_string(),
                },
                InsertValue {
                    name: "name".to_string(),
                    value: format!("User{}", i),
                },
            ];

            write_data("default".to_string(), "users".to_string(), data_list)
                .expect("Failed to write data");
        }

        // 使用大于条件进行范围扫描
        use sqlparser::ast::*;
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Identifier(Ident::new("id"))),
            op: BinaryOperator::GtEq,
            right: Box::new(Expr::Value(Value::Number("3".to_string(), false))),
        };

        let query_result = query(
            "default".to_string(),
            "users".to_string(),
            vec!["id".to_string(), "name".to_string()],
            Some(expr),
            10,
        );

        assert!(query_result.is_ok());
        let result_str = query_result.unwrap();
        let parsed_data: Vec<serde_json::Value> =
            serde_json::from_str(&result_str).expect("Failed to parse query result");

        // 应该返回id大于等于3的记录 (4和5)
        assert_eq!(parsed_data.len(), 3);
    }

    #[test]
    #[serial]
    fn test_query_pk_table_full_scan() {
        let _temp_dir = setup_test_env();
        create_default_db().expect("Failed to create default db");

        // 创建主键表
        let fields = vec![
            Field::new("id".to_string(), "Int".to_string(), true, false),
            Field::new("name".to_string(), "String".to_string(), false, false),
        ];

        create_table("default".to_string(), "users".to_string(), fields)
            .expect("Failed to create table");

        // 插入测试数据
        for i in 1..=3 {
            let data_list = vec![
                InsertValue {
                    name: "id".to_string(),
                    value: i.to_string(),
                },
                InsertValue {
                    name: "name".to_string(),
                    value: format!("User{}", i),
                },
            ];

            write_data("default".to_string(), "users".to_string(), data_list)
                .expect("Failed to write data");
        }

        // 不带条件的全表扫描
        let query_result = query(
            "default".to_string(),
            "users".to_string(),
            vec!["id".to_string(), "name".to_string()],
            None,
            10,
        );

        assert!(query_result.is_ok());
        let result_str = query_result.unwrap();
        let parsed_data: Vec<serde_json::Value> =
            serde_json::from_str(&result_str).expect("Failed to parse query result");

        // 应该返回所有记录
        assert_eq!(parsed_data.len(), 3);
    }

    #[test]
    #[serial]
    fn test_query_append_table_full_scan() {
        let _temp_dir = setup_test_env();
        create_default_db().expect("Failed to create default db");

        // 创建Append表
        let fields = vec![
            Field::new("content".to_string(), "String".to_string(), false, false),
            Field::new("timestamp".to_string(), "Int".to_string(), false, false),
        ];

        create_table("default".to_string(), "logs".to_string(), fields)
            .expect("Failed to create table");

        // 插入测试数据
        for i in 1..=3 {
            let data_list = vec![
                InsertValue {
                    name: "content".to_string(),
                    value: format!("Log message {}", i),
                },
                InsertValue {
                    name: "timestamp".to_string(),
                    value: (1000 + i).to_string(),
                },
            ];

            write_data("default".to_string(), "logs".to_string(), data_list)
                .expect("Failed to write data");
        }

        // Append表只支持全表扫描
        let query_result = query(
            "default".to_string(),
            "logs".to_string(),
            vec!["content".to_string(), "timestamp".to_string()],
            None,
            10,
        );

        assert!(query_result.is_ok());
        let result_str = query_result.unwrap();
        let parsed_data: Vec<serde_json::Value> =
            serde_json::from_str(&result_str).expect("Failed to parse query result");

        // 应该返回所有记录
        assert_eq!(parsed_data.len(), 3);
    }

    #[test]
    #[serial]
    fn test_query_with_field_selection() {
        let _temp_dir = setup_test_env();
        create_default_db().expect("Failed to create default db");

        // 创建主键表
        let fields = vec![
            Field::new("id".to_string(), "Int".to_string(), true, false),
            Field::new("name".to_string(), "String".to_string(), false, false),
            Field::new("email".to_string(), "String".to_string(), false, false),
        ];

        create_table("default".to_string(), "users".to_string(), fields)
            .expect("Failed to create table");

        // 插入测试数据
        let data_list = vec![
            InsertValue {
                name: "id".to_string(),
                value: "1".to_string(),
            },
            InsertValue {
                name: "name".to_string(),
                value: "Alice".to_string(),
            },
            InsertValue {
                name: "email".to_string(),
                value: "alice@example.com".to_string(),
            },
        ];

        write_data("default".to_string(), "users".to_string(), data_list)
            .expect("Failed to write data");

        // 只查询部分字段
        let query_result = query(
            "default".to_string(),
            "users".to_string(),
            vec!["name".to_string()], // 只查询name字段
            None,
            10,
        );

        assert!(query_result.is_ok());
        let result_str = query_result.unwrap();
        let parsed_data: Vec<serde_json::Value> =
            serde_json::from_str(&result_str).expect("Failed to parse query result");

        assert_eq!(parsed_data.len(), 1);
        if let serde_json::Value::Array(arr) = &parsed_data[0] {
            // 只应该返回一个字段 (name)
            assert_eq!(arr.len(), 1);
            assert_eq!(arr[0], serde_json::Value::String("Alice".to_string()));
        }
    }

    #[test]
    #[serial]
    fn test_query_with_limit() {
        let _temp_dir = setup_test_env();
        create_default_db().expect("Failed to create default db");

        // 创建主键表
        let fields = vec![
            Field::new("id".to_string(), "Int".to_string(), true, false),
            Field::new("name".to_string(), "String".to_string(), false, false),
        ];

        create_table("default".to_string(), "users".to_string(), fields)
            .expect("Failed to create table");

        // 插入测试数据
        for i in 1..=5 {
            let data_list = vec![
                InsertValue {
                    name: "id".to_string(),
                    value: i.to_string(),
                },
                InsertValue {
                    name: "name".to_string(),
                    value: format!("User{}", i),
                },
            ];

            write_data("default".to_string(), "users".to_string(), data_list)
                .expect("Failed to write data");
        }

        // 限制返回结果数量
        let query_result = query(
            "default".to_string(),
            "users".to_string(),
            vec!["id".to_string(), "name".to_string()],
            None,
            3, // 限制为3条记录
        );

        assert!(query_result.is_ok());
        let result_str = query_result.unwrap();
        let parsed_data: Vec<serde_json::Value> =
            serde_json::from_str(&result_str).expect("Failed to parse query result");

        // 应该只返回3条记录
        assert_eq!(parsed_data.len(), 3);
    }

    #[test]
    #[serial]
    fn test_query_nonexistent_table_should_fail() {
        let _temp_dir = setup_test_env();
        create_default_db().expect("Failed to create default db");

        // 查询不存在的表应该失败
        let query_result = query(
            "default".to_string(),
            "nonexistent".to_string(),
            vec!["id".to_string()],
            None,
            10,
        );

        assert!(query_result.is_err());
        assert!(query_result
            .unwrap_err()
            .to_string()
            .contains("happened error when get table mapping info"));
    }

    #[test]
    #[serial]
    fn test_query_with_nonexistent_field_should_fail() {
        let _temp_dir = setup_test_env();
        create_default_db().expect("Failed to create default db");

        // 创建主键表
        let fields = vec![
            Field::new("id".to_string(), "Int".to_string(), true, false),
            Field::new("name".to_string(), "String".to_string(), false, false),
        ];

        create_table("default".to_string(), "users".to_string(), fields)
            .expect("Failed to create table");

        // 查询不存在的字段应该失败
        let query_result = query(
            "default".to_string(),
            "users".to_string(),
            vec!["nonexistent_field".to_string()],
            None,
            10,
        );

        assert!(query_result.is_err());
        assert!(query_result
            .unwrap_err()
            .to_string()
            .contains("fields contains unknown columns"));
    }
}
