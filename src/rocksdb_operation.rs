use crate::PATH;
use rocksdb::DB;

use std::error::Error;

use std::string::String;

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
    {
        let table_mapping_db = DB::open_default(table_mapping_db_name).unwrap();
        let new_table_info = TableInfo {
            id: Uuid::new_v4().to_string(),
            status: "show".to_string(),
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
    let path = generate_file_path();
    let schema_info_db_name = path.to_string() + &db_info.clone().unwrap().id + &*"_schema_info";
    {
        let schema_info_db = DB::open_default(schema_info_db_name).unwrap();
        let value: Vec<SchemaColumnInfo> = field_list
            .into_iter()
            .map(|field| SchemaColumnInfo {
                id: Uuid::new_v4().to_string(),
                name: field.name,
                data_type: field.data_type,
                status: "show".to_string(),
                is_index: field.is_index,
                is_pk: field.is_pk,
            })
            .collect();
        let schema_info_key = table_union_key(db_info.clone().unwrap(), table_info.clone());
        let ser_value =
            serde_json::to_string(&value).expect("serde for SchemaColumnInfo list error");
        schema_info_db
            .put(schema_info_key, ser_value)
            .expect("write schema info db error");
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
    let table_info = table_info_result?;

    let table_union_key = table_union_key(db_info.clone().unwrap(), table_info.clone().unwrap());
    let path = generate_file_path();
    let data_db_name = path.to_string() + &*table_union_key + &*"_data";

    // 1. 检查 data_list 中的字段是否和 schema_info 中的字段完全相同
    let schema_info_result =
        get_schema_info(db_info.clone().unwrap(), table_info.clone().unwrap(), true);
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

    if primary_keys.len() != 1 {
        //todo 实现非主键表写入
        return Err(format!("Expected one primary key, found {}", primary_keys.len()).into());
    }
    let primary_key = primary_keys[0].clone();
    let ser_value = serde_json::to_string(&ordered_data).expect("write data db error");
    let data_db = DB::open_default(data_db_name).unwrap();
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
    let db_info_result =
        get_db_info(db_name, true);
    if db_info_result.is_err() {
        return Err(format!(
            "happened error  when get db mapping info : {:?}",
            db_info_result.err()
        )
        .into());
    }
    let db_info = db_info_result?;

    let table_info_result =
        get_table_info(db_info.clone().unwrap(), table_name.clone(), true);
    if table_info_result.is_err() {
        return Err(format!(
            "happened error when get table mapping info: {:?}",
            table_info_result.err()
        )
        .into());
    }
    let table_info = table_info_result?;

    let schema_info_result =
        get_schema_info(db_info.clone().unwrap(), table_info.clone().unwrap(), true);
    if schema_info_result.is_err() {
        return Err(format!(
            "happened error when get table info: {:?}",
            schema_info_result.err()
        )
        .into());
    }
    let table_union_key = table_union_key(db_info.unwrap(), table_info.clone().unwrap());
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
        name: "".to_string(),
    };

    if expr.is_some() {
        match &expr.unwrap() {
            Expr::BinaryOp { left, op, right } => {
                // 提取字段名逻辑
                let field_name = match (&**left, &**right) {
                    (Expr::Identifier(ident), _) => Some(ident.value.clone()),
                    (_, Expr::Identifier(ident)) => Some(ident.value.clone()),
                    _ => extract_identifier(left).or_else(|| extract_identifier(right)),
                };

                if let Some(name) = field_name {
                    for col in &schema_cols {
                        if col.id == name {
                            //rocks特性。对于数据会按照顺序排列
                            if col.is_pk && col.data_type == "int" {
                                if op == &BinaryOperator::Eq {
                                    op_context = OpContext {
                                        mode: OperationMode::PointGet,
                                        name,
                                    };
                                } else {
                                    op_context = OpContext {
                                        mode: OperationMode::PointScan,
                                        name,
                                    }
                                }
                            } else if col.is_index {
                                op_context = OpContext {
                                    mode: OperationMode::RangeScan,
                                    name,
                                };
                            } else {
                                op_context = OpContext {
                                    mode: OperationMode::FullScan,
                                    name,
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
    let data_db = DB::open_default(data_db_name).unwrap();
    let mut result = Vec::new();
    let mut count = 0;
    match op_context.mode {
        OperationMode::PointGet => {
            // 使用主键快速点查
            let key = op_context.name;
            if let Some(value) = data_db.get(key).expect("cannot query data from table") {
                let v = String::from_utf8(value).expect("Invalid UTF-8 sequence");
                //todo 对于返回的值，要按照传入的field来排序返回。
                result.push(v);
            }
        }
        //todo impl pointRange and rangeScan
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
                    //todo 对于返回的值，要按照传入的field来排序返回。并且可以指定field字段
                    result.push(v);
                }
            }
        }
        _ => return Err(format!("Unsupported type: {:?}", op_context.mode).into()),
    }

    let ser_value = serde_json::to_string(&result).expect("serde for query error");
    Ok(ser_value)
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
    PATH.clone().to_owned()
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
}
#[derive(Serialize, Deserialize)]
struct SchemaColumnInfo {
    id: String,
    name: String,
    data_type: String,
    status: String,
    is_pk: bool,
    is_index: bool,
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
    name: String,
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
    use rocksdb::DB;

    #[test]
    fn test_example() {
        let key = "key";
        let db = DB::open_default("_my_test").unwrap();
        db.put(key, "value").expect("TODO: panic message");
        let result = db.get(key);
        assert!(result.is_ok(), "error is {:#?}", result.err());
        let inner_result = result.unwrap().unwrap();
        let string_result = String::from_utf8(inner_result).expect("Invalid UTF-8 sequence");
        println!("{}", string_result);
        db.delete(key).expect("TODO: panic message");
    }
}
