use crate::rocksdb_operation::{create_table, query, write_data, Field, InsertValue};
use sqlparser::ast::{
    ColumnOption, ColumnOptionDef, CreateTable, Expr, Insert, Query, Select, SelectItem, SetExpr,
    Statement, TableFactor, TableWithJoins, Value,
};
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;
use std::error::Error;

pub fn handle(sql: &str) -> Result<String, Box<dyn Error>> {
    let dialect = MySqlDialect {};
    let mut stmts = Parser::parse_sql(&dialect, sql).unwrap();
    let stmt = stmts.swap_remove(0);
    //默认走default。后面根据use db_name来内置一个变量做判断是否替换default
    let default_db_name = "default";
    match stmt {
        Statement::Query(q) => {
            let table_name = extract_table_name_from_query(q.as_ref()).unwrap();
            let fields = extract_select_fields_from_query(q.as_ref());
            let limit = extract_limit_from_query(q.as_ref()).unwrap();
            let where_expr = extract_where_condition_from_query(q.as_ref());

            Ok(query(
                default_db_name.to_string(),
                table_name,
                fields,
                where_expr,
                limit,
            )?)
        }
        Statement::CreateTable(c) => {
            let (table_name, columns) = parse_create_table(c)?;
            let result = create_table(default_db_name.to_string(), table_name, columns);
            if result.is_err() {
                return Err(result.unwrap_err().into());
            }
            Ok("Create table success".to_string())
        }
        Statement::Insert(i) => {
            let (table_name, values) = parse_insert(i)?;
            let result = write_data(default_db_name.to_string(), table_name, values);
            if result.is_err() {
                return Err(result.unwrap_err().into());
            }
            Ok("Insert table success".to_string())
        }
        _ => Err(format!("Unsupported type: {:?}", stmt).into()),
    }
}

pub fn extract_where_condition_from_query(query: &Query) -> Option<Expr> {
    match &*query.body {
        SetExpr::Select(select) => extract_where_condition_from_select(select.as_ref()),
        _ => None,
    }
}

// 提取 Select 中的 WHERE 表达式
fn extract_where_condition_from_select(select: &Select) -> Option<Expr> {
    select.selection.clone()
}

fn is_primary_key(options: Box<[ColumnOptionDef]>) -> bool {
    options.iter().map(|opt| opt.option.clone()).any(|opt| {
        matches!(
            opt,
            ColumnOption::Unique {
                is_primary: true,
                ..
            }
        )
    })
}

fn is_index(options: Box<[ColumnOptionDef]>) -> bool {
    // 如果需要支持显式创建索引，请扩展这部分逻辑
    false
}

pub fn parse_create_table(c: CreateTable) -> Result<(String, Vec<Field>), Box<dyn Error>> {
    let table_name = c.name.0.first().ok_or("Table name is empty")?.value.clone();

    let field_list: Vec<Field> = c
        .columns
        .iter()
        .map(|col| {
            let is_pk = is_primary_key(Box::from(col.options.clone()));
            let is_index = is_index(Box::from(col.options.clone()));

            Field::new(
                col.name.value.clone().into(),
                col.data_type.to_string(),
                is_pk,
                is_index,
            )
        })
        .collect();

    Ok((table_name, field_list))
}


fn extract_limit_from_query(query: &Query) -> Option<i32> {
    match &query.limit {
        Some(Expr::Value(sqlparser::ast::Value::Number(s, _))) => s.parse::<i32>().ok(),
        _ => None,
    }
}

// 提取整个 Query 中的字段名
pub fn extract_select_fields_from_query(query: &Query) -> Vec<String> {
    match &*query.body {
        SetExpr::Select(select) => extract_select_fields_from_select(select.as_ref()),
        _ => vec![],
    }
}

// 提取 Select 结构中的字段名
fn extract_select_fields_from_select(select: &Select) -> Vec<String> {
    select
        .projection
        .iter()
        .filter_map(|item| match item {
            SelectItem::UnnamedExpr(expr) => expr_to_string(expr),
            SelectItem::ExprWithAlias { expr, .. } => expr_to_string(expr),
            SelectItem::QualifiedWildcard(_, _) => Some("*".to_string()),
            SelectItem::Wildcard(_) => Some("*".to_string()),
        })
        .collect()
}

// 将表达式转成字符串表示（如列名）
fn expr_to_string(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Identifier(ident) => Some(ident.value.clone()),
        Expr::CompoundIdentifier(idents) => Some(
            idents
                .iter()
                .map(|i| i.value.clone())
                .collect::<Vec<String>>()
                .join("."),
        ),
        _ => None,
    }
}

fn extract_table_name_from_query(query: &Query) -> Option<String> {
    match &*query.body {
        SetExpr::Select(select) => extract_table_name_from_select(select.as_ref()),
        _ => None,
    }
}

fn extract_table_name_from_select(select: &Select) -> Option<String> {
    select
        .from
        .iter()
        .find_map(|t| extract_table_name_from_table_with_joins(t))
}

fn extract_table_name_from_table_with_joins(twj: &TableWithJoins) -> Option<String> {
    match &twj.relation {
        TableFactor::Table { ref name, .. } => name.0.first().map(|ident| ident.value.clone()),
        _ => None,
    }
}

// 提取 INSERT 语句中的表名和插入数据
pub fn parse_insert(insert: Insert) -> Result<(String, Vec<InsertValue>), Box<dyn Error>> {
    let table_name = insert
        .table_name
        .0
        .first()
        .ok_or("Empty table name in INSERT statement")?
        .value
        .clone();

    // 获取字段名
    let column_names = if let columns = insert.columns {
        columns
            .iter()
            .map(|ident| ident.value.clone())
            .collect::<Vec<String>>()
    } else {
        return Err("Missing column names in INSERT statement".into());
    };

    // 获取值列表
    let values: Vec<_> = if let Some(ref source) = insert.source {
        match **source {
            Query { ref body, .. } => {
                if let SetExpr::Values(ref values) = **body {
                    values
                        .rows
                        .iter()
                        .flat_map(|row| row.iter())
                        .filter_map(|expr| match expr {
                            Expr::Value(Value::SingleQuotedString(s)) => Some(s.clone()),
                            Expr::Value(Value::Number(s, _)) => Some(s.clone()),
                            _ => None,
                        })
                        .collect()
                } else {
                    return Err("Only VALUES are supported in INSERT statements".into());
                }
            }
        }
    } else {
        return Err("No source provided in INSERT statement".into());
    };

    // 检查字段数量和值数量是否一致
    if column_names.len() != values.len() {
        return Err("Column count does not match value count in INSERT statement".into());
    }
    //todo 考虑不指定列名的insert方式

    // 组合成 InsertValue 列表
    let insert_values: Vec<InsertValue> = column_names
        .into_iter()
        .zip(values.into_iter())
        .map(|(name, value)| InsertValue { name, value })
        .collect();

    Ok((table_name, insert_values))
}
#[cfg(test)]
mod test {
    use sqlparser::ast::Statement;

    use super::parse_insert;
    use crate::rocksdb_operation::InsertValue;
    use sqlparser::dialect::MySqlDialect;
    use sqlparser::parser::Parser;

    #[test]
    fn test_sql_parse_example() {
        let sql = "SELECT a, b, 123, myfunc(b) \
           FROM table_1 \
           WHERE a > b AND b < 100 \
           ORDER BY a DESC, b";

        let dialect = MySqlDialect {};
        let mut stmts = Parser::parse_sql(&dialect, sql).unwrap();
        println!("AST: {:?}", stmts);
        let stmt = stmts.swap_remove(0);
        println!(
            "{}",
            serde_json::to_string(&stmt).expect("serde for SchemaColumnInfo list error")
        );
        match stmt {
            Statement::Query(q) => println!("query struct: {:?}", q),
            _ => println!("Unsupported type:  {:?}", stmt),
        }
    }
    #[test]
    fn test_parse_insert_with_columns() {
        let sql = "INSERT INTO test (name, age) VALUES ('Doge', 25)";
        let dialect = MySqlDialect {};
        let mut stmts = Parser::parse_sql(&dialect, sql).unwrap();
        let stmt = stmts.swap_remove(0);
        if let Statement::Insert(insert) = stmt {
            let (table_name, values) = parse_insert(insert).unwrap();
            assert_eq!(table_name, "test");
            assert_eq!(
                values,
                vec![
                    InsertValue {
                        name: "name".to_string(),
                        value: "Doge".to_string(),
                    },
                    InsertValue {
                        name: "age".to_string(),
                        value: "25".to_string(),
                    }
                ]
            );
        } else {
            panic!("Expected INSERT statement");
        }
    }
}
