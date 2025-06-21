use std::error::Error;

pub struct CreateTableNode {
    pub(crate) table_name: String,
    pub(crate) field_list: Vec<String>,
}

pub struct InsertNode {
    pub(crate) table_name: String,
    pub(crate) data_list: Vec<String>,
}

pub struct SelectNode {
    pub(crate) table_name: String,
    pub(crate) fields: Vec<String>,
    pub(crate) limit: i32,
}

pub fn parse_create_table(sql: &str) -> Result<CreateTableNode, Box<dyn Error>> {
    let sql = sql.to_lowercase().replace(";", "");
    let tokens: Vec<&str> = sql.split_whitespace().collect();

    if tokens.len() < 3 || tokens[0] != "create" || tokens[1] != "table" {
        return Err("Invalid CREATE TABLE syntax".into());
    }

    let table_name = tokens[2].to_string();

    // 找到左括号的位置
    let Some(left_paren) = sql.find('(') else {
        return Err("Missing opening parenthesis in CREATE TABLE statement".into());
    };

    // 找到右括号的位置
    let Some(right_paren) = sql.find(')') else {
        return Err("Missing closing parenthesis in CREATE TABLE statement".into());
    };

    // 提取括号内的字段定义部分
    let fields_content = &sql[left_paren + 1..right_paren];

    // 分割字段定义
    let mut field_list = Vec::new();
    for field_def in fields_content.split(',') {
        let parts: Vec<&str> = field_def
            .split_whitespace()
            .filter(|s| !s.is_empty())
            .collect();

        if parts.len() >= 2 {
            let name = parts[0].to_string();
            //类型我们先不管，全部当string
            field_list.push(name);
        }
    }

    Ok(CreateTableNode {
        table_name,
        field_list,
    })
}

pub fn parse_select(sql: &str) -> Result<SelectNode, Box<dyn Error>> {
    let sql = sql.to_lowercase().replace(";", "");
    let tokens: Vec<&str> = sql.split_whitespace().collect();

    if tokens.is_empty() || tokens[0] != "select" {
        return Err("Invalid SELECT syntax: missing 'SELECT'".into());
    }

    // 找到 "from" 关键字的位置
    let from_index = match tokens.iter().position(|&t| t == "from") {
        Some(index) => index,
        None => return Err("Invalid SELECT syntax: missing 'FROM'".into()),
    };

    // 提取字段部分（在 select 和 from 之间）
    let fields = tokens[1..from_index]
        .iter()
        .flat_map(|&token| token.split(','))
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect::<Vec<String>>();

    if fields.is_empty() {
        return Err("No fields specified in SELECT statement".into());
    }

    // 表名必须在 "from" 之后
    let table_name = if from_index + 1 < tokens.len() {
        tokens[from_index + 1].to_string()
    } else {
        return Err("Missing table name after 'FROM'".into());
    };

    // 检查是否有 limit
    let mut limit = 0;
    if let Some(limit_index) = tokens.iter().position(|&t| t == "limit") {
        if limit_index + 1 < tokens.len() {
            let limit_parse_result = tokens[limit_index + 1].parse::<i32>();
            if (limit_parse_result.is_err()) {
                return Err("Parse limit error".into());
            }
            limit = limit_parse_result.unwrap();
        } else {
            return Err("Missing value after 'LIMIT'".into());
        }
    }

    Ok(SelectNode {
        table_name,
        fields,
        limit,
    })
}

pub fn parse_insert(sql: &str) -> Result<InsertNode, Box<dyn Error>> {
    let mut sql = sql.replace(";", "");
    sql.retain(|c| !['(', ')', '\''].contains(&c)); // 去掉括号和单引号

    let tokens: Vec<&str> = sql.split_whitespace().collect();

    if tokens.len() < 5
        || tokens[0].to_lowercase() != "insert"
        || tokens[1].to_lowercase() != "into"
        || tokens[3].to_lowercase() != "values"
    {
        return Err("Invalid INSERT syntax".into());
    }

    let table_name = tokens[2].to_string();

    let mut data_list = Vec::new();

    for chunk in tokens[4..].iter() {
        let mut chunk_str = chunk.to_string();
        chunk_str.retain(|c| ![','].contains(&c));
        data_list.push(chunk_str);
    }

    Ok(InsertNode {
        table_name: table_name,
        data_list: data_list,
    })
}

#[cfg(test)]
mod test {
    use crate::sql_parse::{parse_create_table, parse_insert, parse_select};

    #[test]
    fn test_parse_insert() {
        let insert_sql =
            "insert into test1 values ('Southborough', 'MA', 'United_States', '9686');";

        let result = parse_insert(insert_sql);
        assert_eq!(result.is_ok(), true);
        let node = result.unwrap();
        assert_eq!(node.table_name, "test1");
        assert_eq!(
            node.data_list,
            vec!["Southborough", "MA", "United_States", "9686"]
        )
    }

    #[test]
    fn test_parse_insert_fail() {
        let insert_sql =
            "insert into test1 vlaues ('Southborough', 'MA', 'United_States', '9686');";

        let result = parse_insert(insert_sql);
        assert_eq!(result.is_err(), true);
    }

    #[test]
    fn test_parse_select() {
        let select_sql = "select f1, f2 from test1 limit 10;";
        let result = parse_select(select_sql);
        assert_eq!(result.is_ok(), true, "error is {:#?}", result.err());
        let node = result.unwrap();
        assert_eq!(node.table_name, "test1");
        assert_eq!(node.limit, 10);
        assert_eq!(node.fields, vec!["f1", "f2"]);
    }
    
    #[test]
    fn test_parse_select_fail() {
        let select_sql = "seletc f1, f2 from test1 limit 10;";
        let result = parse_select(select_sql);
        assert_eq!(result.is_err(), true);
    }
    
    #[test]
    fn test_parse_create_table() {
        let create_sql = "CREATE TABLE users (id INT, name TEXT);";
    
        let result = parse_create_table(create_sql);
        assert!(result.is_ok());
    
        let node = result.unwrap();
        assert_eq!(node.table_name, "users");
        assert_eq!(node.field_list.len(), 2);
        assert_eq!(node.field_list[0], "id");
        assert_eq!(node.field_list[1], "name");
    }
    
    #[test]
    fn test_parse_create_table_missing_paren() {
        let create_sql = "CREATE TABLE users id INT, name TEXT);";
        let result = parse_create_table(create_sql);
        assert!(result.is_err());
    }
}
