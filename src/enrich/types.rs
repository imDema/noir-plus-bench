use serde::{Deserialize, Serialize};
use sqlx::FromRow as FromSqlxRow;

#[derive(Debug, Clone, FromSqlxRow)]
pub struct Category {
    pub id: i32,
    pub name: String,
    pub parent_category_id: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, FromSqlxRow)]
pub struct Product {
    pub id: i32,
    pub name: String,
    pub description: Option<String>,
    pub category_id: i32,
    pub hits: i64,
}

impl Product {
    pub fn from_pg_row(row: r2d2_postgres::postgres::Row) -> Self {
        Self {
            id: row.get("id"),
            name: row.get("name"),
            description: row.get("description"),
            category_id: row.get("category_id"),
            hits: row.get("hits"),
        }
    }
}
