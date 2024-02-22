use futures::StreamExt;
use rand::prelude::*;
use scylla::statement::Consistency;
use scylla::transport::query_result::RowsExpectedError;
use scylla::transport::session::Session;
use scylla::transport::Compression;
use scylla::{FromRow, QueryResult, SessionBuilder};

use crate::types::*;

pub type Pool = ScyllaPool;

use self::pool::{Connection, ScyllaManager, ScyllaPool};

const MIGRATIONS: &[&str] = &[
    "DROP TABLE IF EXISTS ks.category;",
    "CREATE TABLE ks.category (
  id INT PRIMARY KEY,
  name TEXT
);",
    "DROP TABLE IF EXISTS ks.product;",
    "CREATE TABLE ks.product (
  id INT,
  name TEXT,
  description TEXT,
  category_id INT,
  PRIMARY KEY(id, category_id)
);",
    // "DROP INDEX IF EXISTS ks.product_score;",
    "DROP MATERIALIZED VIEW IF EXISTS ks.product_score;",
    "DROP TABLE IF EXISTS ks.cat_score;",
    "CREATE TABLE ks.cat_score (
  category_id INT,
  score FLOAT,
  product_id INT,
  PRIMARY KEY(category_id, product_id)
);",
    "CREATE MATERIALIZED VIEW ks.product_score AS SELECT * FROM ks.cat_score
    WHERE category_id IS NOT NULL AND product_id IS NOT NULL AND score IS NOT NULL
    primary key (category_id, score, product_id);
",
];

async fn migrate(db: &Session) -> color_eyre::Result<()> {
    db.query(
        "CREATE KEYSPACE IF NOT EXISTS ks WITH
replication = {'class' : 'SimpleStrategy', 'replication_factor' : 1}",
        &[],
    )
    .await?;

    for &q in MIGRATIONS {
        db.query(q, &[]).await?;
    }

    Ok(())
}

async fn populate(pool: &ScyllaPool) -> color_eyre::Result<()> {
    async fn make_category(pool: ScyllaPool, i: i32) {
        let mut conn = pool.get().await.unwrap();
        let mut q = conn
            .prepare("INSERT INTO ks.category (id, name) VALUES (?, ?)")
            .await
            .unwrap();
        q.set_consistency(Consistency::Any);

        conn.execute(&q, (i, format!("Category {}", i)))
            .await
            .unwrap();
    }

    async fn make_product(pool: ScyllaPool, i: i32) {
        let category_id = thread_rng().gen_range(1..=100);

        let mut conn = pool.get().await.unwrap();
        let mut q = conn
            .prepare(
                "INSERT INTO ks.product (id, name, description, category_id) VALUES (?, ?, ?, ?)",
            )
            .await
            .unwrap();
        q.set_consistency(Consistency::Any);
        conn.execute(
            &q,
            (
                i,
                format!("Product {}", i),
                format!("Description for Product {}", i),
                category_id,
            ),
        )
        .await
        .unwrap()
        .result_not_rows()
        .unwrap();

        let mut q = conn
            .prepare("INSERT INTO ks.cat_score (category_id, score, product_id) VALUES (?, ?, ?)")
            .await
            .unwrap();
        q.set_consistency(Consistency::Any);

        conn.execute(&q, (category_id, 0.0f32, i))
            .await
            .unwrap()
            .result_not_rows()
            .unwrap();
    }

    // Generate 100 categories
    let pool1 = pool.clone();
    futures::stream::iter((1..=100).map(|i| {
        let pool = pool1.clone();
        make_category(pool, i)
    }))
    .buffer_unordered(pool.status().max_size)
    .count()
    .await;
    log::info!("categories done.");

    // Generate one million products and assign them to a category
    let pool1 = pool.clone();
    futures::stream::iter((1..=1_000_000).map(|i| {
        let pool = pool1.clone();
        make_product(pool, i)
    }))
    .buffer_unordered(pool.status().max_size)
    .count()
    .await;

    log::info!("products done.");
    Ok(())
}

pub async fn db_init() -> color_eyre::Result<ScyllaPool> {
    let uri = std::env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());

    let pool = ScyllaPool::builder(ScyllaManager { uri })
        .max_size(64)
        .build()
        .unwrap();

    Ok(pool)
}

pub async fn db_setup() -> color_eyre::Result<()> {
    let uri = std::env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());

    let db: Session = SessionBuilder::new()
        .known_node(&uri)
        .compression(Some(Compression::Snappy))
        .build()
        .await?;

    if std::env::var("SCYLLA_INIT").map_or_else(|_| false, |s| s == "1") {
        log::info!("init start");
        migrate(&db).await?;

        let pool = ScyllaPool::builder(ScyllaManager { uri })
            .max_size(48)
            .build()
            .unwrap();

        log::info!("populating...");
        populate(&pool).await?;
    } else {
        log::info!("init skipped. set SCYLLA_INIT=1 to perform init");
    }

    log::info!("init complete");
    Ok(())
}

#[derive(FromRow)]
struct ProductRaw {
    id: i32,
    name: String,
    decscription: Option<String>,
    category_id: i32,
}

pub async fn get_product(db: &ScyllaPool, id: i32) -> color_eyre::Result<Option<Product>> {
    let mut db = db.get().await?;
    let q = db
        .prepare("SELECT id, name, description, category_id FROM ks.product WHERE id = ?")
        .await?;
    let Some(r1) = db.execute(&q, (id,)).await?.maybe_first_row_typed::<ProductRaw>()? else {
        return Ok(None);
    };

    assert_eq!(r1.id, id);
    let r2 = get_product_score(&mut db, r1.category_id, id).await?;

    let p = Product {
        id,
        name: r1.name,
        description: r1.decscription,
        category_id: r1.category_id,
        hits: r2 as i64,
    };

    Ok(Some(p))
}

async fn get_product_score(
    db: &mut Connection,
    category_id: i32,
    product_id: i32,
) -> Result<f32, color_eyre::Report> {
    assert!((1..=1_000_000).contains(&product_id));
    let mut q = db
        .prepare("SELECT score FROM ks.cat_score WHERE category_id = ? AND product_id = ?")
        .await?;
    q.set_consistency(Consistency::Quorum);

    let mut i = 0;
    let r2 = loop {
        if let Some(r2) = db
            .execute(&q, (category_id, product_id))
            .await?
            .maybe_first_row_typed::<(f32,)>()?
        {
            break r2;
        }
        log::error!("looping in get_product_score {product_id:5}!!! should never happen! ({i:2})");
        i += 1;
    };
    Ok(r2.0)
}

fn check_lwt(r: QueryResult) -> Result<bool, RowsExpectedError> {
    for row in r.rows()?.drain(..) {
        if !row.columns[0]
            .as_ref()
            .and_then(|c| c.as_boolean())
            .unwrap_or(false)
        {
            return Ok(false);
        }
    }
    Ok(true)
}

pub async fn mark_hit(db: &ScyllaPool, p: &Product) -> color_eyre::Result<()> {
    let mut db = db.get().await?;

    // let q_del = db
    //     .prepare("DELETE FROM ks.cat_score WHERE category_id = ? AND product_id = ? AND score = ? IF EXISTS").await?;
    // let q_ins = db
    //     .prepare("INSERT INTO ks.cat_score(category_id, product_id, score) VALUES (?, ?, ?) IF NOT EXISTS").await?;
    let q_upd = db
        .prepare("UPDATE ks.cat_score SET score = ? WHERE product_id = ? AND category_id = ? IF score = ?").await?;


    // let mut batch = Batch::default();
    // batch.append_statement(q_del);
    // batch.append_statement(q_ins);
    // batch.set_consistency(Consistency::Quorum);
    // batch.set_serial_consistency(Some(SerialConsistency::Serial));
    
    let mut i = 0;
    loop {
        let score = get_product_score(&mut db, p.category_id, p.id).await?;

        let result = db
            .execute(&q_upd, (score + 1.0, p.id, p.category_id, score))
            // .batch(
            //     &batch,
            //     (
            //         (p.category_id, p.id, score),
            //         (p.category_id, p.id, score + 1.0),
            //     ),
            // )
            .await?;

        log::debug!("batch statement result: {:?}", result);

        if check_lwt(result)? {
            break;
        } else {
            log::warn!(
                "conflict updating score for {:5}({:4}), updating ({i:3})",
                p.id,
                score
            );
        }
        i += 1;
    }
    Ok(())
}

pub async fn recommend_0(pool: &ScyllaPool, p: &Product) -> color_eyre::Result<Vec<Product>> {
    let mut db = pool.get().await?;
    let q = db
        .prepare(
            "SELECT product_id FROM ks.product_score WHERE category_id = ? ORDER BY score DESC LIMIT 5",
        )
        .await?;
    let r = db
        .execute(&q, (p.category_id,))
        .await?
        .rows_typed::<(i32,)>()?
        .map(|r| r.map(|q| q.0))
        .collect::<Result<Vec<_>, _>>()?;

    let mut res = Vec::with_capacity(r.len());
    for id in r {
        res.push(get_product(pool, id).await?.unwrap());
    }

    Ok(res)
}

// pub async fn recommend_1(db: &ScyllaPool, p: &Product) -> color_eyre::Result<Vec<Product>> {
//     let db = db.get().await?;
//     let q1 = db.prepare("SELECT tag_id FROM ks.product_tag WHERE product_id = ?").await?;
//     let q2 = db.prepare("SELECT product_id FROM ks.product_tag as t WHERE tag_id IN ? ORDER BY p.score DESC LIMIT 5")
//     let r = db.execute(&q1, (p.id,)).await?;

//     let p = r.rows_typed_or_empty::<Product>().map(Result::unwrap).collect();
//     Ok(p)
// }

pub mod pool {
    use std::ops::{Deref, DerefMut};
    use std::time::Duration;

    use async_trait::async_trait;
    use deadpool::managed;
    use quick_cache::unsync::Cache;
    use scylla::prepared_statement::PreparedStatement;
    use scylla::transport::errors::QueryError;
    use scylla::{transport::errors::NewSessionError, Session, SessionBuilder};

    pub struct ScyllaManager {
        pub uri: String,
    }

    pub struct Connection {
        session: Session,
        cache: Cache<&'static str, PreparedStatement>,
    }

    impl Connection {
        pub async fn prepare(
            &mut self,
            stmt: &'static str,
        ) -> Result<PreparedStatement, QueryError> {
            match self.cache.get_mut(stmt) {
                Some(stmt) => Ok(stmt.clone()),
                None => {
                    let p = self.session.prepare(stmt).await?;
                    self.cache.insert(stmt, p.clone());
                    Ok(p)
                }
            }
        }
    }

    impl Deref for Connection {
        type Target = Session;

        fn deref(&self) -> &Self::Target {
            &self.session
        }
    }

    impl DerefMut for Connection {
        fn deref_mut(&mut self) -> &mut Self::Target {
            &mut self.session
        }
    }

    #[async_trait]
    impl managed::Manager for ScyllaManager {
        type Type = Connection;
        type Error = NewSessionError;

        async fn create(&self) -> Result<Self::Type, Self::Error> {
            let session: Session = SessionBuilder::new()
                .known_node(&self.uri)
                .connection_timeout(Duration::from_secs(10))
                .build()
                .await?;

            let cache = Cache::new(100);
            Ok(Connection { session, cache })
        }

        async fn recycle(&self, _: &mut Self::Type) -> managed::RecycleResult<Self::Error> {
            Ok(())
        }
    }

    pub type ScyllaPool = managed::Pool<ScyllaManager>;
}
