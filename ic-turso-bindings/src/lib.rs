//! # Turso bindings for Rust
//!
//! Turso is an in-process SQL database engine, compatible with SQLite.
//!
//! ## Getting Started
//!
//! To get started, you first need to create a [`Database`] object and then open a [`Connection`] to it, which you use to query:
//!
//! ```rust,no_run
//! # async fn run() {
//! use turso::Builder;
//!
//! let db = Builder::new_local(":memory:").build().await.unwrap();
//! let conn = db.connect().unwrap();
//! conn.execute("CREATE TABLE IF NOT EXISTS users (email TEXT)", ()).await.unwrap();
//! conn.execute("INSERT INTO users (email) VALUES ('alice@example.org')", ()).await.unwrap();
//! # }
//! ```
//!
//! You can also prepare statements with the [`Connection`] object and then execute the [`Statement`] objects:
//!
//! ```rust,no_run
//! # async fn run() {
//! # use turso::Builder;
//! # let db = Builder::new_local(":memory:").build().await.unwrap();
//! # let conn = db.connect().unwrap();
//! let mut stmt = conn.prepare("SELECT * FROM users WHERE email = ?1").await.unwrap();
//! let mut rows = stmt.query(["foo@example.com"]).await.unwrap();
//! let row = rows.next().await.unwrap().unwrap();
//! let value = row.get_value(0).unwrap();
//! println!("Row: {:?}", value);
//! # }
//! ```

pub mod params;
pub mod stable_io;
pub mod transaction;
pub mod value;

use ic_stable_structures::{memory_manager::VirtualMemory, Ic0StableMemory};
use transaction::TransactionBehavior;
use turso_core::OpenFlags;
pub use value::Value;

pub use params::params_from_iter;

use crate::params::*;
use crate::stable_io::{StableDatabaseStorage, StableIO};
use std::fmt::Debug;
use std::num::NonZero;
use std::sync::{Arc, Mutex};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("SQL conversion failure: `{0}`")]
    ToSqlConversionFailure(BoxError),
    #[error("Mutex lock error: {0}")]
    MutexError(String),
    #[error("SQL execution failure: `{0}`")]
    SqlExecutionFailure(String),
}

impl From<turso_core::LimboError> for Error {
    fn from(err: turso_core::LimboError) -> Self {
        Error::SqlExecutionFailure(err.to_string())
    }
}

pub(crate) type BoxError = Box<dyn std::error::Error + Send + Sync>;

pub type Result<T> = std::result::Result<T, Error>;

/// A builder for `Database`.
pub struct Builder {
    virtual_memory: VirtualMemory<Ic0StableMemory>,
}

impl Builder {
    /// Create a new local database.
    pub fn with_memory(virtual_memory: VirtualMemory<Ic0StableMemory>) -> Self {
        Self { virtual_memory }
    }

    /// Build the database.
    #[allow(unused_variables, clippy::arc_with_non_send_sync)]
    pub async fn build(self) -> Result<Database> {
        let path = "db";
        let io: Arc<dyn turso_core::IO> = Arc::new(StableIO::new(self.virtual_memory));
        let file = io.open_file(path, OpenFlags::Create, false).unwrap();
        let db_file = Arc::new(StableDatabaseStorage::new(file));
        let db = turso_core::Database::open(io, path, db_file, false, true).unwrap();
        Ok(Database { inner: db })
    }
}

/// A database.
///
/// The `Database` object points to a database and allows you to connect to it
#[derive(Clone)]
pub struct Database {
    inner: Arc<turso_core::Database>,
}

unsafe impl Send for Database {}
unsafe impl Sync for Database {}

impl Debug for Database {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Database").finish()
    }
}

impl Database {
    /// Connect to the database.
    pub fn connect(&self) -> Result<Connection> {
        let conn = self.inner.connect()?;
        #[allow(clippy::arc_with_non_send_sync)]
        let connection = Connection {
            inner: Arc::new(Mutex::new(conn)),
            transaction_behavior: TransactionBehavior::Deferred,
        };
        Ok(connection)
    }
}

/// A database connection.
pub struct Connection {
    inner: Arc<Mutex<Arc<turso_core::Connection>>>,
    transaction_behavior: TransactionBehavior,
}

impl Clone for Connection {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            transaction_behavior: self.transaction_behavior,
        }
    }
}

unsafe impl Send for Connection {}
unsafe impl Sync for Connection {}

impl Connection {
    /// Query the database with SQL.
    pub async fn query(&self, sql: &str, params: impl IntoParams) -> Result<Rows> {
        let mut stmt = self.prepare(sql).await?;
        stmt.query(params).await
    }

    /// Execute SQL statement on the database.
    pub async fn execute(&self, sql: &str, params: impl IntoParams) -> Result<u64> {
        let mut stmt = self.prepare(sql).await?;
        stmt.execute(params).await
    }

    /// Prepare a SQL statement for later execution.
    pub async fn prepare(&self, sql: &str) -> Result<Statement> {
        let conn = self
            .inner
            .lock()
            .map_err(|e| Error::MutexError(e.to_string()))?;

        let stmt = conn.prepare(sql)?;

        #[allow(clippy::arc_with_non_send_sync)]
        let statement = Statement {
            inner: Arc::new(Mutex::new(stmt)),
        };
        Ok(statement)
    }

    /// Query a pragma.
    pub fn pragma_query<F>(&self, pragma_name: &str, mut f: F) -> Result<()>
    where
        F: FnMut(&Row) -> turso_core::Result<()>,
    {
        let conn = self
            .inner
            .lock()
            .map_err(|e| Error::MutexError(e.to_string()))?;

        let rows: Vec<Row> = conn
            .pragma_query(pragma_name)
            .map_err(|e| Error::SqlExecutionFailure(e.to_string()))?
            .iter()
            .map(|row| row.iter().collect::<Row>())
            .collect();

        rows.iter().try_for_each(|row| {
            f(row).map_err(|e| {
                Error::SqlExecutionFailure(format!("Error executing user defined function: {e}"))
            })
        })?;
        Ok(())
    }

    /// Flush dirty pages to disk.
    /// This will write the dirty pages to the WAL.
    pub fn cacheflush(&self) -> Result<()> {
        let conn = self
            .inner
            .lock()
            .map_err(|e| Error::MutexError(e.to_string()))?;
        conn.cacheflush()?;
        Ok(())
    }

    pub fn is_autocommit(&self) -> Result<bool> {
        let conn = self
            .inner
            .lock()
            .map_err(|e| Error::MutexError(e.to_string()))?;

        Ok(conn.get_auto_commit())
    }
}

impl Debug for Connection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Connection").finish()
    }
}

/// A prepared statement.
pub struct Statement {
    inner: Arc<Mutex<turso_core::Statement>>,
}

impl Clone for Statement {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

unsafe impl Send for Statement {}
unsafe impl Sync for Statement {}

impl Statement {
    /// Query the database with this prepared statement.
    pub async fn query(&mut self, params: impl IntoParams) -> Result<Rows> {
        let params = params.into_params()?;
        match params {
            params::Params::None => (),
            params::Params::Positional(values) => {
                for (i, value) in values.into_iter().enumerate() {
                    let mut stmt = self.inner.lock().unwrap();
                    stmt.bind_at(NonZero::new(i + 1).unwrap(), value.into());
                }
            }
            params::Params::Named(values) => {
                for (name, value) in values.into_iter() {
                    let mut stmt = self.inner.lock().unwrap();
                    let i = stmt.parameters().index(name).unwrap();
                    stmt.bind_at(i, value.into());
                }
            }
        }
        #[allow(clippy::arc_with_non_send_sync)]
        let rows = Rows {
            inner: Arc::clone(&self.inner),
        };
        Ok(rows)
    }

    /// Execute this prepared statement.
    pub async fn execute(&mut self, params: impl IntoParams) -> Result<u64> {
        {
            // Reset the statement before executing
            self.inner.lock().unwrap().reset();
        }
        let params = params.into_params()?;
        match params {
            params::Params::None => (),
            params::Params::Positional(values) => {
                for (i, value) in values.into_iter().enumerate() {
                    let mut stmt = self.inner.lock().unwrap();
                    stmt.bind_at(NonZero::new(i + 1).unwrap(), value.into());
                }
            }
            params::Params::Named(values) => {
                for (name, value) in values.into_iter() {
                    let mut stmt = self.inner.lock().unwrap();
                    let i = stmt.parameters().index(name).unwrap();
                    stmt.bind_at(i, value.into());
                }
            }
        }
        loop {
            let mut stmt = self.inner.lock().unwrap();
            match stmt.step() {
                Ok(turso_core::StepResult::Row) => {
                    // unexpected row during execution, error out.
                    return Ok(2);
                }
                Ok(turso_core::StepResult::Done) => {
                    return Ok(0);
                }
                Ok(turso_core::StepResult::IO) => {
                    let _ = stmt.run_once();
                    //return Ok(1);
                }
                Ok(turso_core::StepResult::Busy) => {
                    return Ok(4);
                }
                Ok(turso_core::StepResult::Interrupt) => {
                    return Ok(3);
                }
                Err(err) => {
                    return Err(err.into());
                }
            }
        }
    }

    /// Returns columns of the result of this prepared statement.
    pub fn columns(&self) -> Vec<Column> {
        let stmt = self.inner.lock().unwrap();

        let n = stmt.num_columns();

        let mut cols = Vec::with_capacity(n);

        for i in 0..n {
            let name = stmt.get_column_name(i).into_owned();
            cols.push(Column {
                name,
                decl_type: None, // TODO
            });
        }

        cols
    }
}

/// Column information.
pub struct Column {
    name: String,
    decl_type: Option<String>,
}

impl Column {
    /// Return the name of the column.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the type of the column.
    pub fn decl_type(&self) -> Option<&str> {
        self.decl_type.as_deref()
    }
}

pub trait IntoValue {
    fn into_value(self) -> Result<Value>;
}

#[derive(Debug, Clone)]
pub enum Params {
    None,
    Positional(Vec<Value>),
    Named(Vec<(String, Value)>),
}

pub struct Transaction {}

/// Results of a prepared statement query.
pub struct Rows {
    inner: Arc<Mutex<turso_core::Statement>>,
}

impl Clone for Rows {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

unsafe impl Send for Rows {}
unsafe impl Sync for Rows {}

impl Rows {
    /// Fetch the next row of this result set.
    pub async fn next(&mut self) -> Result<Option<Row>> {
        loop {
            let mut stmt = self
                .inner
                .lock()
                .map_err(|e| Error::MutexError(e.to_string()))?;
            match stmt.step() {
                Ok(turso_core::StepResult::Row) => {
                    let row = stmt.row().unwrap();
                    return Ok(Some(Row {
                        values: row.get_values().map(|v| v.to_owned()).collect(),
                    }));
                }
                Ok(turso_core::StepResult::Done) => return Ok(None),
                Ok(turso_core::StepResult::IO) => {
                    if let Err(e) = stmt.run_once() {
                        return Err(e.into());
                    }
                    continue;
                }
                Ok(turso_core::StepResult::Busy) => return Ok(None),
                Ok(turso_core::StepResult::Interrupt) => return Ok(None),
                _ => return Ok(None),
            }
        }
    }
}

/// Query result row.
#[derive(Debug)]
pub struct Row {
    values: Vec<turso_core::Value>,
}

unsafe impl Send for Row {}
unsafe impl Sync for Row {}

impl Row {
    pub fn get_value(&self, index: usize) -> Result<Value> {
        let value = &self.values[index];
        match value {
            turso_core::Value::Integer(i) => Ok(Value::Integer(*i)),
            turso_core::Value::Null => Ok(Value::Null),
            turso_core::Value::Float(f) => Ok(Value::Real(*f)),
            turso_core::Value::Text(text) => Ok(Value::Text(text.to_string())),
            turso_core::Value::Blob(items) => Ok(Value::Blob(items.to_vec())),
        }
    }

    pub fn column_count(&self) -> usize {
        self.values.len()
    }
}

impl<'a> FromIterator<&'a turso_core::Value> for Row {
    fn from_iter<T: IntoIterator<Item = &'a turso_core::Value>>(iter: T) -> Self {
        let values = iter
            .into_iter()
            .map(|v| match v {
                turso_core::Value::Integer(i) => turso_core::Value::Integer(*i),
                turso_core::Value::Null => turso_core::Value::Null,
                turso_core::Value::Float(f) => turso_core::Value::Float(*f),
                turso_core::Value::Text(s) => turso_core::Value::Text(s.clone()),
                turso_core::Value::Blob(b) => turso_core::Value::Blob(b.clone()),
            })
            .collect();

        Row { values }
    }
}
