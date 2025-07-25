use crate::MEMORY_MANAGER;
use ic_stable_structures::memory_manager::MemoryId;
use ic_turso_bindings::{Builder, Connection};
use std::rc::Rc;

#[ic_cdk::query]
async fn test(name: String) -> String {
    let memory = MEMORY_MANAGER.with_borrow(|m| m.get(MemoryId::new(1)));
    let db = Builder::with_memory(memory).build().await.unwrap();
    let conn = Rc::new(db.connect().unwrap());

    test_create_users_table(&conn).await;
    test_insert_sample_users(&conn, &name).await;
    test_create_logins_table(&conn).await;
    test_insert_logins(&conn, &name).await;
    test_create_messages_table(&conn).await;
    test_insert_messages(&conn, &name).await;
    test_count_users(&conn).await;
    test_count_messages(&conn).await;
    test_select_user_by_name(&conn, &name).await;
    test_select_messages_by_user(&conn, &name).await;
    test_bulk_insert_data(&conn).await;
    // test_create_index_on_users(&conn).await;
    test_delete_random_users(&conn).await;
    test_update_usernames(&conn).await;
    test_cleanup(&conn).await;

    format!("All tests completed for: {}", name)
}

async fn test_create_users_table(conn: &Connection) {
    conn.execute("CREATE TABLE IF NOT EXISTS users (name TEXT)", ())
        .await
        .unwrap();
    ic_cdk::println!("Created 'users' table");
}

async fn test_insert_sample_users(conn: &Connection, name: &str) {
    for i in 0..100 {
        let user = format!("{}_{}", name, i);
        conn.execute("INSERT INTO users (name) VALUES (?1)", [user])
            .await
            .unwrap();
    }
    ic_cdk::println!("Inserted 100 users");
}

async fn test_create_logins_table(conn: &Connection) {
    conn.execute("CREATE TABLE IF NOT EXISTS logins (user TEXT, ts TEXT)", ())
        .await
        .unwrap();
    ic_cdk::println!("Created 'logins' table");
}

async fn test_insert_logins(conn: &Connection, name: &str) {
    for i in 0..50 {
        let user = format!("{}_{}", name, i);
        conn.execute("INSERT INTO logins (user, ts) VALUES (?1, '')", [user])
            .await
            .unwrap();
    }
    ic_cdk::println!("Inserted 50 logins");
}

async fn test_create_messages_table(conn: &Connection) {
    conn.execute(
        "CREATE TABLE IF NOT EXISTS messages (sender TEXT, body TEXT)",
        (),
    )
    .await
    .unwrap();
    ic_cdk::println!("Created 'messages' table");
}

async fn test_insert_messages(conn: &Connection, name: &str) {
    for i in 0..2000 {
        let sender = format!("{}_{}", name, i % 100);
        let body = format!("Hello message number {}", i);
        conn.execute(
            "INSERT INTO messages (sender, body) VALUES (?1, ?2)",
            [sender, body],
        )
        .await
        .unwrap();
    }
    ic_cdk::println!("Inserted 2,000 messages");
}

async fn test_count_users(conn: &Connection) {
    let mut stmt = conn.prepare("SELECT COUNT(*) FROM users").await.unwrap();
    let mut rows = stmt.query(()).await.unwrap();
    let row = rows.next().await.unwrap().unwrap();
    let count: i64 = *row.get_value(0).unwrap().as_integer().unwrap();
    ic_cdk::println!("User count: {}", count);
}

async fn test_count_messages(conn: &Connection) {
    let mut stmt = conn.prepare("SELECT COUNT(*) FROM messages").await.unwrap();
    let mut rows = stmt.query(()).await.unwrap();
    let row = rows.next().await.unwrap().unwrap();
    let count: i64 = *row.get_value(0).unwrap().as_integer().unwrap();
    ic_cdk::println!("Message count: {}", count);
}

async fn test_select_user_by_name(conn: &Connection, name: &str) {
    let mut stmt = conn
        .prepare("SELECT * FROM users WHERE name LIKE ?1")
        .await
        .unwrap();
    let pattern = format!("{}_%", name);
    let mut rows = stmt.query([pattern]).await.unwrap();
    let mut count = 0;
    while rows.next().await.unwrap().is_some() {
        count += 1;
    }
    ic_cdk::println!("Selected {} users by name pattern", count);
}

async fn test_select_messages_by_user(conn: &Connection, name: &str) {
    let sender = format!("{}_1", name);
    let mut stmt = conn
        .prepare("SELECT body FROM messages WHERE sender = ?1")
        .await
        .unwrap();
    let mut rows = stmt.query([sender.clone()]).await.unwrap();
    let mut count = 0;
    while let Some(row) = rows.next().await.unwrap() {
        let _ = row.get_value(0).unwrap().as_text().unwrap();
        count += 1;
    }
    ic_cdk::println!("Found {} messages by sender '{}'", count, sender);
}

async fn test_bulk_insert_data(conn: &Connection) {
    for i in 0..1000 {
        let name = format!("bulk_user_{}", i);
        conn.execute("INSERT INTO users (name) VALUES (?1)", [name])
            .await
            .unwrap();
    }
    ic_cdk::println!("Bulk inserted 1000 users");
}

async fn test_create_index_on_users(conn: &Connection) {
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_users_name ON users(name)",
        (),
    )
    .await
    .unwrap();
    ic_cdk::println!("Created index on users.name");
}

async fn test_delete_random_users(conn: &Connection) {
    conn.execute("DELETE FROM users WHERE name LIKE 'bulk_user_%7'", ())
        .await
        .unwrap();
    ic_cdk::println!("Deleted some users with name like 'bulk_user_%7'");
}

async fn test_update_usernames(conn: &Connection) {
    conn.execute(
        "UPDATE users SET name = 'updated_user' WHERE name = 'bulk_user_1'",
        (),
    )
    .await
    .unwrap();
    ic_cdk::println!("Updated username for 'bulk_user_1'");
}

async fn test_cleanup(conn: &Connection) {
    conn.execute("DROP TABLE IF EXISTS users", ())
        .await
        .unwrap();
    conn.execute("DROP TABLE IF EXISTS logins", ())
        .await
        .unwrap();
    conn.execute("DROP TABLE IF EXISTS messages", ())
        .await
        .unwrap();
}
