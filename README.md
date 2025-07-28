# ic-turso-demo

This app is a proof of concept and an exploration, looking at how the SQLite compatible database [Turso](https://github.com/tursodatabase/turso) can be run on ICP.

```bash
dfx start --clean --background
dfx deploy
dfx canister call ic-turso-demo test '("A name")'
```

The test endpoint roughly makes 3 000 inserts and some queries.

Output:

```bash
❯ dfx canister call ic-turso-demo test '("A name")'
Created 'users' table
Inserted 100 users
First user: name = A name_0, created = 2025-07-28 11:58:46
Created 'logins' table
Inserted 50 logins
Created 'messages' table
Inserted 2,000 messages
User count: 100
Message count: 2000
Selected 100 users by name pattern
Found 20 messages by sender 'A name_1'
Bulk inserted 1000 users
Created index on users.name
Deleted some users with name like 'bulk_user_%7'
Updated username for 'bulk_user_1'
("All tests completed for: A name")
```
