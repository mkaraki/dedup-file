use std::hash::Hasher;
use std::io::Read;
use colored::Colorize;
use rusqlite::{Connection};
use twox_hash::XxHash64;
use time;

struct DuplicateEntry {
    group: i64,
    path: String,
    mtime: i64,
}

fn process_dir(conn: &Connection, path: &str) {
    let mut loader_i: u64 = 0;

    let list = std::fs::read_dir(path).unwrap();

    for entry in list {
        let entry = entry.unwrap();
        let path = entry.path();
        let full_path = path.to_str().unwrap();

        if path.is_dir() {
            process_dir(conn, full_path);
        } else {
            let file_meta = entry.metadata().unwrap();
            let mtime = file_meta.modified().unwrap();
            let mtime = mtime.duration_since(std::time::UNIX_EPOCH).unwrap().as_secs() as i64;
            let size = file_meta.len() as i64;

            let mut hasher = XxHash64::with_seed(0);

            {
                let mut file = std::fs::File::open(full_path).unwrap();

                loop {
                    let mut buf = [0; 4096];
                    let read_size = file.read(&mut buf).unwrap();
                    if read_size == 0 {
                        break;
                    }
                    hasher.write(&buf[..read_size]);
                }
            }

            let hash = hasher.finish() as i64;

            conn.execute(
                "INSERT OR REPLACE INTO files (path, size, mtime, hash) VALUES (?1, ?2, ?3, ?4)",
                rusqlite::params![
                    full_path,
                    size,
                    mtime,
                    hash
                ],
            ).unwrap();

            loader_i += 1;
            eprint!("Loading: {}\r", ["|", "/", "-", "\\"][((loader_i) % 4) as usize]);
        }
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();

    if args.len() != 2 {
        eprintln!("Usage: {} <input>", args[0]);
        std::process::exit(1);
    }

    let path = args[1].clone();

    let conn = Connection::open_in_memory().unwrap();

    conn.execute(
        "CREATE TABLE IF NOT EXISTS files (
            path TEXT PRIMARY KEY,
            size BIGINT NOT NULL,
            mtime BIGINT NOT NULL,
            hash BIGINT NOT NULL
        )",
        [],
    ).unwrap();

    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_files_size ON files (size)",
        [],
    ).unwrap();

    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_files_hash ON files (hash)",
        [],
    ).unwrap();

    process_dir(&conn, &*path);

    // This will erase the loading animation
    eprint!("          \r");

    let mut hash_dup_statement = conn.prepare(
        concat!(
            "SELECT hash, path, mtime FROM files WHERE hash IN (",
                "SELECT hash FROM files GROUP BY hash HAVING COUNT(*) > 1",
            ") ORDER BY hash ASC, path ASC")
    ).unwrap();

    let mut last_hash: i64 = -1;
    let mut last_hash_id = 0;

    let dup_iter = hash_dup_statement.query_map([], |row| {
        let hash: i64 = row.get(0).unwrap();

        if hash != last_hash {
            last_hash = hash;
            last_hash_id += 1;
        }

        let group = last_hash_id;
        let path: String = row.get(1).unwrap();
        let mtime: i64 = row.get(2).unwrap();

        Ok(DuplicateEntry { group, path, mtime })
    }).unwrap();

    println!("Group\tPath\tLast Modified");
    for dup in dup_iter {
        let dup = dup.unwrap();
        let group = dup.group;
        let group_str = group.to_string();
        let mtime = time::OffsetDateTime::from_unix_timestamp(dup.mtime).unwrap();
        let formatted = format!("{}\t{}\t{}", group_str, dup.path, mtime.to_string());
        if group % 2 == 0 {
            println!("{}", formatted.bright_white().on_black());
        } else {
            println!("{}", formatted.black().on_white());
        }
    }
}
