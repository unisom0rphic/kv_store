use std::{collections::HashMap, sync::Arc};
use tokio::sync::RwLock;

type SharedTable = Arc<RwLock<HashMap<String, String>>>;

#[derive(Debug)]
pub struct KvStore {
    table: SharedTable,
}

impl KvStore {
    pub fn new() -> Self {
        Self {
            table: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn get(&mut self, key: &str) -> Option<String> {
        let handle = Arc::clone(&self.table);

        if let Ok(read_guard) = handle.try_read() {
            return read_guard.get(key).cloned();
        }

        None
    }

    pub async fn set(&mut self, key: &str, value: &str) -> Option<String> {
        let handle = Arc::clone(&self.table);

        if let Ok(mut write_guard) = handle.try_write() {
            write_guard.insert(String::from(key), String::from(value));
            Some(String::from("success"))
        } else {
            None
        }
    }

    pub async fn delete(&mut self, key: &str) -> Option<String> {
        let handle = Arc::clone(&self.table);

        if let Ok(mut write_guard) = handle.try_write() {
            write_guard.remove_entry(key);
            Some(String::from("success"))
        } else {
            None
        }
    }
}
