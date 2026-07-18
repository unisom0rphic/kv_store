use std::collections::HashMap;

type SharedTable = HashMap<String, String>;

#[derive(Debug)]
pub struct KvStore {
    table: SharedTable,
}

impl KvStore {
    pub fn new() -> Self {
        Self {
            table: HashMap::new(),
        }
    }

    // proper handling
    pub fn get(&mut self, key: &str) -> Option<String> {
        self.table.get(key).cloned()
    }

    pub fn set(&mut self, key: &str, value: &str) -> Option<String> {
        self.table.insert(String::from(key), String::from(value));
        Some("success".to_string())
    }

    pub fn delete(&mut self, key: &str) -> Option<String> {
        self.table.remove_entry(key);
        Some("success".to_string())
    }
}
