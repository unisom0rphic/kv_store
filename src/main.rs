mod conn;
mod executor;
mod storage;

use conn::open_connection;

#[tokio::main]
async fn main() {
    open_connection().await;
}
// use storage::KvStore;

// #[tokio::main]
// async fn main() {
//     let mut my_storage = KvStore::new();
//     my_storage.set("1", "2").await;
//     println!("Key-value pair: {:?}", my_storage);
//     println!("Key-value pair: {:?}", my_storage.get("1").await);
//     my_storage.delete("1").await;
//     println!("Key-value pair: {:?}", my_storage);

//     let mut my_storage2 = KvStore::new();
//     my_storage2.set("3", "4").await;
//     println!("NEW Key-value pair: {:?}", my_storage2);
// }
