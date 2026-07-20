#[tokio::main]
async fn main() {
    kv_store::conn::open_connection("0.0.0.0:6767").await;
}
