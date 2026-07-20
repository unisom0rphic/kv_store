use kv_store::conn::open_connection;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[tokio::test]
async fn test_full_server_stack_via_tcp() {
    let server_addr = open_connection("127.0.0.1:0").await;

    let mut client_stream = TcpStream::connect(server_addr)
        .await
        .expect("Failed to connect to testing server");

    client_stream.write_all(b"SET key value\n").await.unwrap();

    let mut buffer = [0; 1024];
    let n = client_stream.read(&mut buffer).await.unwrap();
    let response = std::str::from_utf8(&buffer[..n]).unwrap();

    assert!(response.contains("success") || response.contains("Ok"));
}
