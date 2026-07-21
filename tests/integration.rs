use kv_store::conn::open_connection;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[tokio::test]
async fn test_full_server_stack_via_tcp() {
    // This is a hack that unfortunately introduces an intermittent data race
    // Must modify open_connection function to match test environment too
    tokio::spawn(open_connection("127.0.0.1:6767"));

    let mut client_stream = TcpStream::connect("127.0.0.1:6767")
        .await
        .expect("Failed to connect to testing server");

    client_stream.write_all(b"SET key value\n").await.unwrap();

    let mut buffer = [0; 1024];
    let n = client_stream.read(&mut buffer).await.unwrap();
    let response = std::str::from_utf8(&buffer[..n]).unwrap();

    assert!(response.contains("success") || response.contains("Ok"));
}
