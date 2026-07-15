use crate::parser::Executor;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::time::{Duration, timeout};

#[derive(Debug, PartialEq, Clone)]
pub enum Command {
    Set { key: String, value: String },
    Get { key: String },
    Delete { key: String },
}

pub struct StoreRequest {
    pub cmd: Command,
    pub tx: oneshot::Sender<Vec<u8>>,
}

pub async fn open_connection() {
    let listener = TcpListener::bind("0.0.0.0:6767").await.unwrap();
    println!("Server listening on 0.0.0.0:6767");

    if let Ok((mut stream, addr)) = listener.accept().await {
        println!("✅ Connection accepted from {}", addr);

        let _ = tokio::spawn(async move {
            let mut buffer = [0; 1024];

            match timeout(Duration::from_secs(5), stream.read(&mut buffer)).await {
                Ok(Ok(0)) => println!("Client disconnected without sending data"),
                Ok(Ok(n)) => {
                    println!("📥 Received {} bytes: {:?}", n, &buffer[..n]);
                    let parsed_data = Executor::parse(std::str::from_utf8(&buffer[..n]).unwrap());
                    let _ = stream
                        .write_all(format!("{:?}", parsed_data.unwrap()).as_bytes())
                        .await;
                }
                Ok(Err(e)) => eprintln!("Read error: {}", e),
                Err(_) => println!("⏰ Timeout: client didn't send data within 5s"),
            }
        })
        .await;
    }
}
