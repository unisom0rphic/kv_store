use std::sync::Arc;

use tokio::sync::mpsc;

use crate::parser::Executor;
use crate::storage::KvStore;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
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

// unwraps handling
pub async fn open_connection() {
    let listener = TcpListener::bind("0.0.0.0:6767").await.unwrap();
    println!("Server listening on 0.0.0.0:6767");

    const CHANNEL_SIZE: usize = 20;
    let (tx, rx) = mpsc::channel(CHANNEL_SIZE);
    tokio::spawn(async move {
        let mut exec = Executor::new(KvStore::new(), rx);
        exec.run().await;
    });

    loop {
        let tx2 = tx.clone();
        let (socket, addr) = listener.accept().await.unwrap();
        println!("New client: {:?}", addr);
        tokio::spawn(async move {
            process(socket, tx2).await;
        });
    }
}

// SIGINT/SIGTERM handling
pub async fn process(mut stream: TcpStream, tx: mpsc::Sender<StoreRequest>) {
    let mut buffer = [0; 1024];

    // let (tx, rx) = mpsc::channel(CHANNEL_SIZE);
    // let mut exec = Executor::new(KvStore::new(), rx);

    // match inside?

    loop {
        // add timeout here
        let bytes_read = stream.read(&mut buffer).await.unwrap();

        if bytes_read == 0 {
            println!("Connection closed");
            break;
        }

        let parsed_data = Executor::parse(std::str::from_utf8(&buffer[..bytes_read]).unwrap());

        let parsed_cmd = match parsed_data {
            Ok(cmd) => cmd,
            Err(e) => {
                let msg = format!("Error parsing the data: {}", e);
                println!("{}", msg);
                let _ = stream.write_all(format!("{:?}\n", msg).as_bytes()).await;
                continue;
            }
        };
        println!("Parsed command: {:?}", parsed_cmd);

        let (otx, orx) = oneshot::channel();

        let request = StoreRequest {
            cmd: parsed_cmd,
            tx: otx,
        };

        tx.send(request)
            .await
            .expect("Failed to send data: channel closed");
        println!("Send request");

        let server_reply = match orx.await {
            Ok(d) => String::from_utf8(d),
            Err(e) => {
                println!("Error receiving the data: {}", e);
                return;
            }
        };
        println!("Received a response: {:?}", server_reply);

        let _ = stream
            .write_all(format!("{:?}\n", server_reply).as_bytes())
            .await;
    }
}
