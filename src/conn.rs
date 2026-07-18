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

pub async fn open_connection() {
    let listener = TcpListener::bind("0.0.0.0:6767").await.unwrap();
    println!("Server listening on 0.0.0.0:6767");

    loop {
        let (socket, addr) = listener.accept().await.unwrap();
        println!("New client: {:?}", addr);
        tokio::spawn(async {
            process(socket).await;
        });
    }
}

pub async fn process(mut stream: TcpStream) {
    let mut buffer = [0; 1024];

    loop {
        let bytes_read = stream.read(&mut buffer).await.unwrap();

        if bytes_read == 0 {
            println!("Connection closed");
            break;
        }

        println!("Received: {:?}", &buffer[..bytes_read]);
        stream.write_all("got you".as_bytes()).await.unwrap();
    }
}

//     while let Ok((mut stream, addr)) = listener.accept().await {
//         println!("✅ Connection accepted from {}", addr);

//         const CHANNEL_SIZE: usize = 20;
//         let (tx, rx) = mpsc::channel(CHANNEL_SIZE);
//         let mut exec = Executor::new(KvStore::new(), rx);

//         tokio::spawn(async move { exec.run().await });

//         let _ = tokio::spawn(async move {
//             let mut buffer = [0; 1024];
//             const TIMEOUT_SECONDS: u64 = 5;

//             match timeout(
//                 Duration::from_secs(TIMEOUT_SECONDS),
//                 stream.read(&mut buffer),
//             )
//             .await
//             {
//                 Ok(Ok(0)) => println!("Client disconnected without sending data"),
//                 Ok(Ok(n)) => {
//                     println!("📥 Received {} bytes: {:?}", n, &buffer[..n]);

//                     let parsed_data = Executor::parse(std::str::from_utf8(&buffer[..n]).unwrap());

//                     let parsed_cmd = match parsed_data {
//                         Ok(cmd) => cmd,
//                         Err(e) => {
//                             let msg = format!("Error parsing the data: {}", e);
//                             println!("{}", msg);
//                             let _ = stream.write_all(format!("{:?}", msg).as_bytes()).await;
//                             return;
//                         }
//                     };
//                     println!("Parsed command: {:?}", parsed_cmd);

//                     let (otx, orx) = oneshot::channel();

//                     let request = StoreRequest {
//                         cmd: parsed_cmd,
//                         tx: otx,
//                     };

//                     tx.send(request)
//                         .await
//                         .expect("Failed to send data: channel closed");
//                     println!("Send request");

//                     let server_reply = match orx.await {
//                         Ok(d) => String::from_utf8(d),
//                         Err(e) => {
//                             println!("Error receiving the data: {}", e);
//                             return;
//                         }
//                     };
//                     println!("Received a response: {:?}", server_reply);

//                     let _ = stream
//                         .write_all(format!("{:?}", server_reply).as_bytes())
//                         .await;
//                 }
//                 Ok(Err(e)) => eprintln!("Read error: {}", e),
//                 Err(_) => println!("⏰ Timeout: client didn't send data within {TIMEOUT_SECONDS}s"),
//             }
//         })
//         .await;
//     }
// }
