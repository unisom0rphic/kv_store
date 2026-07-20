use tokio::signal;
use tokio::sync::mpsc;

use crate::executor::Executor;
use crate::storage::KvStore;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;

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
    let listener = TcpListener::bind("0.0.0.0:6767")
        .await
        .expect("TCP binding failed");
    println!("Server listening on 0.0.0.0:6767");

    const CHANNEL_SIZE: usize = 20;
    let (tx, rx) = mpsc::channel(CHANNEL_SIZE);
    tokio::spawn(async move {
        let mut exec = Executor::new(KvStore::new(), rx);
        exec.run().await;
    });

    loop {
        tokio::select! {
            res = listener.accept() => {
                let (socket, addr) = res.expect("Client unable to connect");
                let tx2 = tx.clone();
                println!("New client: {:?}", addr);
                tokio::spawn(async move {
                    process(socket, tx2).await;
                });
            }
            _ = shutdown_signal_handler() => {
                println!("Received termination signal: exiting...");
                break;
        }}
    }
}

async fn shutdown_signal_handler() {
    let ctrl_c = async {
        signal::ctrl_c().await.expect("Failed handling Ctrl+C");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("Failed handling SIGTERM")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {}
    }
}

async fn process(mut stream: TcpStream, tx: mpsc::Sender<StoreRequest>) {
    let mut buffer = [0; 1024];

    loop {
        // add timeout here
        let bytes_read = stream
            .read(&mut buffer)
            .await
            .expect("Failed to read the stream");

        if bytes_read == 0 {
            println!("Connection closed");
            break;
        }

        let parsed_data =
            Executor::parse(std::str::from_utf8(&buffer[..bytes_read]).expect("Parsing failed"));

        let parsed_cmd = match parsed_data {
            Ok(cmd) => cmd,
            Err(e) => {
                let msg = format!("Error parsing the data: {}", e);
                println!("{}", msg);
                match stream.write_all(format!("{:?}\n", msg).as_bytes()).await {
                    Ok(_) => println!("Data sent successfully"),
                    Err(e) => eprintln!("Writing to TCP stream failed: {}", e),
                };
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

        match stream
            .write_all(format!("{:?}\n", server_reply).as_bytes())
            .await
        {
            Ok(_) => println!("Data sent successfully"),
            Err(e) => eprintln!("Writing to TCP stream failed: {}", e),
        }
    }
}
