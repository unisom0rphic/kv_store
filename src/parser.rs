use anyhow::{Error, Result};
use tokio::sync::mpsc;

use crate::conn::{Command, StoreRequest};
use crate::storage::KvStore;

pub struct Executor {
    storage: KvStore,
    requests_rx: mpsc::Receiver<StoreRequest>,
}

impl Executor {
    fn new(storage: KvStore, requests_rx: mpsc::Receiver<StoreRequest>) -> Self {
        Self {
            storage,
            requests_rx,
        }
    }

    pub fn parse(s: &str) -> Result<Command, String> {
        let mut tokens = s.split(' ');
        if let Some(command) = tokens.next() {
            match command {
                // check for length
                "SET" => {
                    let key = String::from(tokens.next().ok_or("No key specified")?);
                    let value = String::from(tokens.next().ok_or("No value specified")?);

                    Ok(Command::Set { key, value })
                }
                "DELETE" => {
                    let key = String::from(tokens.next().ok_or("No key specified")?);
                    Ok(Command::Delete { key })
                }
                "GET" => {
                    let key = String::from(tokens.next().ok_or("No key specified")?);
                    Ok(Command::Get { key })
                }
                _ => Err(String::from(
                    "Unknown command.\nCurrently implemented: GET, SET, DELETE",
                )),
            }
        } else {
            Err(String::from("Couldn't find the first token"))
        }
    }

    // sr is received from the loop listening to the channel (like `run()` method or smth)
    pub async fn execute(&mut self, sr: StoreRequest) -> Result<()> {
        let result = match sr.cmd {
            Command::Set { ref key, ref value } => self.storage.set(key, value).await,
            Command::Get { ref key } => self.storage.get(key).await,
            Command::Delete { ref key } => self.storage.delete(key).await,
        }
        .unwrap_or(format!("Error executing the command: {:?}", &sr.cmd));

        sr.tx
            .send(result.bytes().collect())
            .map_err(|bytes| anyhow::anyhow!("{}", String::from_utf8_lossy(&bytes)))?;

        Ok(())
    }

    pub async fn run(&mut self) -> Result<()> {
        loop {
            let sr = self.requests_rx.recv().await;
            match sr {
                Some(sr) => return self.execute(sr).await,
                None => anyhow::anyhow!("empty channel"),
            };
        }
    }
}

/*
A PRETTY IMPORTANT PIECE OF YAPPING
15.07.2026

So I was walking home from the gym yesterday and I thought about concurrent execution in
this particular scenario. Basically if I try to build an fully concurrent system
(multiple threads modifying the same store simultaneously) I will definitely fail.

My idea is to have the executor tied to a *single* KvStore and process code
sequentially, so if TCP connections A and B work with KvStore K at the same time and
send a modifying request at the same time, it is attached to the same executor and
executes on a single thread. It's slow but at least it works. Multiple executors can
be distributed between the thread pool as long as they don't have access to the shared
state (i.e distinct stores).

About architecture - I think the `parser` function should be a part of an executor, as shall
the KvStore itself (the isolated existence of it is meaningless at this stage) ->
it's owned by the executor.

To allow returning data back we might use a struct with Command and oneshot sender so like

```
struct StoreRequest {
    tx: oneshot::sender,
    cmd: Command
};
```

and use in the executor:
```
fn execute(&mut self, sr: StoreRequest) {
    match sr.cmd {
        Command::Set {key, value} => {
            let result = self.kvstore.set(&key, &value).await;
            tx.send(res);
        };
    };
}
something like this?

The executor contains the receiver for the mpsc channel (the sender is inside the tcp conn handler),
every request send and parsed returns a oneshot pair.

is this the actor model? need research
```
*/

#[cfg(test)]
mod tests {
    use tokio::sync::{mpsc, oneshot};
    use tokio::time::{Duration, timeout};

    use crate::{conn::StoreRequest, storage::KvStore};

    use super::{Command, Executor};

    #[test]
    fn test_parse_happy_path() {
        assert_eq!(
            Executor::parse("SET key value").unwrap(),
            Command::Set {
                key: String::from("key"),
                value: String::from("value")
            }
        );

        assert_eq!(
            Executor::parse("DELETE key").unwrap(),
            Command::Delete {
                key: String::from("key"),
            }
        );

        assert_eq!(
            Executor::parse("GET key").unwrap(),
            Command::Get {
                key: String::from("key"),
            }
        );

        assert!(Executor::parse("unknown").is_err());

        assert!(Executor::parse("GET").is_err());

        assert!(Executor::parse("").is_err());
    }

    /// Ensure the Executor can read mpsc::channel and execute the command in
    /// a 2 second window
    #[test]
    fn test_executor_happy_path() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let handle = rt.handle();

        let (tx, rx) = mpsc::channel(10);
        let mut exec = Executor::new(KvStore::new(), rx);

        // might be spwan blocking
        // or spawn local
        // or maybe it won't execute parallel requests I don't really get it
        handle.spawn(async move { exec.run().await });

        let (otx, orx) = oneshot::channel();

        let send_handle = handle.spawn(async move {
            let _ = tx
                .send(StoreRequest {
                    cmd: Command::Set {
                        key: "basic_key".to_string(),
                        value: "basic_value".to_string(),
                    },
                    tx: otx,
                })
                .await;
        });

        let duration = Duration::from_secs(2);

        let result = rt
            .block_on(async {
                timeout(duration, async {
                    send_handle.await.unwrap();
                    orx.await.unwrap()
                })
                .await
            })
            .unwrap();

        assert_eq!(result, b"success");
    }
}
