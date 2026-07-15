use std::{error::Error, sync::Arc};

use crate::storage::KvStore;

#[derive(Debug, PartialEq)]
pub enum Command {
    Set { key: String, value: String },
    Get { key: String },
    Delete { key: String },
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

/*
A PRETTY IMPORTANT PIECE OF YAPPING

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
*/

// notes:
// use `channels` bro like fr
// so cmd receives a struct which contains key/value and a oneshot sender
// executor should own the store
pub async fn execute(store: &mut KvStore, cmd: Command) {
    match cmd {
        Command::Set { key, value } => store.set(&key, &value).await,
        Command::Delete { key } => store.delete(&key).await,
        Command::Get { key } => {
            store.get(&key).await;
        }
    };
}

#[cfg(test)]
mod tests {
    use crate::parser::{Command, parse};

    #[test]
    fn test_parse_happy_path() {
        assert_eq!(
            parse("SET key value").unwrap(),
            Command::Set {
                key: String::from("key"),
                value: String::from("value")
            }
        );

        assert_eq!(
            parse("DELETE key").unwrap(),
            Command::Delete {
                key: String::from("key"),
            }
        );

        assert_eq!(
            parse("GET key").unwrap(),
            Command::Get {
                key: String::from("key"),
            }
        );

        assert!(parse("unknown").is_err());

        assert!(parse("GET").is_err());

        assert!(parse("").is_err());
    }
}
