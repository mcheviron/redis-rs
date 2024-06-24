use bytes::{Bytes, BytesMut};
use clap::Parser;
use redis_starter_rust::*;
use std::{
    collections::HashMap,
    error::Error,
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::oneshot,
};

enum DbOperation {
    Get(String, oneshot::Sender<Option<(Bytes, Option<Instant>)>>),
    Set(String, Bytes, Option<Instant>, oneshot::Sender<()>),
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let cli = Cli::parse();
    let config = ServerConfig::new(&cli);

    let listener = TcpListener::bind(format!("127.0.0.1:{}", config.port)).await?;
    println!("Listening on 127.0.0.1:{}", config.port);

    if config.is_slave {
        println!("Running as slave");
    } else {
        println!("Running as master");
    }

    let (db_sender, db_receiver) = async_channel::unbounded();

    tokio::spawn(run_database(db_receiver));

    loop {
        let (socket, _) = listener.accept().await?;
        let db_sender = db_sender.clone();
        tokio::spawn(async move {
            if let Err(e) = process(socket, db_sender).await {
                eprintln!("Error processing connection: {}", e);
            }
        });
    }
}

async fn process(
    mut socket: TcpStream,
    db_sender: async_channel::Sender<DbOperation>,
) -> Result<(), Box<dyn Error>> {
    let mut buffer = BytesMut::with_capacity(1024);

    loop {
        let bytes_read = socket.read_buf(&mut buffer).await?;
        if bytes_read == 0 {
            return Ok(());
        }

        let request = Bytes::from(buffer.split_to(bytes_read));
        match RespValue::try_from(request) {
            Ok(RespValue::Array(values)) => {
                if let Some(RespValue::BulkString(Some(command))) = values.first() {
                    let command_str = String::from_utf8_lossy(command);
                    match command_str.to_ascii_lowercase().as_str() {
                        "ping" => {
                            let response =
                                if let Some(RespValue::BulkString(Some(arg))) = values.get(1) {
                                    RespValue::BulkString(Some(arg.clone()))
                                } else {
                                    RespValue::SimpleString("PONG".to_string())
                                };
                            let response_bytes = Bytes::from(response);
                            socket.write_all(&response_bytes).await?;
                        }
                        "echo" => {
                            if let Some(RespValue::BulkString(Some(arg))) = values.get(1) {
                                let response = RespValue::BulkString(Some(arg.clone()));
                                let response_bytes = Bytes::from(response);
                                socket.write_all(&response_bytes).await?;
                            } else {
                                let error =
                                    RespValue::Error("Invalid ECHO command format".to_string());
                                let error_bytes = Bytes::from(error);
                                socket.write_all(&error_bytes).await?;
                            }
                        }
                        "get" => {
                            if let Some(RespValue::BulkString(Some(key))) = values.get(1) {
                                let key_str = String::from_utf8_lossy(key).to_string();
                                let (response_sender, response_receiver) = oneshot::channel();
                                db_sender
                                    .send(DbOperation::Get(key_str, response_sender))
                                    .await?;
                                let response = match response_receiver.await? {
                                    Some((value, expiry)) => {
                                        if let Some(exp) = expiry {
                                            if Instant::now() > exp {
                                                RespValue::BulkString(None)
                                            } else {
                                                RespValue::BulkString(Some(value))
                                            }
                                        } else {
                                            RespValue::BulkString(Some(value))
                                        }
                                    }
                                    None => RespValue::BulkString(None),
                                };
                                let response_bytes = Bytes::from(response);
                                socket.write_all(&response_bytes).await?;
                            } else {
                                let error =
                                    RespValue::Error("Invalid GET command format".to_string());
                                let error_bytes = Bytes::from(error);
                                socket.write_all(&error_bytes).await?;
                            }
                        }

                        "set" => {
                            match (values.get(1), values.get(2), values.get(3), values.get(4)) {
                                (
                                    Some(RespValue::BulkString(Some(key))),
                                    Some(RespValue::BulkString(Some(value))),
                                    Some(RespValue::BulkString(Some(px_bytes))),
                                    Some(RespValue::BulkString(Some(ms))),
                                ) if px_bytes.to_ascii_lowercase() == b"px" => {
                                    let key_str = String::from_utf8_lossy(key).to_string();
                                    let expiry = String::from_utf8_lossy(ms)
                                        .parse::<u64>()
                                        .map(|ms| Instant::now() + Duration::from_millis(ms))
                                        .ok();
                                    let (response_sender, response_receiver) = oneshot::channel();
                                    db_sender
                                        .send(DbOperation::Set(
                                            key_str,
                                            value.clone(),
                                            expiry,
                                            response_sender,
                                        ))
                                        .await?;
                                    response_receiver.await?;
                                    let response = RespValue::SimpleString("OK".to_string());
                                    let response_bytes = Bytes::from(response);
                                    socket.write_all(&response_bytes).await?;
                                }
                                (
                                    Some(RespValue::BulkString(Some(key))),
                                    Some(RespValue::BulkString(Some(value))),
                                    None,
                                    None,
                                ) => {
                                    let key_str = String::from_utf8_lossy(key).to_string();
                                    let (response_sender, response_receiver) =
                                        tokio::sync::oneshot::channel();
                                    db_sender
                                        .send(DbOperation::Set(
                                            key_str,
                                            value.clone(),
                                            None,
                                            response_sender,
                                        ))
                                        .await?;
                                    response_receiver.await?;
                                    let response = RespValue::SimpleString("OK".to_string());
                                    let response_bytes = Bytes::from(response);
                                    socket.write_all(&response_bytes).await?;
                                }
                                _ => {
                                    let error =
                                        RespValue::Error("Invalid SET command format".to_string());
                                    let error_bytes = Bytes::from(error);
                                    socket.write_all(&error_bytes).await?;
                                }
                            }
                        }

                        _ => {
                            let error = RespValue::Error("Unknown command".to_string());
                            let error_bytes = Bytes::from(error);
                            socket.write_all(&error_bytes).await?;
                        }
                    }
                } else {
                    let error = RespValue::Error("Invalid command format".to_string());
                    let error_bytes = Bytes::from(error);
                    socket.write_all(&error_bytes).await?;
                }
            }
            Ok(_) => {
                let error = RespValue::Error("Invalid request format".to_string());
                let error_bytes = Bytes::from(error);
                socket.write_all(&error_bytes).await?;
            }
            Err(e) => {
                let error = RespValue::Error(format!("Parse error: {}", e));
                let error_bytes = Bytes::from(error);
                socket.write_all(&error_bytes).await?;
            }
        }
    }
}
async fn run_database(db_receiver: async_channel::Receiver<DbOperation>) {
    let mut db = HashMap::new();

    while let Ok(operation) = db_receiver.recv().await {
        match operation {
            DbOperation::Get(key, response_sender) => {
                let value = db.get(&key).cloned();
                let _ = response_sender.send(value);
            }
            DbOperation::Set(key, value, expiry, response_sender) => {
                db.insert(key, (value, expiry));
                let _ = response_sender.send(());
            }
        }
    }
}
