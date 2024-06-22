use bytes::{Bytes, BytesMut};
use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Mutex,
};

mod resp;
use resp::RespValue;

type DB = Arc<Mutex<HashMap<String, Bytes>>>;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let listener = TcpListener::bind("127.0.0.1:6379").await?;
    println!("Listening on 127.0.0.1:6379");

    let db = Arc::new(Mutex::new(HashMap::new()));

    loop {
        let (socket, _) = listener.accept().await?;
        let db_clone = Arc::clone(&db);
        tokio::spawn(async move {
            if let Err(e) = process(socket, db_clone).await {
                eprintln!("Error processing connection: {}", e);
            }
        });
    }
}

async fn process(mut socket: TcpStream, db: DB) -> Result<(), Box<dyn Error>> {
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
                                let key_str = String::from_utf8_lossy(key);
                                let db_guard = db.lock().await;
                                let response =
                                    if let Some(value) = db_guard.get(&key_str.to_string()) {
                                        RespValue::BulkString(Some(value.clone()))
                                    } else {
                                        RespValue::BulkString(None)
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
                            if let (
                                Some(RespValue::BulkString(Some(key))),
                                Some(RespValue::BulkString(Some(value))),
                            ) = (values.get(1), values.get(2))
                            {
                                let key_str = String::from_utf8_lossy(key).to_string();
                                let mut db_guard = db.lock().await;
                                db_guard.insert(key_str, value.clone());
                                let response = RespValue::SimpleString("OK".to_string());
                                let response_bytes = Bytes::from(response);
                                socket.write_all(&response_bytes).await?;
                            } else {
                                let error =
                                    RespValue::Error("Invalid SET command format".to_string());
                                let error_bytes = Bytes::from(error);
                                socket.write_all(&error_bytes).await?;
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
