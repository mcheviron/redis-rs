use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::io::{self};
use thiserror::Error;

#[derive(Debug, PartialEq, Clone)]
pub enum RespValue {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(Option<Bytes>),
    Array(Vec<RespValue>),
}

#[derive(Error, Debug)]
pub enum RespError {
    #[error("IO error: {0}")]
    Io(#[from] io::Error),
    #[error("Parse error: {0}")]
    Parse(String),
}

impl From<RespValue> for Bytes {
    fn from(value: RespValue) -> Self {
        let mut buf = BytesMut::new();
        match value {
            RespValue::SimpleString(s) => {
                buf.put_u8(b'+');
                buf.put(s.as_bytes());
                buf.put(&b"\r\n"[..]);
            }
            RespValue::Error(s) => {
                buf.put_u8(b'-');
                buf.put(s.as_bytes());
                buf.put(&b"\r\n"[..]);
            }
            RespValue::Integer(i) => {
                buf.put_u8(b':');
                buf.put(i.to_string().as_bytes());
                buf.put(&b"\r\n"[..]);
            }
            RespValue::BulkString(Some(data)) => {
                buf.put_u8(b'$');
                buf.put(data.len().to_string().as_bytes());
                buf.put(&b"\r\n"[..]);
                buf.put(data);
                buf.put(&b"\r\n"[..]);
            }
            RespValue::BulkString(None) => {
                buf.put(&b"$-1\r\n"[..]);
            }
            RespValue::Array(arr) => {
                buf.put_u8(b'*');
                buf.put(arr.len().to_string().as_bytes());
                buf.put(&b"\r\n"[..]);
                for item in arr {
                    buf.put(Bytes::from(item));
                }
            }
        }
        buf.freeze()
    }
}

impl TryFrom<Bytes> for RespValue {
    type Error = RespError;

    fn try_from(mut bytes: Bytes) -> Result<RespValue, <RespValue as TryFrom<Bytes>>::Error> {
        if bytes.is_empty() {
            return Err(RespError::Parse("Empty input".to_string()));
        }

        match bytes[0] {
            b'+' => {
                bytes.advance(1);
                let s = String::from_utf8(bytes.split_to(bytes.len() - 2).to_vec())
                    .map_err(|e| RespError::Parse(e.to_string()))?;
                Ok(RespValue::SimpleString(s))
            }
            b'-' => {
                bytes.advance(1);
                let s = String::from_utf8(bytes.split_to(bytes.len() - 2).to_vec())
                    .map_err(|e| RespError::Parse(e.to_string()))?;
                Ok(RespValue::Error(s))
            }
            b':' => {
                bytes.advance(1);
                let num = String::from_utf8(bytes.split_to(bytes.len() - 2).to_vec())
                    .map_err(|e| RespError::Parse(e.to_string()))?
                    .parse::<i64>()
                    .map_err(|e| RespError::Parse(e.to_string()))?;
                Ok(RespValue::Integer(num))
            }
            b'$' => {
                bytes.advance(1);
                let len_end = bytes
                    .iter()
                    .position(|&b| b == b'\r')
                    .ok_or_else(|| RespError::Parse("Invalid bulk string format".to_string()))?;
                let len = String::from_utf8(bytes.split_to(len_end).to_vec())
                    .map_err(|e| RespError::Parse(e.to_string()))?
                    .parse::<i64>()
                    .map_err(|e| RespError::Parse(e.to_string()))?;
                bytes.advance(2); // Skip \r\n
                if len == -1 {
                    Ok(RespValue::BulkString(None))
                } else {
                    let data = bytes.split_to(len as usize);
                    bytes.advance(2); // Skip \r\n
                    Ok(RespValue::BulkString(Some(data)))
                }
            }
            b'*' => {
                bytes.advance(1);

                let len_end = bytes
                    .iter()
                    .position(|&b| b == b'\r')
                    .ok_or_else(|| RespError::Parse("Invalid array format".to_string()))?;

                let len = String::from_utf8(bytes.split_to(len_end).to_vec())
                    .map_err(|e| RespError::Parse(e.to_string()))?
                    .parse::<usize>()
                    .map_err(|e| RespError::Parse(e.to_string()))?;

                bytes.advance(2);

                let mut array = Vec::with_capacity(len);

                for _ in 0..len {
                    array.push(RespValue::try_from(bytes.clone())?);

                    bytes.advance(
                        Bytes::from(
                            array
                                .last()
                                .ok_or_else(|| RespError::Parse("Empty array".to_string()))?
                                .clone(),
                        )
                        .len(),
                    );
                }

                Ok(RespValue::Array(array))
            }
            _ => Err(RespError::Parse("Invalid RESP data type".to_string())),
        }
    }
}
