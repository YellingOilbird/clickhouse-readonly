use std::hash::{Hash, Hasher};
use std::{convert, fmt, mem, str, sync::Arc};

use either::Either;
use ethnum::I256;

use crate::types::{HasSqlType, SqlType};

/// Client side representation of a value of Clickhouse column.
#[derive(Clone, Debug)]
pub enum Value {
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Int256(I256),
    String(Arc<Vec<u8>>),
    Float32(f32),
    Float64(f64),
    Nullable(Either<&'static SqlType, Box<Value>>),
    Array(&'static SqlType, Arc<Vec<Value>>),
}

impl Hash for Value {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            Self::String(s) => s.hash(state),
            Self::Int8(i) => i.hash(state),
            Self::Int16(i) => i.hash(state),
            Self::Int32(i) => i.hash(state),
            Self::Int64(i) => i.hash(state),
            Self::Int256(i) => i.hash(state),
            Self::UInt8(i) => i.hash(state),
            Self::UInt16(i) => i.hash(state),
            Self::UInt32(i) => i.hash(state),
            Self::UInt64(i) => i.hash(state),
            _ => unimplemented!(),
        }
    }
}

impl Eq for Value {}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Value::UInt8(a), Value::UInt8(b)) => *a == *b,
            (Value::UInt16(a), Value::UInt16(b)) => *a == *b,
            (Value::UInt32(a), Value::UInt32(b)) => *a == *b,
            (Value::UInt64(a), Value::UInt64(b)) => *a == *b,
            (Value::Int8(a), Value::Int8(b)) => *a == *b,
            (Value::Int16(a), Value::Int16(b)) => *a == *b,
            (Value::Int32(a), Value::Int32(b)) => *a == *b,
            (Value::Int64(a), Value::Int64(b)) => *a == *b,
            (Value::String(a), Value::String(b)) => *a == *b,
            (Value::Float32(a), Value::Float32(b)) => *a == *b,
            (Value::Float64(a), Value::Float64(b)) => *a == *b,
            (Value::Nullable(a), Value::Nullable(b)) => *a == *b,
            (Value::Array(ta, a), Value::Array(tb, b)) => *ta == *tb && *a == *b,
            _ => false,
        }
    }
}

impl Value {
    pub(crate) fn default(sql_type: SqlType) -> Value {
        match sql_type {
            SqlType::UInt8 => Value::UInt8(0),
            SqlType::UInt16 => Value::UInt16(0),
            SqlType::UInt32 => Value::UInt32(0),
            SqlType::UInt64 => Value::UInt64(0),
            SqlType::Int8 => Value::Int8(0),
            SqlType::Int16 => Value::Int16(0),
            SqlType::Int32 => Value::Int32(0),
            SqlType::Int64 => Value::Int64(0),
            SqlType::Int256 => Value::Int256(Default::default()),
            SqlType::String => Value::String(Arc::new(Vec::default())),
            SqlType::FixedString(str_len) => Value::String(Arc::new(vec![0_u8; str_len])),
            SqlType::Float32 => Value::Float32(0.0),
            SqlType::Float64 => Value::Float64(0.0),
            SqlType::Nullable(inner) => Value::Nullable(Either::Left(inner)),
            SqlType::Array(inner) => Value::Array(inner, Arc::new(Vec::default())),
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Value::UInt8(ref v) => fmt::Display::fmt(v, f),
            Value::UInt16(ref v) => fmt::Display::fmt(v, f),
            Value::UInt32(ref v) => fmt::Display::fmt(v, f),
            Value::UInt64(ref v) => fmt::Display::fmt(v, f),
            Value::Int8(ref v) => fmt::Display::fmt(v, f),
            Value::Int16(ref v) => fmt::Display::fmt(v, f),
            Value::Int32(ref v) => fmt::Display::fmt(v, f),
            Value::Int64(ref v) => fmt::Display::fmt(v, f),
            Value::Int256(ref v) => fmt::Display::fmt(v, f),
            Value::String(ref v) => match str::from_utf8(v) {
                Ok(s) => fmt::Display::fmt(s, f),
                Err(_) => write!(f, "{:?}", v),
            },
            Value::Float32(ref v) => fmt::Display::fmt(v, f),
            Value::Float64(ref v) => fmt::Display::fmt(v, f),
            Value::Nullable(v) => match v {
                Either::Left(_) => write!(f, "NULL"),
                Either::Right(data) => data.fmt(f),
            },
            Value::Array(_, vs) => {
                let cells: Vec<String> = vs.iter().map(|v| format!("{}", v)).collect();
                write!(f, "[{}]", cells.join(", "))
            }
        }
    }
}

impl From<Value> for SqlType {
    fn from(source: Value) -> Self {
        match source {
            Value::UInt8(_) => SqlType::UInt8,
            Value::UInt16(_) => SqlType::UInt16,
            Value::UInt32(_) => SqlType::UInt32,
            Value::UInt64(_) => SqlType::UInt64,
            Value::Int8(_) => SqlType::Int8,
            Value::Int16(_) => SqlType::Int16,
            Value::Int32(_) => SqlType::Int32,
            Value::Int64(_) => SqlType::Int64,
            Value::Int256(_) => SqlType::Int256,
            Value::String(_) => SqlType::String,
            Value::Float32(_) => SqlType::Float32,
            Value::Float64(_) => SqlType::Float64,
            Value::Nullable(d) => match d {
                Either::Left(t) => SqlType::Nullable(t),
                Either::Right(inner) => {
                    let sql_type = SqlType::from(inner.as_ref().to_owned());
                    SqlType::Nullable(sql_type.into())
                }
            },
            Value::Array(t, _) => SqlType::Array(t),
        }
    }
}

impl<T> From<Option<T>> for Value
where
    Value: From<T>,
    T: HasSqlType,
{
    fn from(value: Option<T>) -> Value {
        match value {
            None => {
                let default_type: SqlType = T::get_sql_type();
                Value::Nullable(Either::Left(default_type.into()))
            }
            Some(inner) => Value::Nullable(Either::Right(Box::new(inner.into()))),
        }
    }
}

macro_rules! value_from {
    ( $( $t:ty : $k:ident ),* ) => {
        $(
            impl convert::From<$t> for Value {
                fn from(v: $t) -> Value {
                    Value::$k(v.into())
                }
            }
        )*
    };
}

macro_rules! value_array_from {
    ( $( $t:ty : $k:ident ),* ) => {
        $(
            impl convert::From<Vec<$t>> for Value {
                fn from(v: Vec<$t>) -> Self {
                    Value::Array(
                        SqlType::$k.into(),
                        Arc::new(v.into_iter().map(|s| s.into()).collect())
                    )
                }
            }
        )*
    };
}

impl From<String> for Value {
    fn from(v: String) -> Value {
        Value::String(Arc::new(v.into_bytes()))
    }
}

impl From<Vec<u8>> for Value {
    fn from(v: Vec<u8>) -> Value {
        Value::String(Arc::new(v))
    }
}

impl From<&[u8]> for Value {
    fn from(v: &[u8]) -> Value {
        Value::String(Arc::new(v.to_vec()))
    }
}

impl From<Vec<String>> for Value {
    fn from(v: Vec<String>) -> Self {
        Value::Array(
            SqlType::String.into(),
            Arc::new(v.into_iter().map(|s| s.into()).collect()),
        )
    }
}

impl From<bool> for Value {
    fn from(v: bool) -> Value {
        Value::UInt8(u8::from(v))
    }
}

value_from! {
    u8: UInt8,
    u16: UInt16,
    u32: UInt32,
    u64: UInt64,

    i8: Int8,
    i16: Int16,
    i32: Int32,
    i64: Int64,
    I256: Int256,

    f32: Float32,
    f64: Float64
}

value_array_from! {
    u16: UInt16,
    u32: UInt32,
    u64: UInt64,

    i8: Int8,
    i16: Int16,
    i32: Int32,
    i64: Int64,
    I256: Int256,

    f32: Float32,
    f64: Float64
}

impl<'a> From<&'a str> for Value {
    fn from(v: &'a str) -> Self {
        let bytes: Vec<u8> = v.as_bytes().into();
        Value::String(Arc::new(bytes))
    }
}

impl From<Value> for String {
    fn from(mut v: Value) -> Self {
        if let Value::String(ref mut x) = &mut v {
            let mut tmp = Arc::new(Vec::new());
            mem::swap(x, &mut tmp);
            if let Ok(result) = str::from_utf8(tmp.as_ref()) {
                return result.into();
            }
        }
        let from = SqlType::from(v);
        panic!("Can't convert Value::{} into String.", from);
    }
}

impl From<Value> for Vec<u8> {
    fn from(v: Value) -> Self {
        match v {
            Value::String(bs) => bs.to_vec(),
            _ => {
                let from = SqlType::from(v);
                panic!("Can't convert Value::{} into Vec<u8>.", from)
            }
        }
    }
}

macro_rules! from_value {
    ( $( $t:ty : $k:ident ),* ) => {
        $(
            impl convert::From<Value> for $t {
                fn from(v: Value) -> $t {
                    if let Value::$k(x) = v {
                        return x;
                    }
                    let from = SqlType::from(v);
                    panic!("Can't convert Value::{} into {}", from, stringify!($t))
                }
            }
        )*
    };
}

from_value! {
    u8: UInt8,
    u16: UInt16,
    u32: UInt32,
    u64: UInt64,
    i8: Int8,
    i16: Int16,
    i32: Int32,
    i64: Int64,
    I256: Int256,
    f32: Float32,
    f64: Float64
}
