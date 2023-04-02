use std::hash::{Hash, Hasher};
use std::{convert, fmt, str, sync::Arc};

use either::Either;
use ethnum::I256;

use crate::error::{Error, FromSqlError};
use crate::{error::Result, types::SqlType, value::Value};

#[derive(Clone, Debug)]
pub enum ValueRef<'a> {
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Int256(I256),
    String(&'a [u8]),
    Float32(f32),
    Float64(f64),
    Nullable(Either<&'static SqlType, Box<ValueRef<'a>>>),
    Array(&'static SqlType, Arc<Vec<ValueRef<'a>>>),
}

impl<'a> Hash for ValueRef<'a> {
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

impl<'a> Eq for ValueRef<'a> {}

impl<'a> PartialEq for ValueRef<'a> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (ValueRef::UInt8(a), ValueRef::UInt8(b)) => *a == *b,
            (ValueRef::UInt16(a), ValueRef::UInt16(b)) => *a == *b,
            (ValueRef::UInt32(a), ValueRef::UInt32(b)) => *a == *b,
            (ValueRef::UInt64(a), ValueRef::UInt64(b)) => *a == *b,
            (ValueRef::Int8(a), ValueRef::Int8(b)) => *a == *b,
            (ValueRef::Int16(a), ValueRef::Int16(b)) => *a == *b,
            (ValueRef::Int32(a), ValueRef::Int32(b)) => *a == *b,
            (ValueRef::Int64(a), ValueRef::Int64(b)) => *a == *b,
            (ValueRef::String(a), ValueRef::String(b)) => *a == *b,
            (ValueRef::Float32(a), ValueRef::Float32(b)) => *a == *b,
            (ValueRef::Float64(a), ValueRef::Float64(b)) => *a == *b,
            (ValueRef::Nullable(a), ValueRef::Nullable(b)) => *a == *b,
            (ValueRef::Array(ta, a), ValueRef::Array(tb, b)) => *ta == *tb && *a == *b,
            _ => false,
        }
    }
}

impl<'a> fmt::Display for ValueRef<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ValueRef::UInt8(v) => fmt::Display::fmt(v, f),
            ValueRef::UInt16(v) => fmt::Display::fmt(v, f),
            ValueRef::UInt32(v) => fmt::Display::fmt(v, f),
            ValueRef::UInt64(v) => fmt::Display::fmt(v, f),
            ValueRef::Int8(v) => fmt::Display::fmt(v, f),
            ValueRef::Int16(v) => fmt::Display::fmt(v, f),
            ValueRef::Int32(v) => fmt::Display::fmt(v, f),
            ValueRef::Int64(v) => fmt::Display::fmt(v, f),
            ValueRef::Int256(v) => fmt::Display::fmt(v, f),
            ValueRef::String(v) => match str::from_utf8(v) {
                Ok(s) => fmt::Display::fmt(s, f),
                Err(_) => write!(f, "{:?}", *v),
            },
            ValueRef::Float32(v) => fmt::Display::fmt(v, f),
            ValueRef::Float64(v) => fmt::Display::fmt(v, f),
            ValueRef::Nullable(v) => match v {
                Either::Left(_) => write!(f, "NULL"),
                Either::Right(inner) => write!(f, "{}", inner),
            },
            ValueRef::Array(_, vs) => {
                let cells: Vec<String> = vs.iter().map(|v| format!("{}", v)).collect();
                write!(f, "[{}]", cells.join(", "))
            }
        }
    }
}

impl<'a> convert::From<ValueRef<'a>> for SqlType {
    fn from(source: ValueRef<'a>) -> Self {
        match source {
            ValueRef::UInt8(_) => SqlType::UInt8,
            ValueRef::UInt16(_) => SqlType::UInt16,
            ValueRef::UInt32(_) => SqlType::UInt32,
            ValueRef::UInt64(_) => SqlType::UInt64,
            ValueRef::Int8(_) => SqlType::Int8,
            ValueRef::Int16(_) => SqlType::Int16,
            ValueRef::Int32(_) => SqlType::Int32,
            ValueRef::Int64(_) => SqlType::Int64,
            ValueRef::Int256(_) => SqlType::Int256,
            ValueRef::String(_) => SqlType::String,
            ValueRef::Float32(_) => SqlType::Float32,
            ValueRef::Float64(_) => SqlType::Float64,
            ValueRef::Nullable(u) => match u {
                Either::Left(sql_type) => SqlType::Nullable(sql_type),
                Either::Right(value_ref) => SqlType::Nullable(SqlType::from(*value_ref).into()),
            },
            ValueRef::Array(t, _) => SqlType::Array(t),
        }
    }
}

impl<'a> ValueRef<'a> {
    pub fn as_str(&self) -> Result<&'a str> {
        if let ValueRef::String(t) = self {
            return Ok(str::from_utf8(t)?);
        }
        let from = SqlType::from(self.clone()).to_string();
        Err(Error::FromSql(FromSqlError::InvalidType {
            src: from,
            dst: "&str".into(),
        }))
    }

    pub fn as_string(&self) -> Result<String> {
        let tmp = self.as_str()?;
        Ok(tmp.to_string())
    }

    pub fn as_bytes(&self) -> Result<&'a [u8]> {
        if let ValueRef::String(t) = self {
            return Ok(t);
        }
        let from = SqlType::from(self.clone()).to_string();
        Err(Error::FromSql(FromSqlError::InvalidType {
            src: from,
            dst: "&[u8]".into(),
        }))
    }
}

impl<'a> From<ValueRef<'a>> for Value {
    fn from(borrowed: ValueRef<'a>) -> Self {
        match borrowed {
            ValueRef::UInt8(v) => Value::UInt8(v),
            ValueRef::UInt16(v) => Value::UInt16(v),
            ValueRef::UInt32(v) => Value::UInt32(v),
            ValueRef::UInt64(v) => Value::UInt64(v),
            ValueRef::Int8(v) => Value::Int8(v),
            ValueRef::Int16(v) => Value::Int16(v),
            ValueRef::Int32(v) => Value::Int32(v),
            ValueRef::Int64(v) => Value::Int64(v),
            ValueRef::Int256(v) => Value::Int256(v),
            ValueRef::String(v) => Value::String(Arc::new(v.into())),
            ValueRef::Float32(v) => Value::Float32(v),
            ValueRef::Float64(v) => Value::Float64(v),
            ValueRef::Nullable(u) => match u {
                Either::Left(sql_type) => Value::Nullable(Either::Left((sql_type.clone()).into())),
                Either::Right(v) => {
                    let value: Value = (*v).into();
                    Value::Nullable(Either::Right(Box::new(value)))
                }
            },
            ValueRef::Array(t, vs) => {
                let mut value_list: Vec<Value> = Vec::with_capacity(vs.len());
                for v in vs.iter() {
                    let value: Value = v.clone().into();
                    value_list.push(value);
                }
                Value::Array(t, Arc::new(value_list))
            }
        }
    }
}

impl<'a> From<&'a str> for ValueRef<'a> {
    fn from(s: &str) -> ValueRef {
        ValueRef::String(s.as_bytes())
    }
}

impl<'a> From<&'a [u8]> for ValueRef<'a> {
    fn from(bs: &[u8]) -> ValueRef {
        ValueRef::String(bs)
    }
}

macro_rules! from_number {
    ( $($t:ty: $k:ident),* ) => {
        $(
            impl<'a> From<$t> for ValueRef<'a> {
                fn from(v: $t) -> ValueRef<'static> {
                    ValueRef::$k(v)
                }
            }
        )*
    };
}

from_number! {
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

impl<'a> From<&'a Value> for ValueRef<'a> {
    fn from(value: &'a Value) -> ValueRef<'a> {
        match value {
            Value::UInt8(v) => ValueRef::UInt8(*v),
            Value::UInt16(v) => ValueRef::UInt16(*v),
            Value::UInt32(v) => ValueRef::UInt32(*v),
            Value::UInt64(v) => ValueRef::UInt64(*v),
            Value::Int8(v) => ValueRef::Int8(*v),
            Value::Int16(v) => ValueRef::Int16(*v),
            Value::Int32(v) => ValueRef::Int32(*v),
            Value::Int64(v) => ValueRef::Int64(*v),
            Value::Int256(v) => ValueRef::Int256(*v),
            Value::String(v) => ValueRef::String(v),
            Value::Float32(v) => ValueRef::Float32(*v),
            Value::Float64(v) => ValueRef::Float64(*v),
            Value::Nullable(u) => match u {
                Either::Left(sql_type) => ValueRef::Nullable(Either::Left(sql_type.to_owned())),
                Either::Right(v) => {
                    let value_ref = v.as_ref().into();
                    ValueRef::Nullable(Either::Right(Box::new(value_ref)))
                }
            },
            Value::Array(t, vs) => {
                let mut ref_vec = Vec::with_capacity(vs.len());
                for v in vs.iter() {
                    let value_ref: ValueRef<'a> = From::from(v);
                    ref_vec.push(value_ref)
                }
                ValueRef::Array(t, Arc::new(ref_vec))
            }
        }
    }
}

macro_rules! value_from {
    ( $( $t:ty: $k:ident ),* ) => {
        $(
            impl<'a> From<ValueRef<'a>> for $t {
                fn from(value: ValueRef<'a>) -> Self {
                    if let ValueRef::$k(v) = value {
                        return v
                    }
                    let from = format!("{}", SqlType::from(value.clone()));
                    panic!("Can't convert ValueRef::{} into {}.",
                            from, stringify!($t))
                }
            }
        )*
    };
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

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_display() {
        assert_eq!(
            "[0, 159, 146, 150]".to_string(),
            format!("{}", ValueRef::String(&[0, 159, 146, 150]))
        );

        assert_eq!("text".to_string(), format!("{}", ValueRef::String(b"text")));

        assert_eq!("42".to_string(), format!("{}", ValueRef::UInt8(42)));
        assert_eq!("42".to_string(), format!("{}", ValueRef::UInt16(42)));
        assert_eq!("42".to_string(), format!("{}", ValueRef::UInt32(42)));
        assert_eq!("42".to_string(), format!("{}", ValueRef::UInt64(42)));

        assert_eq!("42".to_string(), format!("{}", ValueRef::Int8(42)));
        assert_eq!("42".to_string(), format!("{}", ValueRef::Int16(42)));
        assert_eq!("42".to_string(), format!("{}", ValueRef::Int32(42)));
        assert_eq!("42".to_string(), format!("{}", ValueRef::Int64(42)));

        assert_eq!("42".to_string(), format!("{}", ValueRef::Float32(42.0)));
        assert_eq!("42".to_string(), format!("{}", ValueRef::Float64(42.0)));

        assert_eq!(
            "NULL".to_string(),
            format!(
                "{}",
                ValueRef::Nullable(Either::Left(SqlType::UInt8.into()))
            )
        );

        assert_eq!(
            "42".to_string(),
            format!(
                "{}",
                ValueRef::Nullable(Either::Right(Box::new(ValueRef::UInt8(42))))
            )
        );

        assert_eq!(
            "[1, 2, 3]".to_string(),
            format!(
                "{}",
                ValueRef::Array(
                    SqlType::Int32.into(),
                    Arc::new(vec![
                        ValueRef::Int32(1),
                        ValueRef::Int32(2),
                        ValueRef::Int32(3)
                    ])
                )
            )
        );
    }

    #[test]
    fn test_size_of() {
        use std::mem;
        assert_eq!(48, mem::size_of::<[ValueRef<'_>; 1]>());
    }

    #[test]
    fn test_value_from_ref() {
        assert_eq!(Value::from(ValueRef::UInt8(42)), Value::UInt8(42));
        assert_eq!(Value::from(ValueRef::UInt16(42)), Value::UInt16(42));
        assert_eq!(Value::from(ValueRef::UInt32(42)), Value::UInt32(42));
        assert_eq!(Value::from(ValueRef::UInt64(42)), Value::UInt64(42));

        assert_eq!(Value::from(ValueRef::Int8(42)), Value::Int8(42));
        assert_eq!(Value::from(ValueRef::Int16(42)), Value::Int16(42));
        assert_eq!(Value::from(ValueRef::Int32(42)), Value::Int32(42));
        assert_eq!(Value::from(ValueRef::Int64(42)), Value::Int64(42));

        assert_eq!(Value::from(ValueRef::Float32(42.0)), Value::Float32(42.0));
        assert_eq!(Value::from(ValueRef::Float64(42.0)), Value::Float64(42.0));
        assert_eq!(
            Value::from(ValueRef::Array(
                SqlType::Int32.into(),
                Arc::new(vec![
                    ValueRef::Int32(1),
                    ValueRef::Int32(2),
                    ValueRef::Int32(3)
                ])
            )),
            Value::Array(
                SqlType::Int32.into(),
                Arc::new(vec![Value::Int32(1), Value::Int32(2), Value::Int32(3)])
            )
        )
    }

    #[test]
    fn test_get_sql_type() {
        assert_eq!(SqlType::from(ValueRef::UInt8(42)), SqlType::UInt8);
        assert_eq!(SqlType::from(ValueRef::UInt16(42)), SqlType::UInt16);
        assert_eq!(SqlType::from(ValueRef::UInt32(42)), SqlType::UInt32);
        assert_eq!(SqlType::from(ValueRef::UInt64(42)), SqlType::UInt64);

        assert_eq!(SqlType::from(ValueRef::Int8(42)), SqlType::Int8);
        assert_eq!(SqlType::from(ValueRef::Int16(42)), SqlType::Int16);
        assert_eq!(SqlType::from(ValueRef::Int32(42)), SqlType::Int32);
        assert_eq!(SqlType::from(ValueRef::Int64(42)), SqlType::Int64);

        assert_eq!(SqlType::from(ValueRef::Float32(42.0)), SqlType::Float32);
        assert_eq!(SqlType::from(ValueRef::Float64(42.0)), SqlType::Float64);

        assert_eq!(SqlType::from(ValueRef::String(&[])), SqlType::String);

        assert_eq!(
            SqlType::from(ValueRef::Array(
                SqlType::Int32.into(),
                Arc::new(vec![
                    ValueRef::Int32(1),
                    ValueRef::Int32(2),
                    ValueRef::Int32(3)
                ])
            )),
            SqlType::Array(SqlType::Int32.into())
        );

        assert_eq!(
            SqlType::from(ValueRef::Nullable(Either::Left(SqlType::UInt8.into()))),
            SqlType::Nullable(SqlType::UInt8.into())
        );

        assert_eq!(
            SqlType::from(ValueRef::Nullable(Either::Right(Box::new(ValueRef::Int8(
                42
            ))))),
            SqlType::Nullable(SqlType::Int8.into())
        );
    }
}
