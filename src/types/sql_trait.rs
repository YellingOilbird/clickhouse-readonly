use std::str::FromStr;

use crate::error::{Error, FromSqlError, Result};
use crate::types::sql_type::SqlType;
use crate::value::value_ref::ValueRef;

use either::Either;
use ethnum::I256;

pub type FromSqlResult<T> = Result<T>;

pub trait HasSqlType {
    fn get_sql_type() -> SqlType;
}

macro_rules! has_sql_type {
    ( $( $t:ty : $k:expr ),* ) => {
        $(
            impl HasSqlType for $t {
                fn get_sql_type() -> SqlType {
                    $k
                }
            }
        )*
    };
}

has_sql_type! {
    u8: SqlType::UInt8,
    u16: SqlType::UInt16,
    u32: SqlType::UInt32,
    u64: SqlType::UInt64,
    i8: SqlType::Int8,
    i16: SqlType::Int16,
    i32: SqlType::Int32,
    i64: SqlType::Int64,
    I256: SqlType::Int256,
    &str: SqlType::String,
    String: SqlType::String,
    f32: SqlType::Float32,
    f64: SqlType::Float64
}

pub trait FromSql<'a>: Sized {
    fn from_sql(value: ValueRef<'a>) -> FromSqlResult<Self>;
}

macro_rules! from_sql_impl {
    ( $( $t:ident: $k:ident ),* ) => {
        $(
            impl<'a> FromSql<'a> for $t {
                fn from_sql(value: ValueRef<'a>) -> FromSqlResult<Self> {
                    match value {
                        ValueRef::$k(v) => Ok(v),
                        _ => {
                            let from = SqlType::from(value.clone()).to_string();
                            Err(Error::FromSql(FromSqlError::InvalidType {
                                src: from,
                                dst: stringify!($t).into(),
                            }))
                        }
                    }
                }
            }
        )*
    };
}

impl<'a> FromSql<'a> for &'a str {
    fn from_sql(value: ValueRef<'a>) -> FromSqlResult<&'a str> {
        value.as_str()
    }
}

impl<'a> FromSql<'a> for &'a [u8] {
    fn from_sql(value: ValueRef<'a>) -> FromSqlResult<&'a [u8]> {
        value.as_bytes()
    }
}

impl<'a> FromSql<'a> for String {
    fn from_sql(value: ValueRef<'a>) -> FromSqlResult<Self> {
        value.as_str().map(str::to_string)
    }
}

impl<'a> FromSql<'a> for ethereum_types::Address {
    fn from_sql(value: ValueRef<'a>) -> FromSqlResult<Self> {
        match value {
            ValueRef::String(row) => match std::str::from_utf8(row) {
                Ok(s) => Ok(ethereum_types::Address::from_str(s).unwrap_or_else(|e| {
                    panic!(
                        "Unable to get `ethabi::Address` from FixedString({}) {e}",
                        row.len()
                    )
                })),
                Err(e) => panic!("Invalid UTF-8 sequence: {}", e),
            },
            _ => {
                let from = SqlType::from(value.clone()).to_string();
                Err(Error::FromSql(FromSqlError::InvalidType {
                    src: from,
                    dst: "Address".into(),
                }))
            }
        }
    }
}

impl<'a> FromSql<'a> for ethereum_types::U256 {
    fn from_sql(value: ValueRef<'a>) -> FromSqlResult<Self> {
        match value {
            ValueRef::Int256(row) => Ok(ethereum_types::U256::from(&row.to_be_bytes())),
            _ => {
                let from = SqlType::from(value.clone()).to_string();
                Err(Error::FromSql(FromSqlError::InvalidType {
                    src: from,
                    dst: "U256".into(),
                }))
            }
        }
    }
}

macro_rules! from_sql_vec_impl {
    ( $( $t:ty: $k:pat => $f:expr ),* ) => {
        $(
            impl<'a> FromSql<'a> for Vec<$t> {
                fn from_sql(value: ValueRef<'a>) -> FromSqlResult<Self> {
                    match value {
                        ValueRef::Array($k, vs) => {
                            let f: fn(ValueRef<'a>) -> FromSqlResult<$t> = $f;
                            let mut result = Vec::with_capacity(vs.len());
                            for v in vs.iter() {
                                let value: $t = f(v.clone())?;
                                result.push(value);
                            }
                            Ok(result)
                        }
                        _ => {
                            let from = SqlType::from(value.clone()).to_string();
                            Err(Error::FromSql(FromSqlError::InvalidType {
                                src: from,
                                dst: format!("Vec<{}>", stringify!($t)).into(),
                            }))
                        }
                    }
                }
            }
        )*
    };
}

from_sql_vec_impl! {
    &'a str: SqlType::String => |v| v.as_str(),
    String: SqlType::String => |v| v.as_string(),
    &'a [u8]: SqlType::String => |v| v.as_bytes(),
    Vec<u8>: SqlType::String => |v| v.as_bytes().map(<[u8]>::to_vec)
}

impl<'a> FromSql<'a> for Vec<u8> {
    fn from_sql(value: ValueRef<'a>) -> FromSqlResult<Self> {
        match value {
            ValueRef::Array(SqlType::UInt8, vs) => {
                let mut result = Vec::with_capacity(vs.len());
                for v in vs.iter() {
                    result.push(v.clone().into());
                }
                Ok(result)
            }
            _ => value.as_bytes().map(|bs| bs.to_vec()),
        }
    }
}

macro_rules! from_sql_vec_impl {
    ( $( $t:ident: $k:ident ),* ) => {
        $(
            impl<'a> FromSql<'a> for Vec<$t> {
                fn from_sql(value: ValueRef<'a>) -> FromSqlResult<Self> {
                    match value {
                        ValueRef::Array(SqlType::$k, vs) => {
                            let mut result = Vec::with_capacity(vs.len());
                            for v in vs.iter() {
                                let val: $t = v.clone().into();
                                result.push(val);
                            }
                            Ok(result)
                        }
                        _ => {
                            let from = SqlType::from(value.clone()).to_string();
                            Err(Error::FromSql(FromSqlError::InvalidType {
                                src: from,
                                dst: stringify!($t).into(),
                            }))
                        }
                    }
                }
            }
        )*
    };
}

from_sql_vec_impl! {
    i8: Int8,
    i16: Int16,
    i32: Int32,
    i64: Int64,

    u16: UInt16,
    u32: UInt32,
    u64: UInt64,

    f32: Float32,
    f64: Float64
}

impl<'a, T> FromSql<'a> for Option<T>
where
    T: FromSql<'a>,
{
    fn from_sql(value: ValueRef<'a>) -> FromSqlResult<Self> {
        match value {
            ValueRef::Nullable(e) => match e {
                Either::Left(_) => Ok(None),
                Either::Right(u) => {
                    let value_ref = u.as_ref().clone();
                    Ok(Some(T::from_sql(value_ref)?))
                }
            },
            _ => {
                let from = SqlType::from(value.clone()).to_string();
                Err(Error::FromSql(FromSqlError::InvalidType {
                    src: from,
                    dst: stringify!($t).into(),
                }))
            }
        }
    }
}

from_sql_impl! {
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
    use crate::types::sql_trait::FromSql;
    use crate::value::value_ref::ValueRef;

    #[test]
    fn test_u8() {
        let v = ValueRef::from(42_u8);
        let actual = u8::from_sql(v).unwrap();
        assert_eq!(actual, 42_u8);
    }

    #[test]
    fn test_bad_convert() {
        let v = ValueRef::from(42_u16);
        match u32::from_sql(v) {
            Ok(_) => panic!("should fail"),
            Err(e) => assert_eq!(
                "From SQL error: `SqlType::UInt16 cannot be cast to u32.`".to_string(),
                format!("{}", e)
            ),
        }
    }
}
