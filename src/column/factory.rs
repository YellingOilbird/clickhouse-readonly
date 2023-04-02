use chrono_tz::Tz;
use ethnum::I256;

use crate::column::{
    array::ArrayColumnData, fixed_string::FixedStringColumnData, list::List,
    nullable::NullableColumnData, string::StringColumnData, ArcColumnWrapper, ColumnData,
    ColumnWrapper, VectorColumnData,
};

use crate::error::{Error, FromSqlError};
use crate::{binary::ReadEx, error::Result, types::SqlType};

macro_rules! match_str {
    ($arg:ident, {
        $( $($var:literal)|* => $doit:expr,)*
        _ => $dothat:block
    }) => {
        $(
            $(
                if $arg.eq_ignore_ascii_case($var) {
                    $doit
                } else
            )*
        )*
        $dothat
    }
}

impl dyn ColumnData {
    #[allow(clippy::cognitive_complexity)]
    pub(crate) fn load_data<W: ColumnWrapper, T: ReadEx>(
        reader: &mut T,
        type_name: &str,
        size: usize,
        tz: Tz,
    ) -> Result<W::Wrapper> {
        Ok(match_str!(type_name, {
            "UInt8" => W::wrap(VectorColumnData::<u8>::load(reader, size)?),
            "UInt16" => W::wrap(VectorColumnData::<u16>::load(reader, size)?),
            "UInt32" => W::wrap(VectorColumnData::<u32>::load(reader, size)?),
            "UInt64" => W::wrap(VectorColumnData::<u64>::load(reader, size)?),
            "Int8" | "TinyInt" => W::wrap(VectorColumnData::<i8>::load(reader, size)?),
            "Int16" | "SmallInt" => W::wrap(VectorColumnData::<i16>::load(reader, size)?),
            "Int32" | "Int" | "Integer" => W::wrap(VectorColumnData::<i32>::load(reader, size)?),
            "Int64" | "BigInt" => W::wrap(VectorColumnData::<i64>::load(reader, size)?),
            "Float32" | "Float" => W::wrap(VectorColumnData::<f32>::load(reader, size)?),
            "Float64" | "Double" => W::wrap(VectorColumnData::<f64>::load(reader, size)?),
            "Int256" => W::wrap(VectorColumnData::<I256>::load(reader, size)?),
            "String" | "Char" | "Varchar" | "Text" | "TinyText" | "MediumText" | "LongText" | "Blob" | "TinyBlob" | "MediumBlob" | "LongBlob" => W::wrap(StringColumnData::load(reader, size)?),
            _ => {
                if let Some(inner_type) = parse_nullable_type(type_name) {
                    W::wrap(NullableColumnData::load(reader, inner_type, size, tz)?)
                } else if let Some(str_len) = parse_fixed_string(type_name) {
                    W::wrap(FixedStringColumnData::load(reader, size, str_len)?)
                } else if let Some(inner_type) = parse_array_type(type_name) {
                    W::wrap(ArrayColumnData::load(reader, inner_type, size, tz)?)
                } else {
                    return Err(
                        Error::FromSql(
                            FromSqlError::UnsupportedColumnType(type_name.to_string().into())
                        )
                    );
                }
            }
        }))
    }

    pub(crate) fn from_type<W: ColumnWrapper>(
        sql_type: SqlType,
        capacity: usize,
    ) -> Result<W::Wrapper> {
        Ok(match sql_type {
            SqlType::UInt8 => W::wrap(VectorColumnData::<u8>::with_capacity(capacity)),
            SqlType::UInt16 => W::wrap(VectorColumnData::<u16>::with_capacity(capacity)),
            SqlType::UInt32 => W::wrap(VectorColumnData::<u32>::with_capacity(capacity)),
            SqlType::UInt64 => W::wrap(VectorColumnData::<u64>::with_capacity(capacity)),
            SqlType::Int8 => W::wrap(VectorColumnData::<i8>::with_capacity(capacity)),
            SqlType::Int16 => W::wrap(VectorColumnData::<i16>::with_capacity(capacity)),
            SqlType::Int32 => W::wrap(VectorColumnData::<i32>::with_capacity(capacity)),
            SqlType::Int64 => W::wrap(VectorColumnData::<i64>::with_capacity(capacity)),
            SqlType::Int256 => W::wrap(VectorColumnData::<I256>::with_capacity(capacity)),
            SqlType::String => W::wrap(StringColumnData::with_capacity(capacity)),
            SqlType::FixedString(len) => {
                W::wrap(FixedStringColumnData::with_capacity(capacity, len))
            }
            SqlType::Float32 => W::wrap(VectorColumnData::<f32>::with_capacity(capacity)),
            SqlType::Float64 => W::wrap(VectorColumnData::<f64>::with_capacity(capacity)),
            SqlType::Nullable(inner_type) => W::wrap(NullableColumnData {
                inner: <dyn ColumnData>::from_type::<ArcColumnWrapper>(
                    inner_type.clone(),
                    capacity,
                )?,
                nulls: Vec::new(),
            }),
            SqlType::Array(inner_type) => W::wrap(ArrayColumnData {
                inner: <dyn ColumnData>::from_type::<ArcColumnWrapper>(
                    inner_type.clone(),
                    capacity,
                )?,
                offsets: List::with_capacity(capacity),
            }),
        })
    }
}

fn parse_fixed_string(source: &str) -> Option<usize> {
    if !source.starts_with("FixedString") {
        return None;
    }

    let inner_size = &source[12..source.len() - 1];
    match inner_size.parse::<usize>() {
        Err(_) => None,
        Ok(value) => Some(value),
    }
}

fn parse_nullable_type(source: &str) -> Option<&str> {
    if !source.starts_with("Nullable") {
        return None;
    }

    let inner_type = &source[9..source.len() - 1];

    if inner_type.starts_with("Nullable") {
        return None;
    }

    Some(inner_type)
}

fn parse_array_type(source: &str) -> Option<&str> {
    if !source.starts_with("Array") {
        return None;
    }

    let inner_type = &source[6..source.len() - 1];
    Some(inner_type)
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_parse_array_type() {
        assert_eq!(parse_array_type("Array(UInt8)"), Some("UInt8"));
    }

    #[test]
    fn test_parse_nullable_type() {
        assert_eq!(parse_nullable_type("Nullable(Int8)"), Some("Int8"));
        assert_eq!(parse_nullable_type("Int8"), None);
        assert_eq!(parse_nullable_type("Nullable(Nullable(Int8))"), None);
    }

    #[test]
    fn test_parse_fixed_string() {
        assert_eq!(parse_fixed_string("FixedString(8)"), Some(8_usize));
        assert_eq!(parse_fixed_string("FixedString(zz)"), None);
        assert_eq!(parse_fixed_string("Int8"), None);
    }
}
