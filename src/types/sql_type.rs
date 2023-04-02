use std::{borrow::Cow, collections::HashMap, fmt, pin::Pin, sync::Mutex};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum SqlType {
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Int8,
    Int16,
    Int32,
    Int64,
    Int256,
    String,
    FixedString(usize),
    Float32,
    Float64,
    Nullable(&'static SqlType),
    Array(&'static SqlType),
}

lazy_static::lazy_static! {
    static ref TYPES_CACHE: Mutex<HashMap<SqlType, Pin<Box<SqlType>>>> = Mutex::new(HashMap::new());
}

impl From<SqlType> for &'static SqlType {
    fn from(value: SqlType) -> Self {
        match value {
            SqlType::UInt8 => &SqlType::UInt8,
            SqlType::UInt16 => &SqlType::UInt16,
            SqlType::UInt32 => &SqlType::UInt32,
            SqlType::UInt64 => &SqlType::UInt64,
            SqlType::Int8 => &SqlType::Int8,
            SqlType::Int16 => &SqlType::Int16,
            SqlType::Int32 => &SqlType::Int32,
            SqlType::Int64 => &SqlType::Int64,
            SqlType::Int256 => &SqlType::Int256,
            SqlType::String => &SqlType::String,
            SqlType::Float32 => &SqlType::Float32,
            SqlType::Float64 => &SqlType::Float64,
            _ => {
                let mut guard = TYPES_CACHE.lock().unwrap();
                loop {
                    if let Some(value_ref) = guard.get(&value.clone()) {
                        return unsafe { std::mem::transmute(value_ref.as_ref()) };
                    }
                    guard.insert(value.clone(), Box::pin(value.clone()));
                }
            }
        }
    }
}

impl SqlType {
    pub fn to_string(&self) -> Cow<'static, str> {
        match self.clone() {
            SqlType::UInt8 => "UInt8".into(),
            SqlType::UInt16 => "UInt16".into(),
            SqlType::UInt32 => "UInt32".into(),
            SqlType::UInt64 => "UInt64".into(),
            SqlType::Int8 => "Int8".into(),
            SqlType::Int16 => "Int16".into(),
            SqlType::Int32 => "Int32".into(),
            SqlType::Int64 => "Int64".into(),
            SqlType::Int256 => "Int256".into(),
            SqlType::String => "String".into(),
            SqlType::FixedString(str_len) => format!("FixedString({})", str_len).into(),
            SqlType::Float32 => "Float32".into(),
            SqlType::Float64 => "Float64".into(),
            SqlType::Nullable(nested) => format!("Nullable({})", &nested).into(),
            SqlType::Array(nested) => format!("Array({})", &nested).into(),
        }
    }

    pub(crate) fn level(&self) -> u8 {
        match self {
            SqlType::Nullable(inner) => 1 + inner.level(),
            SqlType::Array(inner) => 1 + inner.level(),
            _ => 0,
        }
    }
}

impl fmt::Display for SqlType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", Self::to_string(self))
    }
}
