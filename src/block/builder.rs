#![allow(unused)]

use std::{borrow::Cow, marker};

use crate::{
    block::{Block, ColumnIdx},
    column::{ArcColumnWrapper, Column, ColumnData, ColumnType},
    error::{Error, FromSqlError, Result},
    value::Value,
};

pub trait RowBuilder {
    fn apply<K: ColumnType>(self, block: &mut Block<K>) -> Result<()>;
}

pub struct RNil;

pub struct RCons<T>
where
    T: RowBuilder,
{
    key: Cow<'static, str>,
    value: Value,
    tail: T,
}

impl RNil {
    pub fn put(self, key: Cow<'static, str>, value: Value) -> RCons<Self> {
        RCons {
            key,
            value,
            tail: RNil,
        }
    }
}

impl<T> RCons<T>
where
    T: RowBuilder,
{
    pub fn put(self, key: Cow<'static, str>, value: Value) -> RCons<Self> {
        RCons {
            key,
            value,
            tail: self,
        }
    }
}

impl RowBuilder for RNil {
    #[inline(always)]
    fn apply<K: ColumnType>(self, _block: &mut Block<K>) -> Result<()> {
        Ok(())
    }
}

impl<T> RowBuilder for RCons<T>
where
    T: RowBuilder,
{
    #[inline(always)]
    fn apply<K: ColumnType>(self, block: &mut Block<K>) -> Result<()> {
        put_param(self.key, self.value, block)?;
        self.tail.apply(block)
    }
}

impl RowBuilder for Vec<(String, Value)> {
    fn apply<K: ColumnType>(self, block: &mut Block<K>) -> Result<()> {
        for (k, v) in self {
            put_param(k.into(), v, block)?;
        }
        Ok(())
    }
}

fn put_param<K: ColumnType>(
    key: Cow<'static, str>,
    value: Value,
    block: &mut Block<K>,
) -> Result<()> {
    let col_index = match key.as_ref().get_index(&block.columns) {
        Ok(col_index) => col_index,
        Err(Error::FromSql(FromSqlError::OutOfRange)) => {
            if block.row_count() <= 1 {
                let sql_type = From::from(value.clone());

                let column = Column {
                    name: key.clone().into(),
                    data: <dyn ColumnData>::from_type::<ArcColumnWrapper>(
                        sql_type,
                        block.capacity,
                    )?,
                    _marker: marker::PhantomData,
                };

                block.columns.push(column);
                return put_param(key, value, block);
            } else {
                return Err(Error::FromSql(FromSqlError::OutOfRange));
            }
        }
        Err(err) => return Err(err),
    };

    block.columns[col_index].push(value);
    Ok(())
}
