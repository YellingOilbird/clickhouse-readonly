use std::{cmp, default::Default, fmt, io::Read, marker::PhantomData};

use ethnum::I256;

use crate::{
    binary::{Encoder, ReadEx},
    column::{self, ArcColumnWrapper, Column, ColumnFrom, ColumnType, Simple},
    error::{Error, FromSqlError, Result},
    protocol,
    types::{FromSql, SqlType},
    Complex,
};

use self::chunk_iterator::ChunkIterator;
pub(crate) use self::row::BlockRef;
pub use self::{
    block_info::BlockInfo,
    builder::{RCons, RNil, RowBuilder},
    row::{Row, Rows},
};

mod block_info;
mod builder;
mod chunk_iterator;
mod row;

const INSERT_BLOCK_SIZE: usize = 1_048_576;
const DEFAULT_CAPACITY: usize = 100;

pub trait ColumnIdx {
    fn get_index<K: ColumnType>(&self, columns: &[Column<K>]) -> Result<usize>;
}

pub trait Sliceable {
    fn slice_type() -> SqlType;
}

macro_rules! sliceable {
    ( $($t:ty: $k:ident),* ) => {
        $(
            impl Sliceable for $t {
                fn slice_type() -> SqlType {
                    SqlType::$k
                }
            }
        )*
    };
}

sliceable! {
    u8: UInt8,
    u16: UInt16,
    u32: UInt32,
    u64: UInt64,

    i8: Int8,
    i16: Int16,
    i32: Int32,
    i64: Int64,
    I256: Int256
}

/// Represents Clickhouse Block
#[derive(Default)]
pub struct Block<K: ColumnType = Simple> {
    info: BlockInfo,
    columns: Vec<Column<K>>,
    capacity: usize,
}

impl Block<Simple> {
    pub(crate) fn concat(blocks: &[Self]) -> Block<Complex> {
        let first = blocks.first().expect("blocks should not be empty.");

        for block in blocks {
            assert_eq!(
                first.column_count(),
                block.column_count(),
                "all columns should have the same size."
            );
        }

        let num_columns = first.column_count();
        let mut columns = Vec::with_capacity(num_columns);
        for i in 0_usize..num_columns {
            let chunks = blocks.iter().map(|block| &block.columns[i]);
            columns.push(Column::concat(chunks));
        }

        Block {
            info: first.info,
            columns,
            capacity: blocks.iter().map(|b| b.capacity).sum(),
        }
    }
}

impl<L: ColumnType, R: ColumnType> PartialEq<Block<R>> for Block<L> {
    fn eq(&self, other: &Block<R>) -> bool {
        if self.columns.len() != other.columns.len() {
            return false;
        }

        for i in 0..self.columns.len() {
            if self.columns[i] != other.columns[i] {
                return false;
            }
        }

        true
    }
}

impl<K: ColumnType> Clone for Block<K> {
    fn clone(&self) -> Self {
        Self {
            info: self.info,
            columns: self.columns.iter().map(|c| (*c).clone()).collect(),
            capacity: self.capacity,
        }
    }
}

impl<K: ColumnType> AsRef<Block<K>> for Block<K> {
    fn as_ref(&self) -> &Self {
        self
    }
}

impl ColumnIdx for usize {
    #[inline(always)]
    fn get_index<K: ColumnType>(&self, _: &[Column<K>]) -> Result<usize> {
        Ok(*self)
    }
}

impl<'a> ColumnIdx for &'a str {
    fn get_index<K: ColumnType>(&self, columns: &[Column<K>]) -> Result<usize> {
        match columns
            .iter()
            .enumerate()
            .find(|(_, column)| column.name() == *self)
        {
            None => Err(Error::FromSql(FromSqlError::OutOfRange)),
            Some((index, _)) => Ok(index),
        }
    }
}

impl ColumnIdx for String {
    fn get_index<K: ColumnType>(&self, columns: &[Column<K>]) -> Result<usize> {
        self.as_str().get_index(columns)
    }
}

impl Block {
    /// Constructs a new, empty `Block`.
    pub fn new() -> Self {
        Self::with_capacity(DEFAULT_CAPACITY)
    }

    /// Constructs a new, empty `Block` with the specified capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            info: Default::default(),
            columns: vec![],
            capacity,
        }
    }

    pub(crate) fn load<R>(reader: &mut R, tz: chrono_tz::Tz) -> Result<Self>
    where
        R: Read + ReadEx,
    {
        Self::raw_load(reader, tz)
    }

    fn raw_load<R>(reader: &mut R, tz: chrono_tz::Tz) -> Result<Block<Simple>>
    where
        R: ReadEx,
    {
        let mut block = Block::new();
        block.info = BlockInfo::read(reader)?;

        let num_columns = reader.read_uvarint()?;
        let num_rows = reader.read_uvarint()?;

        for _ in 0..num_columns {
            let column = Column::read(reader, num_rows as usize, tz)?;
            block.append_column(column);
        }

        Ok(block)
    }
}

impl<K: ColumnType> Block<K> {
    /// Return the number of rows in the current block.
    pub fn row_count(&self) -> usize {
        match self.columns.first() {
            None => 0,
            Some(column) => column.len(),
        }
    }

    /// Return the number of columns in the current block.
    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    /// This method returns a slice of columns.
    #[inline(always)]
    pub fn columns(&self) -> &[Column<K>] {
        &self.columns
    }

    fn append_column(&mut self, column: Column<K>) {
        let column_len = column.len();

        if !self.columns.is_empty() && self.row_count() != column_len {
            panic!("all columns in block must have same size.")
        }

        self.columns.push(column);
    }

    /// Get the value of a particular cell of the block.
    pub fn get<'a, T, I>(&'a self, row: usize, col: I) -> Result<T>
    where
        T: FromSql<'a>,
        I: ColumnIdx + Copy,
    {
        let column_index = col.get_index(self.columns())?;
        T::from_sql(self.columns[column_index].at(row))
    }

    /// Add new column into this block
    pub fn add_column<S>(self, name: &str, values: S) -> Self
    where
        S: ColumnFrom,
    {
        self.column(name, values)
    }

    /// Add new column into this block
    pub fn column<S>(mut self, name: &str, values: S) -> Self
    where
        S: ColumnFrom,
    {
        let data = S::column_from::<ArcColumnWrapper>(values);
        let column = column::new_column(name, data);

        self.append_column(column);
        self
    }

    /// Returns true if the block contains no elements.
    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }

    /// This method returns a iterator of rows.
    pub fn rows(&self) -> Rows<K> {
        Rows {
            row: 0,
            block_ref: BlockRef::Borrowed(self),
            kind: PhantomData,
        }
    }

    /// This method is a convenient way to pass row into a block.
    pub fn push<B: RowBuilder>(&mut self, row: B) -> Result<()> {
        row.apply(self)
    }

    /// This method finds a column by identifier.
    pub fn get_column<I>(&self, col: I) -> Result<&Column<K>>
    where
        I: ColumnIdx + Copy,
    {
        let column_index = col.get_index(self.columns())?;
        let column = &self.columns[column_index];
        Ok(column)
    }
}

impl<K: ColumnType> Block<K> {
    pub(crate) fn write(&self, encoder: &mut Encoder) {
        self.info.write(encoder);
        encoder.uvarint(self.column_count() as u64);
        encoder.uvarint(self.row_count() as u64);

        for column in &self.columns {
            column.write(encoder);
        }
    }

    pub(crate) fn send_data(&self, encoder: &mut Encoder) {
        encoder.uvarint(protocol::CLIENT_DATA);
        encoder.string(""); // temporary table
        for chunk in self.chunks(INSERT_BLOCK_SIZE) {
            chunk.write(encoder);
        }
    }

    pub(crate) fn chunks(&self, n: usize) -> ChunkIterator<K> {
        ChunkIterator::new(n, self)
    }
}

impl<K: ColumnType> fmt::Debug for Block<K> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let titles: Vec<&str> = self.columns.iter().map(|column| column.name()).collect();

        let cells: Vec<_> = self.columns.iter().map(|col| text_cells(col)).collect();

        let titles_len: Vec<_> = titles
            .iter()
            .map(|t| t.chars().count())
            .zip(cells.iter().map(|w| column_width(w)))
            .map(|(a, b)| cmp::max(a, b))
            .collect();

        print_line(f, &titles_len, "\n\u{250c}", '┬', "\u{2510}\n")?;

        for (i, title) in titles.iter().enumerate() {
            write!(f, "\u{2502}{:>width$} ", title, width = titles_len[i] + 1)?;
        }
        write!(f, "\u{2502}")?;

        if self.row_count() > 0 {
            print_line(f, &titles_len, "\n\u{251c}", '┼', "\u{2524}\n")?;
        }

        for j in 0..self.row_count() {
            for (i, col) in cells.iter().enumerate() {
                write!(f, "\u{2502}{:>width$} ", col[j], width = titles_len[i] + 1)?;
            }

            let new_line = (j + 1) != self.row_count();
            write!(f, "\u{2502}{}", if new_line { "\n" } else { "" })?;
        }

        print_line(f, &titles_len, "\n\u{2514}", '┴', "\u{2518}")
    }
}

fn column_width(column: &[String]) -> usize {
    column.iter().map(|cell| cell.len()).max().unwrap_or(0)
}

fn print_line(
    f: &mut fmt::Formatter,
    lens: &[usize],
    left: &str,
    center: char,
    right: &str,
) -> fmt::Result {
    write!(f, "{}", left)?;
    for (i, len) in lens.iter().enumerate() {
        if i != 0 {
            write!(f, "{}", center)?;
        }

        write!(f, "{:\u{2500}>width$}", "", width = len + 2)?;
    }
    write!(f, "{}", right)
}

fn text_cells<K: ColumnType>(data: &Column<K>) -> Vec<String> {
    (0..data.len()).map(|i| format!("{}", data.at(i))).collect()
}
