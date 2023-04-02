use std::{iter::FusedIterator, marker, mem, ptr, slice};

use crate::{
    column::{Column, Simple},
    error::{Error, FromSqlError, Result},
    types::SqlType,
};

use crate::column::{column_data::ArcColumnData, ColumnType, Complex, StringPool};

fn check_type(src: &SqlType, dst: &SqlType) -> bool {
    src == dst
}

macro_rules! simple_num_iterable {
    ( $($t:ty: $k:ident),* ) => {
        $(
            impl<'a> Iterable<'a, Simple> for $t {
                type Iter = slice::Iter<'a, $t>;

                fn iter_with_props(column: &'a Column<Simple>, column_type: SqlType, props: u32) -> Result<Self::Iter> {
                    if !check_type(&column_type, &SqlType::$k) {
                        return Err(Error::FromSql(FromSqlError::InvalidType {
                            src: column.sql_type().to_string().into(),
                            dst: SqlType::$k.to_string().into(),
                        }))
                    }

                    unsafe {
                        let mut ptr: *const u8 = ptr::null();
                        let mut size: usize = 0;
                        column.get_internal(
                            &[&mut ptr, &mut size as *mut usize as *mut *const u8],
                            0,
                            props,
                        )?;
                        assert_ne!(ptr, ptr::null());
                        Ok(slice::from_raw_parts(ptr as *const $t, size).iter())
                    }
                }
            }
        )*
    };
}

simple_num_iterable! {
    u8: UInt8,
    u16: UInt16,
    u32: UInt32,
    u64: UInt64,

    i8: Int8,
    i16: Int16,
    i32: Int32,
    i64: Int64,

    f32: Float32,
    f64: Float64
}

pub trait Iterable<'a, K: ColumnType> {
    type Iter: Iterator;

    fn iter(column: &'a Column<K>, column_type: SqlType) -> Result<Self::Iter> {
        Self::iter_with_props(column, column_type, 0)
    }

    fn iter_with_props(
        column: &'a Column<K>,
        column_type: SqlType,
        _props: u32,
    ) -> Result<Self::Iter>;
}

enum StringInnerIterator<'a> {
    String(&'a StringPool),
    FixedString(*const u8, usize),
}

pub struct StringIterator<'a> {
    inner: StringInnerIterator<'a>,
    index: usize,
    size: usize,
}

pub struct NullableIterator<'a, I> {
    inner: I,
    ptr: *const u8,
    end: *const u8,
    _marker: marker::PhantomData<&'a ()>,
}

pub struct ArrayIterator<'a, I> {
    inner: I,
    offsets: &'a [u64],
    index: usize,
    size: usize,
}

impl ExactSizeIterator for StringIterator<'_> {
    #[inline(always)]
    fn len(&self) -> usize {
        self.size - self.index
    }
}

impl<'a> Iterator for StringIterator<'a> {
    type Item = &'a [u8];

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        match self.inner {
            StringInnerIterator::String(string_pool) => {
                if self.index == self.size {
                    None
                } else {
                    let old_index = self.index;
                    self.index += 1;
                    Some(unsafe { string_pool.get_unchecked(old_index) })
                }
            }
            StringInnerIterator::FixedString(buffer, str_len) => {
                if self.index >= self.size {
                    None
                } else {
                    let shift = self.index * str_len;
                    self.index += 1;
                    unsafe {
                        let ptr = buffer.add(shift);
                        Some(slice::from_raw_parts(ptr, str_len))
                    }
                }
            }
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let exact = self.len();
        (exact, Some(exact))
    }

    #[inline]
    fn count(self) -> usize {
        self.len()
    }

    #[inline]
    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        if n >= self.len() {
            // This iterator is now empty.
            self.index = self.size;
            return None;
        }

        self.index += n;
        self.next()
    }
}

impl<'a, I> ExactSizeIterator for NullableIterator<'a, I>
where
    I: Iterator,
{
    #[inline(always)]
    fn len(&self) -> usize {
        let start = self.ptr;
        self.end as usize - start as usize
    }
}

impl<'a, I> Iterator for NullableIterator<'a, I>
where
    I: Iterator,
{
    type Item = Option<I::Item>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.ptr == self.end {
            return None;
        }

        let value = self.inner.next()?;
        unsafe {
            let flag = *self.ptr;
            self.ptr = self.ptr.offset(1);

            Some(if flag != 0 { None } else { Some(value) })
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let exact = self.len();
        (exact, Some(exact))
    }

    #[inline]
    fn count(self) -> usize {
        self.len()
    }
}

impl<'a, I: Iterator> FusedIterator for NullableIterator<'a, I> {}

impl<'a, I: Iterator> ExactSizeIterator for ArrayIterator<'a, I> {
    #[inline(always)]
    fn len(&self) -> usize {
        self.size - self.index
    }
}

impl<'a, I: Iterator> Iterator for ArrayIterator<'a, I> {
    type Item = Vec<I::Item>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index == self.size {
            return None;
        }

        let start = if self.index > 0 {
            self.offsets[self.index - 1] as usize
        } else {
            0_usize
        };
        let end = self.offsets[self.index] as usize;

        let size = end - start;

        let mut v = Vec::with_capacity(size);
        for _ in 0..size {
            if let Some(item) = self.inner.next() {
                v.push(item);
            }
        }

        self.index += 1;
        Some(v)
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let exact = self.len();
        (exact, Some(exact))
    }

    #[inline]
    fn count(self) -> usize {
        self.len()
    }
}

impl<'a, I: Iterator> FusedIterator for ArrayIterator<'a, I> {}

impl<'a> Iterable<'a, Simple> for &[u8] {
    type Iter = StringIterator<'a>;

    fn iter_with_props(
        column: &'a Column<Simple>,
        column_type: SqlType,
        props: u32,
    ) -> Result<Self::Iter> {
        let mut size: usize = 0;
        let inner = match column_type {
            SqlType::String => {
                let string_pool = unsafe {
                    let mut string_pool: *const u8 = ptr::null();
                    column.get_internal(
                        &[&mut string_pool, &mut size as *mut usize as *mut *const u8],
                        0,
                        props,
                    )?;
                    &*(string_pool as *const StringPool)
                };
                StringInnerIterator::String(string_pool)
            }
            SqlType::FixedString(str_len) => {
                let buffer = unsafe {
                    let mut buffer: *const u8 = ptr::null();
                    column.get_internal(
                        &[&mut buffer, &mut size as *mut usize as *mut *const u8],
                        0,
                        0,
                    )?;
                    assert_ne!(buffer, ptr::null());
                    buffer
                };

                StringInnerIterator::FixedString(buffer, str_len)
            }
            _ => {
                return Err(Error::FromSql(FromSqlError::InvalidType {
                    src: column.sql_type().to_string(),
                    dst: SqlType::String.to_string(),
                }))
            }
        };

        Ok(StringIterator {
            inner,
            size,
            index: 0,
        })
    }
}

impl<'a, T> Iterable<'a, Simple> for Option<T>
where
    T: Iterable<'a, Simple>,
{
    type Iter = NullableIterator<'a, T::Iter>;

    fn iter_with_props(
        column: &'a Column<Simple>,
        column_type: SqlType,
        props: u32,
    ) -> Result<Self::Iter> {
        let inner = if let SqlType::Nullable(inner_type) = column_type {
            T::iter(column, inner_type.clone())?
        } else {
            return Err(Error::FromSql(FromSqlError::InvalidType {
                src: column.sql_type().to_string(),
                dst: "Nullable".into(),
            }));
        };

        let (ptr, end) = unsafe {
            let mut ptr: *const u8 = ptr::null();
            let mut size: usize = 0;
            column.get_internal(
                &[&mut ptr, &mut size as *mut usize as *mut *const u8],
                column_type.level(),
                props,
            )?;
            assert_ne!(ptr, ptr::null());
            let end = ptr.add(size);
            (ptr, end)
        };

        Ok(NullableIterator {
            inner,
            ptr,
            end,
            _marker: marker::PhantomData,
        })
    }
}

impl<'a, T> Iterable<'a, Simple> for Vec<T>
where
    T: Iterable<'a, Simple>,
{
    type Iter = ArrayIterator<'a, T::Iter>;

    fn iter_with_props(
        column: &'a Column<Simple>,
        column_type: SqlType,
        props: u32,
    ) -> Result<Self::Iter> {
        let inner = if let SqlType::Array(inner_type) = column_type {
            T::iter_with_props(column, inner_type.clone(), props)?
        } else {
            return Err(Error::FromSql(FromSqlError::InvalidType {
                src: column.sql_type().to_string(),
                dst: "Array".into(),
            }));
        };

        let mut size: usize = 0;
        let offsets = unsafe {
            let mut ptr: *const u8 = ptr::null();
            column.get_internal(
                &[&mut ptr, &mut size as *mut usize as *mut *const u8],
                column_type.level(),
                0,
            )?;
            assert_ne!(ptr, ptr::null());
            slice::from_raw_parts(ptr as *const u64, size)
        };

        Ok(ArrayIterator {
            inner,
            offsets,
            index: 0,
            size,
        })
    }
}

pub struct ComplexIterator<'a, T>
where
    T: Iterable<'a, Simple>,
{
    column_type: SqlType,

    data: &'a Vec<ArcColumnData>,

    current_index: usize,
    current: Option<<T as Iterable<'a, Simple>>::Iter>,

    _marker: marker::PhantomData<T>,
}

impl<'a, T> Iterator for ComplexIterator<'a, T>
where
    T: Iterable<'a, Simple>,
{
    type Item = <<T as Iterable<'a, Simple>>::Iter as Iterator>::Item;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_index == self.data.len() && self.current.is_none() {
            return None;
        }

        if self.current.is_none() {
            let column: Column<Simple> = Column {
                name: String::new(),
                data: self.data[self.current_index].clone(),
                _marker: marker::PhantomData,
            };

            let iter =
                unsafe { T::iter(mem::transmute(&column), self.column_type.clone()) }.unwrap();

            self.current = Some(iter);
            self.current_index += 1;
        }

        let ret = match self.current {
            None => None,
            Some(ref mut iter) => iter.next(),
        };

        match ret {
            None => {
                self.current = None;
                self.next()
            }
            Some(r) => Some(r),
        }
    }
}

impl<'a, T> Iterable<'a, Complex> for T
where
    T: Iterable<'a, Simple> + 'a,
{
    type Iter = ComplexIterator<'a, T>;

    fn iter_with_props(
        column: &Column<Complex>,
        column_type: SqlType,
        props: u32,
    ) -> Result<Self::Iter> {
        let data: &Vec<ArcColumnData> = unsafe {
            let mut data: *const Vec<ArcColumnData> = ptr::null();

            column.get_internal(
                &[&mut data as *mut *const Vec<ArcColumnData> as *mut *const u8],
                0xff,
                props,
            )?;

            &*data
        };

        Ok(ComplexIterator {
            column_type,
            data,

            current_index: 0,
            current: None,

            _marker: marker::PhantomData,
        })
    }
}
