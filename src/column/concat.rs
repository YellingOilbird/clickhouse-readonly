use crate::column::column_data::{ArcColumnData, BoxColumnData, ColumnData};

use crate::error::{Error, FromSqlError};
use crate::{
    binary::Encoder,
    error::Result,
    types::SqlType,
    value::{Value, ValueRef},
};

pub struct ConcatColumnData {
    data: Vec<ArcColumnData>,
    index: Vec<usize>,
}

impl ColumnData for ConcatColumnData {
    fn sql_type(&self) -> SqlType {
        self.data[0].sql_type()
    }

    fn save(&self, _: &mut Encoder, _: usize, _: usize) {
        unimplemented!()
    }

    fn len(&self) -> usize {
        *self.index.last().unwrap()
    }

    fn push(&mut self, _value: Value) {
        unimplemented!()
    }

    fn at(&self, index: usize) -> ValueRef {
        let chunk_index = find_chunk(&self.index, index);
        let chunk = &self.data[chunk_index];
        chunk.at(index - self.index[chunk_index])
    }

    fn clone_instance(&self) -> BoxColumnData {
        unimplemented!()
    }

    unsafe fn get_internal(
        &self,
        pointers: &[*mut *const u8],
        level: u8,
        _props: u32,
    ) -> Result<()> {
        if level == 0xff {
            *pointers[0] = &self.data as *const Vec<ArcColumnData> as *mut u8;
            Ok(())
        } else {
            Err(Error::FromSql(FromSqlError::UnsupportedOperation))
        }
    }
}

impl ConcatColumnData {
    pub(crate) fn concat(data: Vec<ArcColumnData>) -> Self {
        Self::check_columns(&data);

        let index = build_index(data.iter().map(|x| x.len()));
        Self { data, index }
    }

    fn check_columns(data: &[ArcColumnData]) {
        match data.first() {
            None => panic!("data should not be empty."),
            Some(first) => {
                for column in data.iter().skip(1) {
                    if first.sql_type() != column.sql_type() {
                        panic!(
                            "all columns should have the same type ({:?} != {:?}).",
                            first.sql_type(),
                            column.sql_type()
                        );
                    }
                }
            }
        }
    }
}

fn build_index<'a, I>(sizes: I) -> Vec<usize>
where
    I: std::iter::Iterator<Item = usize> + 'a,
{
    let mut acc = 0;
    let mut index = vec![acc];

    for size in sizes {
        acc += size;
        index.push(acc);
    }

    index
}

fn find_chunk(index: &[usize], ix: usize) -> usize {
    let mut lo = 0_usize;
    let mut hi = index.len() - 1;

    while lo < hi {
        let mid = lo + (hi - lo) / 2;

        if index[lo] == index[lo + 1] {
            lo += 1;
            continue;
        }

        if ix < index[mid] {
            hi = mid;
        } else if ix >= index[mid + 1] {
            lo = mid + 1;
        } else {
            return mid;
        }
    }

    0
}

#[cfg(test)]
mod test {
    use super::*;

    fn build_index<'a, I>(sizes: I) -> Vec<usize>
    where
        I: std::iter::Iterator<Item = usize> + 'a,
    {
        let mut acc = 0;
        let mut index = vec![acc];

        for size in sizes {
            acc += size;
            index.push(acc);
        }

        index
    }

    #[test]
    fn test_build_index() {
        let sizes = vec![2_usize, 3, 4];
        let index = build_index(sizes.iter().cloned());
        assert_eq!(index, vec![0, 2, 5, 9])
    }

    #[test]
    fn test_find_chunk() {
        let index = vec![0_usize, 2, 5, 9];
        assert_eq!(find_chunk(&index, 0), 0);
        assert_eq!(find_chunk(&index, 1), 0);
        assert_eq!(find_chunk(&index, 2), 1);
        assert_eq!(find_chunk(&index, 3), 1);
        assert_eq!(find_chunk(&index, 4), 1);
        assert_eq!(find_chunk(&index, 5), 2);
        assert_eq!(find_chunk(&index, 6), 2);

        assert_eq!(find_chunk(&index, 7), 2);
        assert_eq!(find_chunk(&[0], 7), 0);
    }

    #[test]
    fn test_find_chunk2() {
        let index = vec![0_usize, 0, 5];
        assert_eq!(find_chunk(&index, 0), 1);
        assert_eq!(find_chunk(&index, 1), 1);
        assert_eq!(find_chunk(&index, 2), 1);
        assert_eq!(find_chunk(&index, 3), 1);
        assert_eq!(find_chunk(&index, 4), 1);
        assert_eq!(find_chunk(&index, 5), 0);
    }

    #[test]
    fn test_find_chunk5() {
        let index = vec![
            0_usize, 0, 3, 6, 9, 12, 15, 18, 21, 24, 27, 30, 33, 36, 39, 42, 45, 48, 51, 54, 57,
            60, 63, 66, 69,
        ];
        for i in 0..69 {
            assert_eq!(find_chunk(&index, i), 1 + i / 3);
        }
    }
}
