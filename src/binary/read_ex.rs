use crate::{
    column::StringPool,
    error::{DriverError, Error, Result},
    types::StatBuffer,
};
use std::{io, mem::MaybeUninit};

use crate::binary::micromarshal::Unmarshal;

pub(crate) trait ReadEx {
    fn read_bytes(&mut self, rv: &mut [u8]) -> Result<()>;
    fn read_scalar<V>(&mut self) -> Result<V>
    where
        V: Copy + Unmarshal<V> + StatBuffer;
    fn read_string(&mut self) -> Result<String>;
    fn skip_string(&mut self) -> Result<()>;
    fn read_uvarint(&mut self) -> Result<u64>;
    fn read_str_into_buffer(&mut self, pool: &mut StringPool) -> Result<()>;
}

const MAX_STACK_BUFFER_LEN: usize = 1024;

impl<T> ReadEx for T
where
    T: io::Read,
{
    fn read_bytes(&mut self, rv: &mut [u8]) -> Result<()> {
        let mut i = 0;
        while i < rv.len() {
            let res_nread = {
                let buf = &mut rv[i..];
                self.read(buf)
            };
            match res_nread {
                Ok(0) => {
                    let ret = io::Error::new(io::ErrorKind::WouldBlock, "would block");
                    return Err(ret.into());
                }
                Ok(nread) => i += nread,
                Err(e) => return Err(From::from(e)),
            }
        }
        Ok(())
    }

    fn read_scalar<V>(&mut self) -> Result<V>
    where
        V: Copy + Unmarshal<V> + StatBuffer,
    {
        let mut buffer = V::buffer();
        self.read_bytes(buffer.as_mut())?;
        Ok(V::unmarshal(buffer.as_ref()))
    }

    fn read_string(&mut self) -> Result<String> {
        let str_len = self.read_uvarint()? as usize;
        let mut buffer = vec![0_u8; str_len];
        self.read_bytes(buffer.as_mut())?;
        Ok(String::from_utf8(buffer)?)
    }

    fn skip_string(&mut self) -> Result<()> {
        let str_len = self.read_uvarint()? as usize;

        if str_len <= MAX_STACK_BUFFER_LEN {
            unsafe {
                let mut buffer: [MaybeUninit<u8>; MAX_STACK_BUFFER_LEN] =
                    MaybeUninit::uninit().assume_init();
                self.read_bytes(
                    &mut *(&mut buffer[..str_len] as *mut [MaybeUninit<u8>] as *mut [u8]),
                )?;
            }
        } else {
            let mut buffer = vec![0_u8; str_len];
            self.read_bytes(buffer.as_mut())?;
        }

        Ok(())
    }

    fn read_uvarint(&mut self) -> Result<u64> {
        let mut x = 0_u64;
        let mut s = 0_u32;
        let mut i = 0_usize;
        loop {
            let b: u8 = self.read_scalar()?;

            if b < 0x80 {
                if i > 9 || i == 9 && b > 1 {
                    return Err(Error::Driver(DriverError::Overflow));
                }
                return Ok(x | (u64::from(b) << s));
            }

            x |= u64::from(b & 0x7f) << s;
            s += 7;

            i += 1;
        }
    }

    fn read_str_into_buffer(&mut self, pool: &mut StringPool) -> Result<()> {
        let str_len = self.read_uvarint()? as usize;
        let buffer = pool.allocate(str_len);
        self.read_bytes(buffer)?;
        Ok(())
    }
}
