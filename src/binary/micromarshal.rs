use ethnum::I256;

// Marshal //

pub trait Marshal {
    fn marshal(&self, scratch: &mut [u8]);
}

macro_rules! impl_marshal {
    ( $( $t:ident),* ) => {
        $(
            impl Marshal for $t {
                fn marshal(&self, scratch: &mut [u8]) {
                    scratch[..].copy_from_slice(&self.to_le_bytes());
                }
            }
        )*
    };
}

impl_marshal! {
    u16, u32, u64, i16, i32, i64, I256
}

impl Marshal for u8 {
    fn marshal(&self, scratch: &mut [u8]) {
        scratch[0] = *self;
    }
}

impl Marshal for i8 {
    fn marshal(&self, scratch: &mut [u8]) {
        scratch[0] = *self as u8;
    }
}

impl Marshal for f32 {
    fn marshal(&self, scratch: &mut [u8]) {
        let bits = self.to_bits();
        let bytes = bits.to_le_bytes();
        scratch.copy_from_slice(&bytes);
    }
}

impl Marshal for f64 {
    fn marshal(&self, scratch: &mut [u8]) {
        let bits = self.to_bits();
        let bytes = bits.to_le_bytes();
        scratch.copy_from_slice(&bytes);
    }
}

impl Marshal for bool {
    fn marshal(&self, scratch: &mut [u8]) {
        scratch[0] = *self as u8;
    }
}

// Unmarshal //

pub trait Unmarshal<T: Copy> {
    fn unmarshal(scratch: &[u8]) -> T;
}

macro_rules! impl_unmarshal {
    ( $( $t:ident),* ) => {
        $(
            impl Unmarshal<$t> for $t {
                fn unmarshal(scratch: &[u8]) -> $t {
                    $t::from_le_bytes(
                        scratch.try_into().unwrap_or_else(|e| {
                            panic!("Error `{e}` on Unmarshal from `{scratch:?}`.")
                        }),
                    )
                }
            }
        )*
    };
}

impl Unmarshal<u8> for u8 {
    fn unmarshal(scratch: &[u8]) -> Self {
        scratch[0]
    }
}

impl Unmarshal<i8> for i8 {
    fn unmarshal(scratch: &[u8]) -> Self {
        scratch[0] as Self
    }
}

impl_unmarshal! {
    u16, u32, u64, i16, i32, i64, I256
}

impl Unmarshal<f32> for f32 {
    fn unmarshal(scratch: &[u8]) -> Self {
        let bits = u32::from_le_bytes(scratch.try_into().unwrap());
        Self::from_bits(bits)
    }
}

impl Unmarshal<f64> for f64 {
    fn unmarshal(scratch: &[u8]) -> Self {
        let bits = u64::from_le_bytes(scratch.try_into().unwrap());
        Self::from_bits(bits)
    }
}

impl Unmarshal<bool> for bool {
    fn unmarshal(scratch: &[u8]) -> Self {
        scratch[0] != 0
    }
}

#[cfg(test)]
mod test {
    use std::fmt;

    use rand::distributions::{Distribution, Standard};
    use rand::random;

    use super::{Marshal, Unmarshal};
    use crate::types::StatBuffer;

    fn test_some<T>()
    where
        T: Copy + fmt::Debug + StatBuffer + Marshal + Unmarshal<T> + PartialEq,
        Standard: Distribution<T>,
    {
        for _ in 0..100 {
            let mut buffer = T::buffer();
            let v = random::<T>();

            v.marshal(buffer.as_mut());
            let u = T::unmarshal(buffer.as_ref());

            assert_eq!(v, u);
        }
    }

    #[test]
    fn test_u8() {
        test_some::<u8>()
    }

    #[test]
    fn test_u16() {
        test_some::<u16>()
    }

    #[test]
    fn test_u32() {
        test_some::<u32>()
    }

    #[test]
    fn test_u64() {
        test_some::<u64>()
    }

    #[test]
    fn test_i8() {
        test_some::<i8>()
    }

    #[test]
    fn test_i16() {
        test_some::<i16>()
    }

    #[test]
    fn test_i32() {
        test_some::<i32>()
    }

    #[test]
    fn test_i64() {
        test_some::<i64>()
    }

    #[test]
    fn test_f32() {
        test_some::<f32>()
    }

    #[test]
    fn test_f64() {
        test_some::<f64>()
    }

    #[test]
    fn test_bool() {
        test_some::<bool>()
    }
}
