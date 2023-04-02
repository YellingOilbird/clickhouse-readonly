//! Clickhouse protocol keys receiver and parsed at packets.

pub const DBMS_MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO: u64 = 54060;
//pub const DBMS_MIN_REVISION_WITH_SETTINGS_SERIALIZED_AS_STRINGS: u64 = 54429;

/// Allows only read from DataBase
pub const READONLY_LEVEL: u64 = 1;
pub const READONLY_FLAG: &str = "readonly";

pub const CLIENT_HELLO: u64 = 0;
pub const CLIENT_QUERY: u64 = 1;
pub const CLIENT_DATA: u64 = 2;
pub const CLIENT_CANCEL: u64 = 3;
pub const CLIENT_PING: u64 = 4;

// pub const COMPRESS_ENABLE: u64 = 1;
pub const COMPRESS_DISABLE: u64 = 0;

pub const STATE_COMPLETE: u64 = 2;

pub const SERVER_HELLO: u64 = 0;
pub const SERVER_DATA: u64 = 1;
pub const SERVER_EXCEPTION: u64 = 2;
pub const SERVER_PROGRESS: u64 = 3;
pub const SERVER_PONG: u64 = 4;
pub const SERVER_END_OF_STREAM: u64 = 5;
pub const SERVER_PROFILE_INFO: u64 = 6;
pub const SERVER_TOTALS: u64 = 7;
pub const SERVER_EXTREMES: u64 = 8;

pub mod client_info {
    use crate::binary::Encoder;

    pub static CLIENT_NAME: &str = "RustCHDriver";

    pub const CLICK_HOUSE_REVISION: u64 = 54213;
    pub const CLICK_HOUSE_DBMSVERSION_MAJOR: u64 = 1;
    pub const CLICK_HOUSE_DBMSVERSION_MINOR: u64 = 1;

    pub fn write(encoder: &mut Encoder) {
        encoder.string(CLIENT_NAME);
        encoder.uvarint(CLICK_HOUSE_DBMSVERSION_MAJOR);
        encoder.uvarint(CLICK_HOUSE_DBMSVERSION_MINOR);
        encoder.uvarint(CLICK_HOUSE_REVISION);
    }
}
