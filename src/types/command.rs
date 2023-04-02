use log::trace;

use crate::{
    binary::Encoder,
    block::Block,
    client::Context,
    column::Simple,
    error::Result,
    protocol::{self, client_info},
    query::Query,
};

/// Represents Clickhouse commands.
pub(crate) enum Cmd {
    Hello(Context),
    Ping,
    SendQuery(Query, Context),
    Cancel,
}

impl Cmd {
    /// Returns the packed command as a byte vector.
    #[inline(always)]
    pub(crate) fn get_packed_command(&self) -> Result<Vec<u8>> {
        encode_command(self)
    }
}

fn encode_command(cmd: &Cmd) -> Result<Vec<u8>> {
    match cmd {
        Cmd::Hello(context) => encode_hello(context),
        Cmd::Ping => Ok(encode_ping()),
        Cmd::SendQuery(query, context) => encode_query(query, context),
        Cmd::Cancel => Ok(encode_cancel()),
    }
}

fn encode_hello(context: &Context) -> Result<Vec<u8>> {
    trace!("[hello]");

    let mut encoder = Encoder::new();
    encoder.uvarint(protocol::CLIENT_HELLO);
    client_info::write(&mut encoder);

    let config = context.config.clone();

    encoder.string(&config.database);
    encoder.string(&config.username);
    encoder.string(&config.password);

    Ok(encoder.get_buffer())
}

fn encode_ping() -> Vec<u8> {
    trace!("[ping]         -> ping");

    let mut encoder = Encoder::new();
    encoder.uvarint(protocol::CLIENT_PING);
    encoder.get_buffer()
}

fn encode_cancel() -> Vec<u8> {
    trace!("[cancel]");

    let mut encoder = Encoder::new();
    encoder.uvarint(protocol::CLIENT_CANCEL);
    encoder.get_buffer()
}

fn encode_query(query: &Query, context: &Context) -> Result<Vec<u8>> {
    trace!("[send query] {}", query.get_sql());

    let mut encoder = Encoder::new();
    encoder.uvarint(protocol::CLIENT_QUERY);
    encoder.string(""); // readonly

    {
        let hostname = &context.hostname;
        encoder.uvarint(1);
        encoder.string("");
        encoder.string(query.get_id()); // initial_query_id;
        encoder.string("[::ffff:127.0.0.1]:0");
        encoder.uvarint(1); // iface type TCP;
        encoder.string(hostname);
        encoder.string(hostname);
    }
    client_info::write(&mut encoder);

    if context.server_info.revision >= protocol::DBMS_MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO {
        encoder.string("");
    }

    // settings
    encoder.string(protocol::READONLY_FLAG);
    encoder.uvarint(protocol::READONLY_LEVEL);
    encoder.string("");

    encoder.uvarint(protocol::STATE_COMPLETE);

    encoder.uvarint(protocol::COMPRESS_DISABLE);

    encoder.string(query.get_sql());

    Block::<Simple>::default().send_data(&mut encoder);

    Ok(encoder.get_buffer())
}
