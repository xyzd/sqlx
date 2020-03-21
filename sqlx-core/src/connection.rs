use std::convert::TryInto;

use futures_core::future::BoxFuture;

use crate::executor::Executor;
use crate::maybe_owned::MaybeOwned;
use crate::pool::{Pool, PoolConnection};
use crate::transaction::Transaction;
use crate::url::Url;

/// Represents a single database connection rather than a pool of database connections.
///
/// Prefer running queries from [Pool] unless there is a specific need for a single, continuous
/// connection.
pub trait Connection
where
    Self: Send + 'static,
    Self: Executor,
{
    /// Starts a transaction.
    ///
    /// Returns [`Transaction`](struct.Transaction.html).
    fn begin(self) -> BoxFuture<'static, crate::Result<Self::Database, Transaction<Self>>>
    where
        Self: Sized,
    {
        Box::pin(Transaction::new(0, self))
    }

    /// Close this database connection.
    fn close(self) -> BoxFuture<'static, crate::Result<Self::Database, ()>>;

    /// Verifies a connection to the database is still alive.
    fn ping(&mut self) -> BoxFuture<crate::Result<Self::Database, ()>>;
}

/// Represents a type that can directly establish a new connection.
pub trait Connect: Connection {
    /// Establish a new database connection.
    fn connect<T>(url: T) -> BoxFuture<'static, crate::Result<Self::Database, Self>>
    where
        T: TryInto<Url, Error = url::ParseError>,
        Self: Sized;
}

pub(crate) enum ConnectionSource<'c, C>
where
    C: Connect,
{
    Connection(MaybeOwned<PoolConnection<C>, &'c mut C>),

    #[allow(dead_code)]
    Pool(Pool<C>),
}

impl<'c, C> ConnectionSource<'c, C>
where
    C: Connect,
{
    #[allow(dead_code)]
    pub(crate) async fn resolve(&mut self) -> crate::Result<C::Database, &'_ mut C> {
        if let ConnectionSource::Pool(pool) = self {
            *self = ConnectionSource::Connection(MaybeOwned::Owned(pool.acquire().await?));
        }

        Ok(match self {
            ConnectionSource::Connection(conn) => match conn {
                MaybeOwned::Borrowed(conn) => &mut *conn,
                MaybeOwned::Owned(ref mut conn) => conn,
            },
            ConnectionSource::Pool(_) => unreachable!(),
        })
    }
}
