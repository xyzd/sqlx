//! Types and traits for decoding values from the database.

use crate::database::Database;
use crate::database::HasRawValue;

/// Decode a single value from the database.
pub trait Decode<'de, DB>
where
    Self: Sized + 'de,
    DB: Database,
{
    fn decode(value: <DB as HasRawValue<'de>>::RawValue) -> crate::Result<DB, Self>;
}
