use sqlx::connection::{Connect, Connection};
use sqlx::describe::Describe;
use sqlx::executor::{Executor, RefExecutor};
use url::Url;

use std::fmt::{self, Display, Formatter};

use crate::database::DatabaseExt;
use std::fs::File;
use syn::export::Span;

#[cfg_attr(feature = "offline", derive(serde::Deserialize, serde::Serialize))]
pub struct QueryData {
    pub(super) query: String,
    pub(super) input_types: Vec<Option<String>>,
    pub(super) outputs: Vec<(String, String)>,
}

impl QueryData {
    pub fn from_db(db_url: &str, query: &str) -> crate::Result<Self> {
        crate::runtime::block_on(async {
            let db_url = db_url.parse::<Url>()?;

            match db_url.scheme() {
                #[cfg(feature = "sqlite")]
                "sqlite" => {
                    let mut conn = sqlx::sqlite::SqliteConnection::connect(db_url.as_str())
                        .await
                        .map_err(|e| format!("failed to connect to database: {}", e))?;

                    describe_query(conn, query).await
                }
                #[cfg(not(feature = "sqlite"))]
                "sqlite" => Err(format!(
                    "database URL {} has the scheme of a SQLite database but the `sqlite` \
                     feature of sqlx was not enabled",
                    db_url
                )
                .into()),
                #[cfg(feature = "postgres")]
                "postgresql" | "postgres" => {
                    let mut conn = sqlx::postgres::PgConnection::connect(db_url.as_str())
                        .await
                        .map_err(|e| format!("failed to connect to database: {}", e))?;

                    describe_query(conn, query).await
                }
                #[cfg(not(feature = "postgres"))]
                "postgresql" | "postgres" => Err(format!(
                    "database URL {} has the scheme of a Postgres database but the `postgres` \
                     feature of sqlx was not enabled",
                    db_url
                )
                .into()),
                #[cfg(feature = "mysql")]
                "mysql" | "mariadb" => {
                    let mut conn = sqlx::mysql::MySqlConnection::connect(db_url.as_str())
                        .await
                        .map_err(|e| format!("failed to connect to database: {}", e))?;

                    describe_query(conn, query).await
                }
                #[cfg(not(feature = "mysql"))]
                "mysql" | "mariadb" => Err(format!(
                    "database URL {} has the scheme of a MySQL/MariaDB database but the `mysql` \
                     feature of sqlx was not enabled",
                    db_url
                )
                .into()),
                scheme => {
                    Err(format!("unexpected scheme {:?} in database URL {}", scheme, db_url).into())
                }
            }
        })
    }

    #[cfg(feature = "offline")]
    pub fn from_file(path: &str, query: &str) -> crate::Result<QueryData> {
        serde_json::from_reader(
            File::open(path).map_err(|e| format!("failed to open path {:?}: {}", path, e).into()),
        )
        .map_err(Into::into)
    }

    #[cfg(feature = "offline")]
    pub fn to_file(&self, path: &str) -> crate::Result<()> {
        serde_json::to_writer(
            File::create(path).map_err(|e| format!("failed to open path {:?}: {}", path, e).into()),
            self,
        )
        .map_err(Into::into)
    }
}

async fn describe_query<C: Connection>(mut conn: C, query: &str) -> crate::Result<QueryData>
where
    <C as Executor>::Database: DatabaseExt,
{
    let describe: Describe<<C as Executor>::Database> = conn.describe(query).await?;

    let input_types = describe
        .param_types
        .iter()
        .map(|param_ty| {
            <<C as Executor>::Database as DatabaseExt>::param_type_for_id(param_ty.as_ref()?)
                .map(Into::into)
        })
        .collect();

    let outputs = describe
        .result_columns
        .iter()
        .enumerate()
        .map(|(i, column)| -> crate::Result<_> {
            let name = column
                .name
                .cloned()
                .ok_or_else(|| format!("column at position {} must have a name", i))?;

            let type_info = column.type_info.ok_or_else(|| {
                syn::Error::new(
                    Span::call_site(),
                    format!(
                        "database couldn't tell us the type of {col}; \
                     this can happen for columns that are the result of an expression",
                        col = DisplayColumn {
                            idx: i,
                            name: column.name.as_deref()
                        }
                    ),
                )
            })?;

            let type_ = <<C as Executor>::Database as DatabaseExt>::return_type_for_id(&type_info)
                .ok_or_else(|| {
                    let message = if let Some(feature_gate) =
                        <<C as Executor>::Database as DatabaseExt>::get_feature_gate(&type_info)
                    {
                        format!(
                            "optional feature `{feat}` required for type {ty} of {col}",
                            ty = &type_info,
                            feat = feature_gate,
                            col = DisplayColumn {
                                idx: i,
                                name: column.name.as_deref()
                            }
                        )
                    } else {
                        format!(
                            "unsupported type {ty} of {col}",
                            ty = type_info,
                            col = DisplayColumn {
                                idx: i,
                                name: column.name.as_deref()
                            }
                        )
                    };
                    syn::Error::new(Span::call_site(), message)
                })?;

            let type_ = if column.non_null.unwrap_or(false) {
                format!("Option<{}>", type_)
            } else {
                type_.into()
            };

            Ok((name, type_))
        })
        .collect::<crate::Result<Vec<_>>>()?;

    Ok(QueryData {
        query: query.into(),
        input_types,
        outputs,
    })
}

struct DisplayColumn<'a> {
    // zero-based index, converted to 1-based number
    idx: usize,
    name: Option<&'a str>,
}

impl Display for DisplayColumn<'_> {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let num = self.idx + 1;

        if let Some(name) = self.name {
            write!(f, "column #{} ({:?})", num, name)
        } else {
            write!(f, "column #{}", num)
        }
    }
}
