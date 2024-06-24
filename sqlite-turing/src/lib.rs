use sqlite_loadable::prelude::*;
use sqlite_loadable::{api, define_scalar_function, Result};
use turing_beavers::turing::Table;

pub fn transition_table(
    context: *mut sqlite3_context,
    values: &[*mut sqlite3_value],
) -> Result<()> {
    let slice = api::value_blob(values.get(0).expect("1st argument as blob"));
    let table = Table::try_from(slice).map_err(|_| "Could not parse blob")?;
    api::result_text(context, table.to_string())?;
    Ok(())
}

#[sqlite_entrypoint]
pub fn sqlite3_sqliteturing_init(db: *mut sqlite3) -> Result<()> {
    define_scalar_function(
        db,
        "transition_table",
        1,
        transition_table,
        FunctionFlags::DETERMINISTIC,
    )?;
    Ok(())
}
