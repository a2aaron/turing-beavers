use std::str::FromStr;

use sqlite_loadable::prelude::*;
use sqlite_loadable::{api, define_scalar_function, Result};
use turing_beavers::sql::PackedTable;
use turing_beavers::turing::MachineTable;

pub fn transition_table(
    context: *mut sqlite3_context,
    values: &[*mut sqlite3_value],
) -> Result<()> {
    let slice = api::value_blob(values.get(0).expect("1st argument as blob"));
    let table = MachineTable::try_from(slice).map_err(|_| "Could not parse blob")?;
    api::result_text(context, table.to_string())?;
    Ok(())
}

pub fn transition_bytes(
    context: *mut sqlite3_context,
    values: &[*mut sqlite3_value],
) -> Result<()> {
    let string = api::value_text(values.get(0).expect("1st argument as string"))?;
    let table = MachineTable::from_str(string).map_err(|_| "Could not parse string")?;
    api::result_blob(context, &PackedTable::from(table));
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
    define_scalar_function(
        db,
        "transition_bytes",
        1,
        transition_bytes,
        FunctionFlags::UTF8 | FunctionFlags::DETERMINISTIC,
    )?;
    Ok(())
}
