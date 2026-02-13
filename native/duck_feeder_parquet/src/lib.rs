use std::collections::BTreeSet;
use std::fs::File;
use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use rustler::Atom;
use serde_json::Value;

mod atoms {
    rustler::atoms! {
        ok,
        error
    }
}

#[rustler::nif(schedule = "DirtyIo")]
fn nif_write_parquet(path: String, rows_json: String) -> Result<Atom, (Atom, String)> {
    match do_write_parquet(&path, &rows_json) {
        Ok(()) => Ok(atoms::ok()),
        Err(reason) => Err((atoms::error(), reason)),
    }
}

fn do_write_parquet(path: &str, rows_json: &str) -> Result<(), String> {
    let rows: Vec<Value> = serde_json::from_str(rows_json)
        .map_err(|e| format!("invalid_rows_json: {}", e))?;

    let columns = collect_columns(&rows);
    let schema = Arc::new(Schema::new(
        columns
            .iter()
            .map(|name| Field::new(name, DataType::Utf8, true))
            .collect::<Vec<_>>(),
    ));

    let arrays = columns
        .iter()
        .map(|column| {
            let values: Vec<Option<String>> = rows
                .iter()
                .map(|row| {
                    row.get(column)
                        .and_then(|value| value_to_string(value))
                })
                .collect();

            Arc::new(StringArray::from(values)) as ArrayRef
        })
        .collect::<Vec<_>>();

    let batch = RecordBatch::try_new(schema.clone(), arrays)
        .map_err(|e| format!("record_batch_failed: {}", e))?;

    let file = File::create(path).map_err(|e| format!("create_file_failed: {}", e))?;
    let mut writer =
        ArrowWriter::try_new(file, schema, None).map_err(|e| format!("writer_init_failed: {}", e))?;

    writer
        .write(&batch)
        .map_err(|e| format!("writer_write_failed: {}", e))?;

    writer
        .close()
        .map_err(|e| format!("writer_close_failed: {}", e))?;

    Ok(())
}

fn collect_columns(rows: &[Value]) -> Vec<String> {
    let mut set = BTreeSet::new();

    for row in rows {
        if let Value::Object(map) = row {
            for key in map.keys() {
                set.insert(key.clone());
            }
        }
    }

    set.into_iter().collect()
}

fn value_to_string(value: &Value) -> Option<String> {
    match value {
        Value::Null => None,
        Value::String(s) => Some(s.clone()),
        Value::Bool(b) => Some(b.to_string()),
        Value::Number(n) => Some(n.to_string()),
        Value::Array(_) | Value::Object(_) => serde_json::to_string(value).ok(),
    }
}

rustler::init!("Elixir.DuckFeeder.Writer.ParquetNif");
