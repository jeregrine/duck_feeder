use std::collections::BTreeSet;
use std::fs::File;
use std::sync::Arc;

use arrow_array::{ArrayRef, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use rustler::Atom;
use serde_json::{Number, Value};

mod atoms {
    rustler::atoms! {
        ok,
        error
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ColumnKind {
    Utf8,
    Boolean,
    Int64,
    Float64,
}

#[rustler::nif(schedule = "DirtyIo")]
fn nif_write_parquet(path: String, rows_json: String) -> Result<Atom, (Atom, String)> {
    match do_write_parquet(&path, &rows_json) {
        Ok(()) => Ok(atoms::ok()),
        Err(reason) => Err((atoms::error(), reason)),
    }
}

fn do_write_parquet(path: &str, rows_json: &str) -> Result<(), String> {
    let rows: Vec<Value> =
        serde_json::from_str(rows_json).map_err(|e| format!("invalid_rows_json: {}", e))?;

    let columns = collect_columns(&rows);
    let column_specs = columns
        .iter()
        .map(|name| (name.clone(), infer_column_kind(&rows, name)))
        .collect::<Vec<_>>();

    let schema = Arc::new(Schema::new(
        column_specs
            .iter()
            .map(|(name, kind)| Field::new(name, column_kind_to_data_type(*kind), true))
            .collect::<Vec<_>>(),
    ));

    let arrays = column_specs
        .iter()
        .map(|(column, kind)| build_array(&rows, column, *kind))
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

fn infer_column_kind(rows: &[Value], column: &str) -> ColumnKind {
    let mut inferred: Option<ColumnKind> = None;

    for row in rows {
        let Some(value) = row.get(column) else {
            continue;
        };

        let Some(value_kind) = classify_value_kind(value) else {
            continue;
        };

        inferred = Some(match inferred {
            None => value_kind,
            Some(existing) => merge_column_kinds(existing, value_kind),
        });

        if inferred == Some(ColumnKind::Utf8) {
            return ColumnKind::Utf8;
        }
    }

    inferred.unwrap_or(ColumnKind::Utf8)
}

fn classify_value_kind(value: &Value) -> Option<ColumnKind> {
    match value {
        Value::Null => None,
        Value::Bool(_) => Some(ColumnKind::Boolean),
        Value::Number(number) => Some(classify_number_kind(number)),
        Value::String(_) | Value::Array(_) | Value::Object(_) => Some(ColumnKind::Utf8),
    }
}

fn classify_number_kind(number: &Number) -> ColumnKind {
    if number.is_i64() {
        ColumnKind::Int64
    } else if number.is_u64() {
        match number.as_u64() {
            Some(value) if i64::try_from(value).is_ok() => ColumnKind::Int64,
            _ => ColumnKind::Utf8,
        }
    } else if number.is_f64() {
        ColumnKind::Float64
    } else {
        ColumnKind::Utf8
    }
}

fn merge_column_kinds(existing: ColumnKind, incoming: ColumnKind) -> ColumnKind {
    use ColumnKind::*;

    match (existing, incoming) {
        (Utf8, _) | (_, Utf8) => Utf8,
        (Boolean, Boolean) => Boolean,
        (Int64, Int64) => Int64,
        (Float64, Float64) => Float64,
        (Int64, Float64) | (Float64, Int64) => Float64,
        _ => Utf8,
    }
}

fn column_kind_to_data_type(kind: ColumnKind) -> DataType {
    match kind {
        ColumnKind::Utf8 => DataType::Utf8,
        ColumnKind::Boolean => DataType::Boolean,
        ColumnKind::Int64 => DataType::Int64,
        ColumnKind::Float64 => DataType::Float64,
    }
}

fn build_array(rows: &[Value], column: &str, kind: ColumnKind) -> ArrayRef {
    match kind {
        ColumnKind::Utf8 => {
            let values: Vec<Option<String>> = rows
                .iter()
                .map(|row| row.get(column).and_then(value_to_string))
                .collect();

            Arc::new(StringArray::from(values)) as ArrayRef
        }
        ColumnKind::Boolean => {
            let values: Vec<Option<bool>> = rows
                .iter()
                .map(|row| row.get(column).and_then(value_to_bool))
                .collect();

            Arc::new(BooleanArray::from(values)) as ArrayRef
        }
        ColumnKind::Int64 => {
            let values: Vec<Option<i64>> = rows
                .iter()
                .map(|row| row.get(column).and_then(value_to_i64))
                .collect();

            Arc::new(Int64Array::from(values)) as ArrayRef
        }
        ColumnKind::Float64 => {
            let values: Vec<Option<f64>> = rows
                .iter()
                .map(|row| row.get(column).and_then(value_to_f64))
                .collect();

            Arc::new(Float64Array::from(values)) as ArrayRef
        }
    }
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

fn value_to_bool(value: &Value) -> Option<bool> {
    match value {
        Value::Null => None,
        Value::Bool(value) => Some(*value),
        _ => None,
    }
}

fn value_to_i64(value: &Value) -> Option<i64> {
    match value {
        Value::Null => None,
        Value::Number(number) => number
            .as_i64()
            .or_else(|| number.as_u64().and_then(|value| i64::try_from(value).ok())),
        _ => None,
    }
}

fn value_to_f64(value: &Value) -> Option<f64> {
    match value {
        Value::Null => None,
        Value::Number(number) => number
            .as_f64()
            .or_else(|| number.as_i64().map(|value| value as f64))
            .or_else(|| number.as_u64().map(|value| value as f64)),
        _ => None,
    }
}

rustler::init!("Elixir.DuckFeeder.Writer.ParquetNif");
