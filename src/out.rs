//! Parquet output through the parquet crate's low-level column writers (no Arrow).

use parquet::basic::Compression;
use parquet::data_type::{ByteArray, ByteArrayType, Int32Type, Int64Type};
use parquet::file::properties::{EnabledStatistics, WriterProperties, WriterVersion};
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;
use std::fs::File;
use std::io::BufWriter;
use std::path::Path;
use std::sync::Arc;

pub const USER_SCHEMA: &str = "message user_daily {
  required binary user_id (UTF8);
  required int32 day (DATE);
  required int64 sessions;
  required int64 events;
  required int64 revenue_cents;
  optional binary top_category (UTF8);
}";

pub const CAMPAIGN_SCHEMA: &str = "message campaign_daily {
  required binary campaign_id (UTF8);
  required binary channel (UTF8);
  required int32 day (DATE);
  required int64 sessions;
  required int64 purchasing_sessions;
  required int64 revenue_cents;
  required int64 assisted_sessions;
}";

pub enum Column {
    Bytes(Vec<ByteArray>),
    OptBytes(Vec<ByteArray>, Vec<i16>),
    I32(Vec<i32>),
    I64(Vec<i64>),
}

fn properties() -> Arc<WriterProperties> {
    Arc::new(
        WriterProperties::builder()
            .set_writer_version(WriterVersion::PARQUET_1_0)
            .set_compression(Compression::UNCOMPRESSED)
            .set_dictionary_enabled(false)
            .set_statistics_enabled(EnabledStatistics::None)
            .set_max_row_group_row_count(Some(1 << 24))
            .build(),
    )
}

/// Writes one file with one row group (none when there are no rows).
pub fn write(path: &Path, schema: &str, columns: Vec<Column>, rows: usize) {
    let schema = Arc::new(parse_message_type(schema).expect("schema"));
    let file = BufWriter::with_capacity(1 << 22, File::create(path).expect("create parquet"));
    let mut writer = SerializedFileWriter::new(file, schema, properties()).expect("writer");
    if rows > 0 {
        let mut group = writer.next_row_group().expect("row group");
        let mut columns = columns.into_iter();
        while let Some(mut col) = group.next_column().expect("column") {
            match columns.next().expect("column count") {
                Column::Bytes(v) => {
                    col.typed::<ByteArrayType>().write_batch(&v, None, None).expect("write");
                }
                Column::OptBytes(v, def) => {
                    col.typed::<ByteArrayType>().write_batch(&v, Some(&def), None).expect("write");
                }
                Column::I32(v) => {
                    col.typed::<Int32Type>().write_batch(&v, None, None).expect("write");
                }
                Column::I64(v) => {
                    col.typed::<Int64Type>().write_batch(&v, None, None).expect("write");
                }
            }
            col.close().expect("close column");
        }
        group.close().expect("close row group");
    }
    writer.close().expect("close parquet");
}
