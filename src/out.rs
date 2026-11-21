//! Parquet output, written directly: one row group per file, one uncompressed data page (format
//! v1, PLAIN values) per column, definition levels as one bit-packed run, and the footer encoded
//! by hand in Thrift's compact protocol. The files have the same layout and metadata as the
//! parquet crate wrote before (schema, String/Date logical and converted types, PLAIN + RLE
//! encodings, no dictionary, no statistics; checked field by field with pyarrow), without its
//! per-value encoders and buffers.

/// A column's type as the output tables need it.
#[derive(Clone, Copy, PartialEq)]
pub enum Kind {
    Str,
    Date,
    I64,
}

pub struct Spec {
    pub name: &'static str,
    pub kind: Kind,
    pub optional: bool,
}

const fn spec(name: &'static str, kind: Kind, optional: bool) -> Spec {
    Spec { name, kind, optional }
}

pub const USER: [Spec; 6] = [
    spec("user_id", Kind::Str, false),
    spec("day", Kind::Date, false),
    spec("sessions", Kind::I64, false),
    spec("events", Kind::I64, false),
    spec("revenue_cents", Kind::I64, false),
    spec("top_category", Kind::Str, true),
];

pub const CAMPAIGN: [Spec; 7] = [
    spec("campaign_id", Kind::Str, false),
    spec("channel", Kind::Str, false),
    spec("day", Kind::Date, false),
    spec("sessions", Kind::I64, false),
    spec("purchasing_sessions", Kind::I64, false),
    spec("revenue_cents", Kind::I64, false),
    spec("assisted_sessions", Kind::I64, false),
];

/// One column's page body: the PLAIN values of its present rows, and for an optional column a
/// bitmap of which rows are present (row i is bit i % 8 of byte i / 8).
#[derive(Default)]
pub struct Col {
    values: Vec<u8>,
    present: Vec<u8>,
}

impl Col {
    pub fn with_capacity(bytes: usize) -> Col {
        Col { values: Vec::with_capacity(bytes), present: Vec::new() }
    }

    #[inline(always)]
    pub fn push_i32(&mut self, v: i32) {
        self.values.extend_from_slice(&v.to_le_bytes());
    }

    #[inline(always)]
    pub fn push_i64(&mut self, v: i64) {
        self.values.extend_from_slice(&v.to_le_bytes());
    }

    /// A string: 4-byte little-endian length, then its bytes.
    #[inline(always)]
    pub fn push_str(&mut self, s: &[u8]) {
        self.values.extend_from_slice(&(s.len() as u32).to_le_bytes());
        self.values.extend_from_slice(s);
    }

    /// A string that `fill` appends to the buffer it is given; the length is patched in after.
    #[inline(always)]
    pub fn push_str_with(&mut self, fill: impl FnOnce(&mut Vec<u8>)) {
        let at = self.values.len();
        self.values.extend_from_slice(&[0; 4]);
        fill(&mut self.values);
        let n = (self.values.len() - at - 4) as u32;
        self.values[at..at + 4].copy_from_slice(&n.to_le_bytes());
    }

    /// Whether row `row` of an optional column is present; rows must come in order from 0.
    #[inline(always)]
    pub fn set_present(&mut self, row: usize, present: bool) {
        if row % 8 == 0 {
            self.present.push(0);
        }
        *self.present.last_mut().unwrap() |= (present as u8) << (row % 8);
    }
}

/// Thrift compact protocol, just what the Parquet footer and page headers use.
struct Thrift {
    buf: Vec<u8>,
    last: Vec<i16>, // last field id of each open struct
}

const T_I32: u8 = 5;
const T_I64: u8 = 6;
const T_BINARY: u8 = 8;
const T_LIST: u8 = 9;
const T_STRUCT: u8 = 12;

impl Thrift {
    fn new() -> Thrift {
        Thrift { buf: Vec::with_capacity(1024), last: vec![0] }
    }

    fn varint(&mut self, mut v: u64) {
        while v >= 0x80 {
            self.buf.push((v as u8) | 0x80);
            v >>= 7;
        }
        self.buf.push(v as u8);
    }

    fn field(&mut self, id: i16, ty: u8) {
        let last = self.last.last_mut().unwrap();
        let delta = id - *last;
        if (1..=15).contains(&delta) {
            self.buf.push((delta as u8) << 4 | ty);
        } else {
            self.buf.push(ty);
            let zz = ((id << 1) ^ (id >> 15)) as u16 as u64;
            self.varint(zz);
        }
        *self.last.last_mut().unwrap() = id;
    }

    fn i32(&mut self, id: i16, v: i32) {
        self.field(id, T_I32);
        self.varint(((v << 1) ^ (v >> 31)) as u32 as u64);
    }

    fn i64(&mut self, id: i16, v: i64) {
        self.field(id, T_I64);
        self.varint(((v << 1) ^ (v >> 63)) as u64);
    }

    fn binary(&mut self, id: i16, b: &[u8]) {
        self.field(id, T_BINARY);
        self.varint(b.len() as u64);
        self.buf.extend_from_slice(b);
    }

    fn list(&mut self, id: i16, elem: u8, n: usize) {
        self.field(id, T_LIST);
        if n < 15 {
            self.buf.push((n as u8) << 4 | elem);
        } else {
            self.buf.push(0xF0 | elem);
            self.varint(n as u64);
        }
    }

    /// A struct-typed field: its fields follow, then `end`.
    fn begin(&mut self, id: i16) {
        self.field(id, T_STRUCT);
        self.last.push(0);
    }

    /// A struct that is a list element (no field header).
    fn begin_elem(&mut self) {
        self.last.push(0);
    }

    fn end(&mut self) {
        self.buf.push(0);
        self.last.pop();
    }

    fn list_i32(&mut self, v: i32) {
        self.varint(((v << 1) ^ (v >> 31)) as u32 as u64);
    }

    fn list_binary(&mut self, b: &[u8]) {
        self.varint(b.len() as u64);
        self.buf.extend_from_slice(b);
    }
}

// Parquet enum values (parquet.thrift).
const TYPE_INT32: i32 = 1;
const TYPE_INT64: i32 = 2;
const TYPE_BYTE_ARRAY: i32 = 6;
const REQUIRED: i32 = 0;
const OPTIONAL: i32 = 1;
const CONVERTED_UTF8: i32 = 0;
const CONVERTED_DATE: i32 = 6;
const ENC_PLAIN: i32 = 0;
const ENC_RLE: i32 = 3;
const CODEC_UNCOMPRESSED: i32 = 0;
const PAGE_DATA: i32 = 0;

fn physical(kind: Kind) -> i32 {
    match kind {
        Kind::Str => TYPE_BYTE_ARRAY,
        Kind::Date => TYPE_INT32,
        Kind::I64 => TYPE_INT64,
    }
}

/// Writes one file with one row group (none when there are no rows). `cols[i]` holds the values
/// of `specs[i]` for all `rows` rows.
pub fn write(path: &std::path::Path, root: &str, specs: &[Spec], cols: &[Col], rows: usize) {
    assert_eq!(specs.len(), cols.len());
    let body: usize = cols.iter().map(|c| c.values.len() + c.present.len() + 64).sum();
    let mut f: Vec<u8> = Vec::with_capacity(body + 1024);
    f.extend_from_slice(b"PAR1");
    // (offset of the page header, bytes of header + page)
    let mut chunks: Vec<(usize, usize)> = Vec::with_capacity(cols.len());
    if rows > 0 {
        for (spec, col) in specs.iter().zip(cols) {
            let mut levels = Vec::new();
            if spec.optional {
                // RLE/bit-packed hybrid with bit width 1: one bit-packed run of whole groups of
                // 8 levels (the page header's value count says how many are real).
                let groups = rows.div_ceil(8);
                assert_eq!(col.present.len(), groups, "presence bits for every row");
                let mut h = Thrift::new();
                h.varint(((groups as u64) << 1) | 1);
                levels = h.buf;
                levels.extend_from_slice(&col.present);
            }
            let page = col.values.len() + if spec.optional { 4 + levels.len() } else { 0 };
            let mut h = Thrift::new();
            h.i32(1, PAGE_DATA);
            h.i32(2, page as i32);
            h.i32(3, page as i32);
            h.begin(5); // DataPageHeader
            h.i32(1, rows as i32);
            h.i32(2, ENC_PLAIN);
            h.i32(3, ENC_RLE);
            h.i32(4, ENC_RLE);
            h.end();
            h.buf.push(0); // end of PageHeader
            let at = f.len();
            f.extend_from_slice(&h.buf);
            if spec.optional {
                f.extend_from_slice(&(levels.len() as u32).to_le_bytes());
                f.extend_from_slice(&levels);
            }
            f.extend_from_slice(&col.values);
            chunks.push((at, f.len() - at));
        }
    }

    let mut t = Thrift::new();
    t.i32(1, 1); // version
    t.list(2, T_STRUCT, specs.len() + 1);
    t.begin_elem();
    t.binary(4, root.as_bytes());
    t.i32(5, specs.len() as i32);
    t.end();
    for spec in specs {
        t.begin_elem();
        t.i32(1, physical(spec.kind));
        t.i32(3, if spec.optional { OPTIONAL } else { REQUIRED });
        t.binary(4, spec.name.as_bytes());
        match spec.kind {
            Kind::Str => {
                t.i32(6, CONVERTED_UTF8);
                t.begin(10); // LogicalType
                t.begin(1); // STRING
                t.end();
                t.end();
            }
            Kind::Date => {
                t.i32(6, CONVERTED_DATE);
                t.begin(10); // LogicalType
                t.begin(6); // DATE
                t.end();
                t.end();
            }
            Kind::I64 => {}
        }
        t.end();
    }
    t.i64(3, rows as i64);
    t.list(4, T_STRUCT, chunks.len().min(1));
    if !chunks.is_empty() {
        t.begin_elem(); // RowGroup
        t.list(1, T_STRUCT, chunks.len());
        for (spec, &(at, size)) in specs.iter().zip(&chunks) {
            t.begin_elem(); // ColumnChunk
            t.i64(2, at as i64);
            t.begin(3); // ColumnMetaData
            t.i32(1, physical(spec.kind));
            t.list(2, T_I32, 2);
            t.list_i32(ENC_PLAIN);
            t.list_i32(ENC_RLE);
            t.list(3, T_BINARY, 1);
            t.list_binary(spec.name.as_bytes());
            t.i32(4, CODEC_UNCOMPRESSED);
            t.i64(5, rows as i64);
            t.i64(6, size as i64);
            t.i64(7, size as i64);
            t.i64(9, at as i64);
            t.end();
            t.end();
        }
        let total: usize = chunks.iter().map(|c| c.1).sum();
        t.i64(2, total as i64);
        t.i64(3, rows as i64);
        t.end();
    }
    t.binary(6, b"shopping-sessions pipeline");
    t.buf.push(0); // end of FileMetaData
    f.extend_from_slice(&t.buf);
    f.extend_from_slice(&(t.buf.len() as u32).to_le_bytes());
    f.extend_from_slice(b"PAR1");
    std::fs::write(path, &f).expect("write parquet");
}
