use regex::Regex;
use thiserror::Error;

use std::fs::File;
use std::io;
use std::io::{prelude::*, BufReader, SeekFrom};
use std::str::FromStr;

#[derive(Error, Debug)]
pub enum SQLiteError {
    #[error("can't open the database file")]
    CantOpen(#[from] io::Error),
    #[error("{}", .0)]
    SQLiteQueryError(#[from] SQLQueryError),
    #[error("Query parsing error: {}", .0)]
    QueryParsingError(#[from] SQLQueryParsingError),
    #[error("Internal error: {}", .0)]
    InternalError(#[from] SQLiteInternalError),
}

fn main() -> Result<(), SQLiteError> {
    // Parse arguments
    let args = std::env::args().collect::<Vec<_>>();
    match args.len() {
        0 | 1 => panic!("Missing <database path> and <command>"),
        2 => panic!("Missing <command>"),
        _ => {}
    }

    let command = &args[2];
    match command.as_str() {
        ".dbinfo" => {
            let mut file = File::open(&args[1])?;
            let mut db_header = [0; 100];
            file.read_exact(&mut db_header)?;

            // 'The page size for a database file is determined by the 2-byte integer located
            // at an offset of 16 bytes from the beginning of the database file.'
            let page_size = u16::from_be_bytes([db_header[16], db_header[17]]);

            println!("database page size: {page_size}");

            // Next, reading the 'sqlite_schema' table header
            let mut sqlite_schema_table_header = [0; 8];
            file.read_exact(&mut sqlite_schema_table_header)?;

            // 'The two-byte integer at offset 3 gives the number of cells on the page.'
            let nb_tables =
                u16::from_be_bytes([sqlite_schema_table_header[3], sqlite_schema_table_header[4]]);
            println!("number of tables: {nb_tables}");
        }
        ".tables" => {
            let mut file = File::open(&args[1])?;

            // Skipping the database header
            let db_header_size = 100;

            file.seek(SeekFrom::Start(db_header_size))?;

            let table_names: Vec<String> = parse_schema_table(&mut file)?
                .into_iter()
                .map(|row| row.tbl_name)
                .collect();

            let mut output_str = String::new();
            for tbl_name in &table_names[..&table_names.len() - 1] {
                output_str.push_str(tbl_name);
                output_str.push(' ');
            }
            output_str.push_str(&table_names[&table_names.len() - 1]);
            println!("{output_str}");
        }
        sql_query if !sql_query.is_empty() => {
            let sql_query = pseudo_sql_query_parsing(sql_query)?;

            let mut db_file = File::open(&args[1])?;
            handle_sql_query(&sql_query, &mut db_file)?;
        }
        _ => panic!("Missing or invalid command passed: {command}"),
    }

    Ok(())
}

#[derive(Debug)]
enum SQLQuery {
    CountRows(String),       // count rows in a table. The string hold the table name.
    Select(SelectQueryData), // SELECT name FROM apples
}

#[derive(Debug)]
struct SelectQueryData {
    table_name: String,
    column_name: String,
}

#[derive(Debug, Error)]
pub enum SQLQueryParsingError {
    #[error("Only 'SELECT COUNT(*) FROM xxx' is supported, got: {}", .0)]
    BadQuery(String),
}

// NOTE: Hardcoding supported queries for now
const SELECT_COUNT_STAR_FROM: &str = "SELECT COUNT(*) FROM ";
const SELECT_COL_FROM_TABLE_RE: &str = r"^SELECT (.+) FROM (.+)$";

fn pseudo_sql_query_parsing(sql_query: &str) -> Result<SQLQuery, SQLQueryParsingError> {
    if let Some(table_name) = sql_query.strip_prefix(SELECT_COUNT_STAR_FROM) {
        Ok(SQLQuery::CountRows(table_name.to_string()))
    } else {
        let re_select_col_from_table =
            Regex::new(SELECT_COL_FROM_TABLE_RE).expect("creating new regex should not fail");
        if let Some(caps) = re_select_col_from_table.captures(sql_query) {
            dbg!(&caps);
            match caps.len() {
                3 => Ok(SQLQuery::Select(SelectQueryData {
                    column_name: caps.get(1).map_or("", |m| m.as_str()).to_string(),
                    table_name: caps.get(2).map_or("", |m| m.as_str()).to_string(),
                })),
                _ => Err(SQLQueryParsingError::BadQuery(sql_query.to_string())),
            }
        } else {
            Err(SQLQueryParsingError::BadQuery(sql_query.to_string()))
        }
    }
}

#[derive(Debug, Error)]
pub enum SQLQueryError {
    #[error("Invalid SQL query: {}", .0)]
    InvalidSQL(String),
    #[error("SQL query not implemented yet: {}", .0)]
    NotImplementedYet(String),
    #[error("Internal error: {}", .0)]
    InternalError(#[from] SQLiteInternalError),
}

fn handle_sql_query(
    sql_query: &SQLQuery,
    db: &mut (impl Read + Seek),
) -> Result<(), SQLQueryError> {
    match sql_query {
        SQLQuery::CountRows(target_tbl_name) => {
            // Skipping the database header
            let db_header_size = 100;

            db.seek(SeekFrom::Start(db_header_size))
                .map_err(SQLiteInternalError::SeekError)?;

            let table_rows = parse_schema_table(db)?;

            let target_table_row = table_rows
                .iter()
                .find(|&r| r.tbl_name == *target_tbl_name)
                .unwrap_or_else(|| panic!("Could not find table with name '{target_tbl_name}'"));

            // Get the database page size
            // This info is in the database header, at offset [16, 18]
            db.seek(SeekFrom::Start(16))
                .map_err(SQLiteInternalError::SeekError)?;
            let mut page_size_be_bytes = [0; 2];
            db.read_exact(&mut page_size_be_bytes)
                .map_err(SQLiteInternalError::ReadError)?;
            let page_size = u16::from_be_bytes(page_size_be_bytes);

            // Get to correct page in the db
            let table_page_offset = page_size * (target_table_row.root_page - 1) as u16;
            db.seek(SeekFrom::Start(table_page_offset as u64))
                .map_err(SQLiteInternalError::SeekError)?;

            // Read the page header
            let mut table_header_bytes = [0; 8];
            db.read_exact(&mut table_header_bytes)
                .map_err(SQLiteInternalError::ReadError)?;

            // Extract the number of cells ~ the number of rows
            let nb_cells = u16::from_be_bytes([table_header_bytes[3], table_header_bytes[4]]);

            println!("{nb_cells}");
        }
        SQLQuery::Select(SelectQueryData {
            table_name,
            column_name,
        }) => {
            println!("SELECT COL FROM TABLE");
            dbg!(table_name);
            dbg!(column_name);
        }
    }
    Ok(())
}

#[derive(Debug, Error)]
pub enum SQLiteInternalError {
    #[error("Could not seek db file from start to offset: {:?}", .0)]
    SeekError(io::Error),
    #[error("Db file read error: {:?}", .0)]
    ReadError(io::Error),
    #[error("Failed to convert parsed varint to u64")]
    VarIntConversionFail,
    #[error("Invalid UTF-8: {:?}", .0)]
    InvalidUTF8(#[from] std::string::FromUtf8Error),
    #[error("Found bad object type: {}", .0)]
    FoundBadObjectType(String),
    #[error("{}", .0)]
    SerialTypeError(#[from] SerialTypeError),
}

/// Varint:
/// A variable-length integer or "varint" is a static Huffman encoding of 64-bit twos-complement integers that uses less space for small positive values. A varint is between 1 and 9 bytes in length. The varint consists of either zero or more bytes which have the high-order bit set followed by a single byte with the high-order bit clear, or nine bytes, whichever is shorter. The lower seven bits of each of the first eight bytes and all 8 bits of the ninth byte are used to reconstruct the 64-bit twos-complement integer. Varints are big-endian: bits taken from the earlier byte of the varint are more significant than bits taken from the later bytes.
///
/// "The cell pointer array of a b-tree page immediately follows the b-tree page header. Let K be the number of cells on the btree. The cell pointer array consists of K 2-byte integer offsets to the cell contents."
/// And codecrafters add: "The offsets are relative to the start of the page".
fn get_cell_ptr_array(
    header: [u8; 8],
    b_tree_page_content: &mut (impl Read + Seek),
) -> Result<Vec<u16>, SQLiteInternalError> {
    let nb_cells: u16 = u16::from_be_bytes([header[3], header[4]]);

    let mut offsets_array_buff: Vec<u8> = vec![0; (2 * nb_cells).into()];
    b_tree_page_content
        .read_exact(&mut offsets_array_buff)
        .map_err(SQLiteInternalError::ReadError)?;

    let offsets_array: Vec<u16> = offsets_array_buff
        .chunks_exact(2)
        .map(|chunk| {
            let cell_offset_bytes: [u8; 2] = chunk
                .try_into()
                .expect("expect cell offsets array to have an even number of bytes");
            u16::from_be_bytes(cell_offset_bytes)
        })
        .collect();
    Ok(offsets_array)
}

#[derive(Debug)]
enum ObjectType {
    Table,
    Index,
    View,
    Trigger,
}

impl FromStr for ObjectType {
    type Err = String;

    fn from_str(input: &str) -> Result<ObjectType, Self::Err> {
        match input {
            "table" => Ok(ObjectType::Table),
            "index" => Ok(ObjectType::Index),
            "view" => Ok(ObjectType::View),
            "trigger" => Ok(ObjectType::Trigger),
            _ => Err("Invalid object type: {input}".to_string()),
        }
    }
}

/// https://www.sqlite.org/schematab.html
#[derive(Debug)]
struct SchemaTableRow {
    _object_type: ObjectType,
    _name: String,
    tbl_name: String,
    root_page: u8,
    _sql: String,
}
/// Parse the 'sql_schema' table.
/// See the 'sql schema table' doc: https://www.sqlite.org/schematab.html
fn parse_schema_table(
    db: &mut (impl Read + Seek),
) -> Result<Vec<SchemaTableRow>, SQLiteInternalError> {
    // Reading the 'sqlite_schema' table

    // Reading its header
    // 'The two-byte integer at offset 3 gives the number of cells on the page.'
    let mut sqlite_schema_table_header = [0; 8];
    db.read_exact(&mut sqlite_schema_table_header)
        .map_err(SQLiteInternalError::ReadError)?;

    let cell_ptr_array = get_cell_ptr_array(sqlite_schema_table_header, db)?;

    // NOTE: at this point, we are 2*nb_cells bytes deep after the page header

    let page_offset = 0;
    let mut sql_schema_rows = Vec::new();
    for cell_offset in cell_ptr_array {
        let row = parse_sql_schema_table_cell(page_offset, cell_offset, db)?;
        sql_schema_rows.push(row);
    }

    Ok(sql_schema_rows)
}

/// Parse one cell in the 'sql_schema' table, at a given cell offset.
///
/// See the 'sql schema table' doc: https://www.sqlite.org/schematab.html
//
/// Cell structure:
/// - cell size (varint): 'the total number of bytes of payload, including any overflow'
/// - rowid (varint)
/// - 'record'
///   Documentation on the varint encoding: https://protobuf.dev/programming-guides/encoding/#varints
fn parse_sql_schema_table_cell(
    page_offset: u16,
    cell_offset: u16,
    db: &mut (impl Read + Seek),
) -> Result<SchemaTableRow, SQLiteInternalError> {
    let mut offset = (page_offset + cell_offset) as u64;

    let (_cell_size, cell_varint_size) = parse_varint(offset, db)?;

    // Next, the rowid
    offset += cell_varint_size as u64;
    let (_rowid, rowid_varint_size) = parse_varint(offset, db)?;

    offset += rowid_varint_size as u64;

    // Reading the record header size (varint)
    let (header_size, header_size_varint) = parse_varint(offset, db)?;

    // Array of the serial types
    let mut columns_serial_types = [0; 5];

    let mut header_read_size = header_size_varint as u64; // we already read the bytes for the header-size varint itself
    offset += header_read_size;
    let mut col_idx = 0;
    while header_read_size < header_size {
        assert!(col_idx < columns_serial_types.len());

        let (serial_type, varint_size) = parse_varint(offset, db)?;

        columns_serial_types[col_idx] = serial_type;

        offset += varint_size as u64;
        header_read_size += varint_size as u64;
        col_idx += 1;
    }

    // Map the serial types to the byte length they encode
    let columns_byte_lengths = columns_serial_types
        .iter()
        .map(|&s| serial_type_2_byte_length(s))
        .collect::<Result<Vec<_>, SerialTypeError>>()?;

    // NOTE: odd -> encode text (https://sqlite.org/fileformat2.html#record_format)
    // Will check this before trying to parse bytes as utf-8

    // Reading the record body:
    db.seek(SeekFrom::Start(offset))
        .map_err(SQLiteInternalError::SeekError)?;

    // 1st column: 'type'
    let mut obj_type_bytes = vec![0; columns_byte_lengths[0] as usize];
    db.read_exact(&mut obj_type_bytes)
        .map_err(SQLiteInternalError::ReadError)?;

    assert!(columns_serial_types[0].rem_euclid(2) == 1);
    let object_type = ObjectType::from_str(&String::from_utf8(obj_type_bytes)?)
        .map_err(SQLiteInternalError::FoundBadObjectType)?;

    // 2nd column: 'name'
    let mut name_bytes = vec![0; columns_byte_lengths[1] as usize];
    db.read_exact(&mut name_bytes)
        .map_err(SQLiteInternalError::ReadError)?;
    assert!(columns_serial_types[1].rem_euclid(2) == 1);
    let name = String::from_utf8(name_bytes)?;

    // 3rd column: 'tbl_name'
    let mut tbl_name_bytes = vec![0; columns_byte_lengths[2] as usize];
    db.read_exact(&mut tbl_name_bytes)
        .map_err(SQLiteInternalError::ReadError)?;

    assert!(columns_serial_types[2].rem_euclid(2) == 1);
    let tbl_name = String::from_utf8(tbl_name_bytes)?;

    // 4th column: 'rootpage'
    let mut rootpage_bytes = vec![0; columns_byte_lengths[3] as usize];
    assert!(columns_byte_lengths[3] == 1); // TODO: add support for serial types 0..9.
                                           // Currently, assuming the serial type is 1,
                                           // i.e., that the root page is a u8 (1 byte
                                           // integer)

    db.read_exact(&mut rootpage_bytes)
        .map_err(SQLiteInternalError::ReadError)?;
    let be_bytes: [u8; 1] = rootpage_bytes[..1]
        .try_into()
        .expect("slice should have 8 bytes");
    let root_page = u8::from_be_bytes(be_bytes);

    // NOTE: the `sql` column corresponds to CREATE statements
    // This is what we want to parse to extract the current table , e.g. 'apples', describe in this
    // cell of the sql_schema table
    //
    let mut sql_bytes = vec![0; columns_byte_lengths[4] as usize];
    db.read_exact(&mut sql_bytes)
        .map_err(SQLiteInternalError::ReadError)?;

    assert!(columns_serial_types[4].rem_euclid(2) == 1);
    let sql = String::from_utf8(sql_bytes)?;

    Ok(SchemaTableRow {
        _object_type: object_type,
        _name: name,
        tbl_name,
        root_page,
        _sql: sql,
    })
}

/// Reads the varint[1] using the Reader starting from the given offset.
/// Uses a combination of bit-shifts, comparaisons and cast to from u8 to u64 to decode the varint.
/// Will used buffer reads to read 1 byte at a time from the varint.
///
/// Returns:
/// - the decoded varint as a u64
//  - the size in bytes of this decoded varint
//
// [1]: Protobuf documentation on varint encoding: https://protobuf.dev/programming-guides/encoding/#varints
fn parse_varint(
    offset: u64,
    reader: &mut (impl Read + Seek),
) -> Result<(u64, usize), SQLiteInternalError> {
    reader
        .seek(SeekFrom::Start(offset))
        .map_err(SQLiteInternalError::SeekError)?;
    let mut buf_reader = BufReader::new(reader);

    // Parsing the varint
    // Going byte by byte, checking the MSB for continuation
    let mut varint_total: u64 = 0; // each byte contribution will be accumulated here

    let mut varint_byte_idx: usize = 0; // idx into the varint bytes

    // The Most Significant Byte (MSB) tells us about continuation:
    // - 0 ~ false ~ end of the varint
    // - 1 ~ true ~ varint continues onto the next byte
    let mut msb: bool = true;

    let mut varint_byte = [0; 1]; // current varint byte will be read into this buffer
    while msb {
        // NOTE: important assumption about the varint encoding.
        // -> At most 9 bytes should be read.
        assert!(varint_byte_idx < 9);

        buf_reader
            .read_exact(&mut varint_byte)
            .map_err(SQLiteInternalError::ReadError)?;

        // update MSB
        msb = varint_byte[0] > 128; // 128 = 0x80


        let contrib = u64::from(varint_byte[0]); // current byte contribution
        varint_total = (varint_total << 7) + contrib;
        if !msb {
            return Ok((varint_total, varint_byte_idx + 1)); // MSB indicates this is the end of the
                                                            // varint -> early return
        }

        varint_total -= 0x80; // 'dropping' the MSB (bit = 1) from the current byte
                              // contribution we just  added
        varint_byte_idx += 1;
    }

    Ok((varint_total, varint_byte_idx))
}

#[derive(Debug, Error)]
pub enum SerialTypeError {
    #[error("Could not convert serial type: {:?}", .0)]
    BadSerialNumber(u64),
}
fn serial_type_2_byte_length(serial_type: u64) -> Result<u64, SerialTypeError> {
    match serial_type {
        0..5 => Ok(serial_type),
        5 => Ok(6),
        6 | 7 => Ok(8),
        8 | 9 => Ok(0),
        n if n >= 12 && n.rem_euclid(2) == 0 => Ok((n - 12) / 2),
        n if n >= 13 && n.rem_euclid(2) == 1 => Ok((n - 13) / 2),
        _ => Err(SerialTypeError::BadSerialNumber(serial_type)),
    }
}

// Hex notes
//
// ec0 -> 14*(16*16) + 12*16 + 0 = 3584 + 192 + 0 = 3776
