use anyhow::{bail, Result};
use std::fs::File;
use std::io::{prelude::*, BufReader, SeekFrom};

fn main() -> Result<()> {
    // Parse arguments
    let args = std::env::args().collect::<Vec<_>>();
    match args.len() {
        0 | 1 => bail!("Missing <database path> and <command>"),
        2 => bail!("Missing <command>"),
        _ => {}
    }

    let command = &args[2];
    match command.as_str() {
        ".dbinfo" => {
            let mut file = File::open(&args[1])?;
            let mut db_header = [0; 100];
            file.read_exact(&mut db_header)?;

            // 'The page size for a database file is determined by the 2-byte integer located at an offset of 16 bytes from the beginning of the database file.'

            #[allow(unused_variables)]
            let page_size = u16::from_be_bytes([db_header[16], db_header[17]]);

            println!("database page size: {}", page_size);
            // Next, reading the 'sqlite_schema' table

            // Reading its header
            // 'The two-byte integer at offset 3 gives the number of cells on the page.'
            let mut sqlite_schema_table_header = [0; 8];
            file.read_exact(&mut sqlite_schema_table_header)?;

            // Extraction info from this  'sqlite_schema' table _header_

            let nb_tables =
                u16::from_be_bytes([sqlite_schema_table_header[3], sqlite_schema_table_header[4]]);
            println!("number of tables: {}", nb_tables);
        }
        ".tables" => {
            let mut file = File::open(&args[1])?;
            let mut db_header = [0; 100];
            file.read_exact(&mut db_header)?;

            // 'The page size for a database file is determined by the 2-byte integer located at an offset of 16 bytes from the beginning of the database file.'

            #[allow(unused_variables)]
            let page_size = u16::from_be_bytes([db_header[16], db_header[17]]);
            // Next, reading the 'sqlite_schema' table

            // Reading its header
            // 'The two-byte integer at offset 3 gives the number of cells on the page.'
            let mut sqlite_schema_table_header = [0; 8];
            file.read_exact(&mut sqlite_schema_table_header)?;

            // Extraction info from this  'sqlite_schema' table _header_

            let nb_tables =
                u16::from_be_bytes([sqlite_schema_table_header[3], sqlite_schema_table_header[4]]);

            // 'The two-byte integer at offset 5 designates the start of the cell content area.
            // A zero value for this integer is interpreted as 65536.'
            let _cell_content_area_start =
                u16::from_be_bytes([sqlite_schema_table_header[5], sqlite_schema_table_header[6]]);

            let cell_ptr_array = get_cell_ptr_array(&sqlite_schema_table_header, &mut file)?;

            // NOTE: at this point, we are 2*nb_cells bytes deep after the page header
            // let sqlite_page_offset = 100; // it's right after the database header

            let page_offset = 0;
            let mut table_names = Vec::new();
            for cell_offset in cell_ptr_array {
                let tbl_name_bytes = get_record_in_cell(page_offset, cell_offset, &mut file)?;
                table_names.push(String::from_utf8(tbl_name_bytes)?);
            }
            let mut output_str = String::new();
            for tbl_name in &table_names[..&table_names.len() - 1] {
                output_str.push_str(&tbl_name);
                output_str.push(' ');
            }
            output_str.push_str(&table_names[&table_names.len() - 1]);
            println!("{output_str}");
        }
        _ => bail!("Missing or invalid command passed: {}", command),
    }

    Ok(())
}

/// Varint:
/// A variable-length integer or "varint" is a static Huffman encoding of 64-bit twos-complement integers that uses less space for small positive values. A varint is between 1 and 9 bytes in length. The varint consists of either zero or more bytes which have the high-order bit set followed by a single byte with the high-order bit clear, or nine bytes, whichever is shorter. The lower seven bits of each of the first eight bytes and all 8 bits of the ninth byte are used to reconstruct the 64-bit twos-complement integer. Varints are big-endian: bits taken from the earlier byte of the varint are more significant than bits taken from the later bytes.

/// "The cell pointer array of a b-tree page immediately follows the b-tree page header. Let K be the number of cells on the btree. The cell pointer array consists of K 2-byte integer offsets to the cell contents."
/// And codecrafters add: "The offsets are relative to the start of the page".
fn get_cell_ptr_array(header: &[u8; 8], b_tree_page_content: &mut File) -> Result<Vec<u16>> {
    let nb_cells: u16 = u16::from_be_bytes([header[3], header[4]]);

    let mut offsets_array_buff: Vec<u8> = vec![0; (2 * nb_cells).into()];
    b_tree_page_content.read_exact(&mut offsets_array_buff)?;

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

// Get the record bytes from the cell data. Cell structure:
// - cell size (varint): 'the total number of bytes of payload, including any overflow'
// - rowid (varint)
// - 'record'
// Documentation on the varint encoding: https://protobuf.dev/programming-guides/encoding/#varints
fn get_record_in_cell(page_offset: u16, cell_offset: u16, db: &mut File) -> Result<Vec<u8>> {
    let mut offset = (page_offset + cell_offset) as u64;

    let (_cell_size, cell_varint_size) = parse_varint(offset, db)?;

    // Next, the rowid
    offset += cell_varint_size as u64;
    let (_rowid, rowid_varint_size) = parse_varint(offset, db)?;

    // Now the record.
    // The record is composed of a header and a body
    // - header:
    //   - its size
    //   - serial numbers
    // - body:
    //   - values for each column. Immediatly follows the header.
    // For the sql schema table: https://www.sqlite.org/schematab.html
    // rows are: type, name, tbl_name, rootpage and sql

    offset += rowid_varint_size as u64;

    // Reading the record header size (varint)
    let (header_size, header_size_varint) = parse_varint(offset, db)?;

    // Array of the serial types
    let mut columns_serial_types = [0; 5];

    let mut header_read_size = header_size_varint as u64; // we already read the bytes for the header-size varint itself
    offset += header_read_size;
    let mut col_idx = 0;
    while header_read_size < header_size as u64 {
        assert!(col_idx < columns_serial_types.len());

        let (serial_type, varint_size) = parse_varint(offset, db)?;

        columns_serial_types[col_idx] = serial_type;

        offset += varint_size as u64;
        header_read_size += varint_size as u64;
        col_idx += 1;
    }

    // Map the serial types to the byte length they encode
    let columns_byte_lengths =
        columns_serial_types.map(|s| serial_type_2_byte_length(s).expect("valid serial type"));

    // Now, reading the record body. We want to read the value for the tbl_name column
    // 'tbl_name' is the 3rd column

    // The offset to the 3rd column value: after the first 2 column values
    offset += columns_byte_lengths[0] + columns_byte_lengths[1]; // skipping over the first two

    // Reading the 3rd column (tbl_name) value
    // NOTE: 3rd column -> table_name column.
    // See the 'sql schema table' doc: https://www.sqlite.org/schematab.html
    let mut tbl_name_value = Vec::new();
    tbl_name_value.resize(columns_byte_lengths[2] as usize, 0);
    //columns_byte_lengths[2] as usize);

    // let mut tbl_name_value = [0; 7];

    db.seek(SeekFrom::Start(offset))?;

    db.read_exact(&mut tbl_name_value)?;

    // let mut nb_reads = 0;
    // while tbl_name_value.len() < columns_byte_lengths[2] as usize {
    //     db.read_exact(&mut tbl_name_value)?;
    //     nb_reads += 1;
    //     if nb_reads == 10 {
    //         bail!("Too many reads to get the table name value")
    //     }
    // }

    Ok(tbl_name_value)
}

// fn tbl_name_from_record(record: Vec<u8>) -> Result<String> {
//     todo!()
// }

// Reads the varint using the Reader starting from the given offset.
// Will used buffer reads to read 1 byte at a time the varint.
//
// Returns:
// - the parsed varint as a u64
// - the size in bytes of the varint encoding
fn parse_varint(offset: u64, reader: &mut (impl Read + Seek)) -> Result<(u64, usize)> {
    reader.seek(SeekFrom::Start(offset))?;
    let mut buf_reader = BufReader::new(reader);

    // Parsing the varint
    // Going byte by byte, checking the MSB for continuation

    let mut varint_bytes = [0; 9]; // max 9 bytes for a varint
    let mut idx: usize = 0; // idx into the varint bytes
    let mut msb: bool = true; // 0 ~ false ~ end of the varint ; 1 ~ true ~ varint continues onto the
                              // next byte
    let mut varint_byte = [0; 1];
    while msb {
        buf_reader.read_exact(&mut varint_byte)?;

        // update MSB
        msb = ((varint_byte[0] >> 7) & 0b1) == 1;

        // drop the MSB
        let byte_without_msb = varint_byte[0] & 0b01111111;
        // add this byte to the varint bytes we already read
        varint_bytes[idx] = byte_without_msb;
        idx += 1;
    }
    assert!(varint_bytes[varint_bytes.len() - 1] == 0);
    let le_bytes: [u8; 8] = varint_bytes[..8]
        .try_into()
        .expect("slice should have 8 bytes");
    let parsed_varint = u64::from_le_bytes(le_bytes);

    return Ok((parsed_varint, idx));
}

fn serial_type_2_byte_length(serial_type: u64) -> Result<u64> {
    match serial_type {
        0..5 => Ok(serial_type),
        5 => Ok(6),
        6 | 7 => Ok(8),
        8 | 9 => Ok(0),
        n if n >= 12 && n.rem_euclid(2) == 0 => Ok((n - 12) / 2),
        n if n >= 13 && n.rem_euclid(2) == 1 => Ok((n - 13) / 2),
        _ => bail!("Bad serial type: {}", serial_type),
    }
}

// Hex notes
//
// ec0 -> 14*(16*16) + 12*16 + 0 = 3584 + 192 + 0 = 3776
