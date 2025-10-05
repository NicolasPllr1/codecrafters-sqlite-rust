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

            // 'The two-byte integer at offset 5 designates the start of the cell content area.
            // A zero value for this integer is interpreted as 65536.'
            let cell_content_area_start =
                u16::from_be_bytes([sqlite_schema_table_header[5], sqlite_schema_table_header[6]]);
            println!("Cell content area offset: {}", cell_content_area_start);

            let cell_ptr_array = get_cell_ptr_array(&sqlite_schema_table_header, &mut file)?;
            println!("Cells offsets: {:?}", cell_ptr_array);

            // NOTE: at this point, we are 2*nb_cells bytes deep after the page header
            let sqlite_page_offset = 100; // it's right after the database header

            // let table_names = Vec::new();
            for cell_off_set in cell_ptr_array {
                let record = get_record_in_cell(0, cell_off_set, &mut file);
            }
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
    println!("#cells: {nb_cells}");

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
fn get_record_in_cell(page_offset: u16, cell_offset: u16, db: &mut File) -> Result<()> {
    dbg!(cell_offset);

    db.seek(SeekFrom::Start((page_offset + cell_offset) as u64))?;
    let mut reader = BufReader::new(db);

    // Reading first varint: the cell size
    // Going byte by byte, checking the MSB for continuation
    let mut cell_size_varint_bytes = [0; 9]; // max 9 bytes for a varint
    let mut idx: usize = 0; // idx into the varint bytes
    let mut msb: bool = true; // 0 ~ false ~ end of the varint ; 1 ~ true ~ varint continues onto the
                              // next byte
    let mut varint_byte = [0; 1];
    while msb {
        reader.read_exact(&mut varint_byte)?;

        dbg!(varint_byte);
        // update MSB
        msb = ((varint_byte[0] >> 7) & 0b1) == 1;
        dbg!(msb);

        // drop the MSB
        let byte_without_msb = varint_byte[0] & 0b01111111;
        dbg!(byte_without_msb);
        // add this byte to the varint bytes we already read
        cell_size_varint_bytes[idx] = byte_without_msb;
        idx += 1;
    }
    assert!(cell_size_varint_bytes[cell_size_varint_bytes.len() - 1] == 0);
    let cell_size_le_bytes: [u8; 8] = cell_size_varint_bytes[..8]
        .try_into()
        .expect("slice should have 8 bytes");
    let cell_size = u64::from_le_bytes(cell_size_le_bytes);
    dbg!(cell_size);
    println!("---\n");

    // Next, skipping over the rowid as we don't need to parse it for now
    // this will get us to the _record_
    while msb {
        reader.read_exact(&mut varint_byte)?;
        msb = ((varint_byte[0] >> 7) & 0b1) == 1;
    }

    // Now the reader cursor is at the record.
    // The record is composed of a header and a body
    // - header:
    //   - its size
    //   - serial numbers
    // - body:
    //   - values for each column. Immediatly follows the header.
    // For the sql schema table: https://www.sqlite.org/schematab.html
    // rows are: type, name, tbl_name, rootpage and sql

    // Reading the record header size (varint)

    Ok(())
}
fn tbl_name_from_record(record: Vec<u8>) -> Result<String> {
    todo!()
}

// Reads the varint using the Reader starting from the given offset.
// Will used buffer reads to read 1 byte at a time the varint.
//
// Returns:
// - the parsed varint as a u64
// - the size in bytes of the varint encoding
fn parse_varint(offset: u64, reader: &mut impl Read + Seek) -> Result<(u64, usize)> {
    reader.seek(SeekFrom::Start(offset))?;
    let mut buf_reader = BufReader::new(reader);

    let mut varint_bytes = [0; 9]; // max 9 bytes for a varint
    let mut idx: usize = 0; // idx into the varint bytes
    let mut msb: bool = true; // 0 ~ false ~ end of the varint ; 1 ~ true ~ varint continues onto the
                              // next byte
    let mut varint_byte = [0; 1];
    while msb {
        buf_reader.read_exact(&mut varint_byte)?;

        dbg!(varint_byte);
        // update MSB
        msb = ((varint_byte[0] >> 7) & 0b1) == 1;
        dbg!(msb);

        // drop the MSB
        let byte_without_msb = varint_byte[0] & 0b01111111;
        dbg!(byte_without_msb);
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

// Hex notes
//
// ec0 -> 14*(16*16) + 12*16 + 0 = 3584 + 192 + 0 = 3776
