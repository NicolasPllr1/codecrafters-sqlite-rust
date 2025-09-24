use anyhow::{bail, Result};
use std::fs::File;
use std::io::prelude::*;

fn main() -> Result<()> {
    // Parse arguments
    let args = std::env::args().collect::<Vec<_>>();
    match args.len() {
        0 | 1 => bail!("Missing <database path> and <command>"),
        2 => bail!("Missing <command>"),
        _ => {}
    }

    // Parse command and act accordingly
    let command = &args[2];
    match command.as_str() {
        ".dbinfo" => {
            let mut file = File::open(&args[1])?;
            let mut db_header = [0; 100];
            file.read_exact(&mut db_header)?;

            // The page size is stored at the 16th byte offset, using 2 bytes in big-endian order
            #[allow(unused_variables)]
            let page_size = u16::from_be_bytes([db_header[16], db_header[17]]);

            println!("database page size: {}", page_size);

            // 'The two-byte integer at offset 3 gives the number of cells on the page.'
            // from https://www.sqlite.org/fileformat.html#b_tree_pages,
            // in the ' B-tree Page Header Format' table
            let mut sqlite_schema_table_header = [0; 100];
            file.read_exact(&mut sqlite_schema_table_header)?;
            let nb_pages =
                u16::from_be_bytes([sqlite_schema_table_header[3], sqlite_schema_table_header[4]]);
            println!("number of tables: {}", nb_pages);
        }
        _ => bail!("Missing or invalid command passed: {}", command),
    }

    Ok(())
}
