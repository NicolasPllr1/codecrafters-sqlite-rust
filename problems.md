# Problems

Documenting problems I encounter while implementing this challenge, and the
solutions I end up finding.

Note: starting this at stage 7: 'Read data from a single column'. I will try to
remember past challenges in retrospect though.

## Parsing the 'sql' field of the 'schema' table

- [The schema table](https://www.sqlite.org/schematab.html) doc

For the first stages, I had to parse this 'schema table'. I ran into utf-8
problems reading the 'sql' field but this field was _not_ useful yet. I simply
stoped from parsing it and moved on.

However, I now must parse this 'sql' field to understand the structure of the
tables it describes:

> The sqlite_schema.sql column stores SQL text that describes the object.

I think my problem is related to varint parsing and decoding the bytes as utf-8.

Early on, I noticed that my varint parsing gave me a different number than what
is shown in codecrafters website as an example of decoding this data.

This was for the
[print table names](https://app.codecrafters.io/courses/sqlite/stages/sz4)
stage. All the db's tables are described in the 'schema' table.

As for any table, its content is organized into a header + records. A record is
akin to a row. And in this 'schema' table, one such row ~ one table in the db
(excepted this schema 'meta' table).

The [record format](https://www.sqlite.org/fileformat.html#record_format)

- The 'sql' cell bytes (hex): `81 47`

### 'sql' issue

![hexdump of the sample.db file with the record we're analyzing highlighted](./assets/hexdump_sampledb_record_highlighted.png%7C500)

From codecrafters
[example](https://app.codecrafters.io/courses/sqlite/stages/sz4):
![codecrafters sql record decoding example parsing varint 81 47 to 199](./assets/codecrafters_sql_record_decoding_example.png%7C300)

- 199 in binary is: `1100_0111`

VS my parsing:
![ogs from my mistaken varint parser](./assets/my_wrong_varint_parsing_logs.png%7C300)

With hex `81 47` being in bits: `1000_0001 0100_0111`

Let's follow the
[varint doc](https://protobuf.dev/programming-guides/encoding/#varints) from
protobuf to convert the varint manually:
![protobuf docs varint parsing walkthrough example](protobuf_varint_parsing_walkthrough.png%7C400)

- original inputs: `1000_0001 0100_0111`

- drop continuation bits: `000_0001 100_0111`

- convert to big-endian: <mark style="background: #D2B3FFA6;">not doing as it's
  already the case in the sqlite format ?</mark>

- interpret as 64-bit integer: `1100_0111` = 199

- Note: if we do 'convert to bi-endian': `100_0111 000_0001`

- Which gives us: `0010_0011 1000_0001` = 9089

While this
[online varint converter](https://bluecrewforensics.com/varint-converter/) says
it's 9089:

![online varint converter - 8147 is 9089](./assets/online_varint_converter_8147.png%7C300)

The mistake in my impl.:
![my mistaken varint implementation](./assets/wrong_varint_parsing.png%7C400)

I don't _really_ drop the MSB. I just set it to `0`. In effect this happens:

- original inputs: `1000_0001 0100_0111`
- <mark style="background: #FF5582A6;">set</mark> continuation bits to 0:
  `0000_0001 0100_0111`
- convert to big-endian: <mark style="background: #D2B3FFA6;">not doing as it's
  already the case in the sqlite format ?</mark>
- interpret as 64-bit integer: `0000_0001 0100_0111` = 327
  - and actually flipping to big-endian: `0100_0111 0000_0001` = **18177**

I was lucky that for the first columns, which happened to be the one I needed to
pass early stages, there are only 1 byte-long varint. So both mistakes of
flipping the big-endian and of not really dropping the continuation bits did
have any effect. But on the last column - 'sql' - my varint parsing mistakes
kicked in (and did cancelled-out!) producing an erroneous value.

#### Does this require some `unsafe`

- do I need to work with 'u7' ??
- Is is it even possible to safely concatenate the 'u7's into a larger vec of
  u8, maybe 0 padded at the beggining ?

I think I may not absolutely need unsafe. I can make a `[u8; 8]`, and fill it
iterating over the collected 'varint' bytes. For each varint byte:

- 'dropping' the MSB simply means not allocating it within my final array. I
  would do something like `final_array[idx..idx+7] = varint_byte[1..]` so that
  the the next 7 bits after idx in my final_array get the values from the 7 LSB
  in the varint.

- [tokio's prost implemenation of varint decoding](https://github.com/tokio-rs/prost/blob/25cef930100c10879a98ee2724ee44b94e436135/prost/src/encoding/varint.rs#L71)

  - fully unrolling the loop, as they are max 9 bytes to decode
  - accumulator used, adding up each byte contribution to the varint as its
    decoded using a combination of bit-shit and casting.

Beggining of this fn:

![Tokio's prost varint decoding without unsafe excepted get_unchcked calls](tokio_prost_safe_varint_decoding.png%7C400)
