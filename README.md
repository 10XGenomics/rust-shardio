# rust-shardio

Library for handling out-of-memory sorting of large datasets which need to be processed in multiple passes map / sort / reduce passes. 

Items of an arbitrary type T implementing Serialize and Deserialize are buffered, sorted according to a customizable sort key, then serialized in chunks with serde + lz4 and written to disk, while maintaing an index and an index of the position and key range of each chunk. 

See [Docs](https://10xgenomics.github.io/rust-shardio) for API and examples.

Note: Enable the 'full-test' feature in Release mode to turn on some long-running stress tests.