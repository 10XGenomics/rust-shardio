# rust-shardio

[![Coverage Status](https://coveralls.io/repos/github/10XGenomics/rust-shardio/badge.svg)](https://coveralls.io/github/10XGenomics/rust-shardio)

Library for handling out-of-memory sorting of large datasets which need to be processed in multiple passes map / sort / reduce passes. 

You write a stream of items of type `T` implementing `Serialize` and `Deserialize` to a `ShardWriter`. The items are buffered, sorted according to a customizable sort key, then serialized to disk in chunks with serde + lz4 and written to disk, while maintaing an index of the position and key range of each chunk. You use a `ShardReader` to stream through a item in a selected interval of the key space, in sorted order.

See [Docs](https://10xgenomics.github.io/rust-shardio) for API and examples.

Note: Enable the 'full-test' feature in Release mode to turn on some long-running stress tests.
