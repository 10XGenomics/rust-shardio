# rust-shardio

[![Crates.io Downloads](https://img.shields.io/crates/d/shardio.svg)](https://crates.io/crates/shardio)
[![Crates.io Version](https://img.shields.io/crates/v/shardio.svg)](https://crates.io/crates/shardio)
[![Crates.io License](https://img.shields.io/crates/l/shardio.svg)](https://crates.io/crates/shardio)
[![Build Status](https://travis-ci.org/10XGenomics/rust-shardio.svg?branch=master)](https://travis-ci.org/10XGenomics/rust-shardio)
[![Coverage Status](https://coveralls.io/repos/github/10XGenomics/rust-shardio/badge.svg)](https://coveralls.io/github/10XGenomics/rust-shardio)
[![API Docs](https://img.shields.io/badge/API-Docs-blue.svg)](https://10xgenomics.github.io/rust-shardio)

Library for out-of-memory sorting of large datasets which need to be processed in multiple map / sort / reduce passes. 

You write a stream of items of type `T` implementing `Serialize` and `Deserialize` to a `ShardWriter`. The items are buffered, sorted according to a customizable sort key, then serialized to disk in chunks with serde + lz4, while maintaing an index of the position and key range of each chunk. You use a `ShardReader` to stream through a item in a selected interval of the key space, in sorted order.

See [Docs](https://10xgenomics.github.io/rust-shardio) for API and examples.

Note: Enable the 'full-test' feature in Release mode to turn on some long-running stress tests.
