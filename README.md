# rust-shardio

Library for streaming items to an on-disk store. Items have a customizable bucket key. Items are buffered into chunks, then serialized with serde + lz4 and written to file containing data chunks, and an index of the position and bucket of each chunk. 

Useful for implementing out-of-memory bucket sorting or map-reduce style computations.