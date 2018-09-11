initSidebarItems({"mod":[["helper",""],["pmap",""],["range",""]],"struct":[["DefaultSort","Marker struct for sorting types that implement `Ord` in their 'natural' order."],["MergeIterator","Iterator over merged shardio files"],["ShardItemIter","Iterator of items from a single shardio reader"],["ShardReader","Read from a collection of shardio files. The input data is merged to give a single sorted view of the combined dataset. The input files must be created with the same sort order `S` as they are read with."],["ShardSender","A handle that is used to send data to a `ShardWriter`. Each thread that is producing data needs it's own ShardSender. A `ShardSender` can be obtained with the `get_sender` method of `ShardWriter`.  ShardSender implement clone."],["ShardWriter","Write a stream data items of type `T` to disk."]],"trait":[["SortKey","Specify a key function from data items of type `T` to a sort key of type `Key`. Impelment this trait to create a custom sort order. The function `sort_key` returns a `Cow` so that we abstract over Owned or Borrowed data."]]});