---
tocpdeth: 3
---

# Content addressable hashing summary

redun uses hashing to create deterministic ids for many records in the redun data model. The design of this hashing scheme has several properties:

- Every hash is unique across all records.
  - While a cryptographically secure hash function can be assumed to not produce hash collisions, we can still have collisions on pre-image data if we are not careful. For example, a CallNode could have the same hash as a file that contains a pickle of a CallNode, if the hashing schema is not carefully designed.
  - To achieve this property, every pre-image is prefixed with a record type, such as 'Task', 'CallNode', etc.
  - It is also easy to have pre-image collisions if arguments to a hash are not well escaped (e.g. if tab is used as an argument delimiter, you need to always escape tabs in string arguments). We solve this systematically by defining a consistent way of efficiently serializing structures using bittorrent encoding format (bencode).
- When one parental record refers to a child record, the child record's hash is included in the parent's hash.
  - This forms a [Merkle Tree](https://en.wikipedia.org/wiki/Merkle_tree) that gives unique hashes to nodes in the resulting DAG.
- Hashes are represented by hex strings instead of bytes to aid easy debugging and display.
  - If greater hashing efficiency is desired, hashes could be kept in their binary byte representation.
- Hashes are truncated to 40 hex bytes for easier display.
  - If the reduced hash space is a concern this truncation could be removed.
