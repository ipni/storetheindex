// The assigner service is responsible for assigning content advertisement
// publishers to indexers, and forwarding advertisement announcement messages
// to indexers. This allows the ingestion load to be divided among a pool of
// indexers, with a configurable replication factor.
//
// Indexers that are under control of an assigner must be configured to use an
// assigner. If an assigner-controlled indexer runs out of storage space and
// enters frozen mode, then indexing is handed off to the next available
// indexer in the pool.
//
// An assigner may also be used to forward announcement messages to indexers
// that are not in the assigner's indexer pool or to other assigners. For
// example, in the case of multiple redundant indexers it is not necessary to
// manage index publisher assignments, and forwarding announcements is all that
// is needed.
package main
