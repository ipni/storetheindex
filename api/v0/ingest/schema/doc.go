// Package schema defines the makeup of an advertisement.  An advertisement is
// used to inform an indexer that there are updates to a provider's content.
// The IsRm and Entries fields determine how the indexer handles
// the advertisement. The following table describes this:
//
//  IsRm    Entries    Action
// ------+-----------+------------------------------------
// false | NoEntries | Update metadata
// false | data      | Update metadata and index entries
// true  | NoEntries | Delete content with context ID
// true  | data      | Delete specific multihash indexes *
//
// * Deleting entries still requires a context ID, because a multihash can map
// to multiple contextID/metadata values.
//
// When removing content (IsRm true) the metadata is ignored.
//
// All advertisements update the provider's addresses.  To create an
// advertisement that only updates a provider's address, create an
// advertisement to remove content using a context ID that is not used.
package schema
