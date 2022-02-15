# Providing Data to a network indexer

There are two parts to the ingestion / providing protocol used by store the index.

1. Advertisments maintains an immutable authenticated data structure where providers describe what content they are have available.
2. Announcements are a transient notification that the content on a provider has changed.

## Advertisements

### Advertisement data structures + construction

An individual advertisement is an [IPLD](https://ipld.io/docs/data-model/) object with the following [schema](https://github.com/filecoin-project/storetheindex/blob/main/api/v0/ingest/schema/schema.ipldsch):
```
type Advertisement struct {
	# Previous advertisement.
	PreviousID optional Link_Advertisement
	# Provider ID of the advertisement.
	Provider String
	# Addresses, as list of multiaddr strings, to use for content retrieval.
	Addresses List_String
	# Advertisement signature.
	Signature Bytes
	# Entries with a link to the list of CIDs
	Entries Link
	# Context ID for entries.
	ContextID Bytes
	# Serialized v0.Metadata for all entries in advertisement.
	Metadata Bytes
	# IsRm or Put?
	IsRm Bool
}
```

* The `PreviousID` is the CID of the previous advertisement, and is empty for the 'genesis'.
* The Provider is the `peer.ID` of the libp2p host providing the content.
* The Addresses are the multiaddrs to provide to clients in order to connect to the provider.
* Entries is CID to the list of Multihashes this advertisement is indicating are available.
* ContextID is an identifier you may use to subsequently update a an advertisement. It has the following semantics:
  * If a ContextID is used with different entries, those entries will be _added_ to the association with that ContextID
  * If a ContextID is used with different provider, addresses, or metadata, all previous CIDs advertised under that ContextID will have their provider, addresses, and metadata updated to the most recent.
  * If a ContextID is used with the `IsRM` flag set, all previous CIDs advertised under that ContextID will be removed.
* Metadata represents additional opaque data that will be forwarded to client queries for any of the CIDs in this advertisement. It is expected to start with a `varint` indicating the remaining format of metadata. Store the index operators mauy limit the length of this field, and it is recommended to keep it below 100 bytes.

#### Entries data structure

Entries is defined as the following schema:
```
type EntryChunk struct {
	Entries List_Bytes
	Next optional Link_EntryChunk
}
```

The primary `Entries` list is the array of multihashes in the advertisement.
If an advertisement has more CIDs than fit into a single block for purposes of data transfer, they may be split into multiple chunks, conceptually a linked list, by using `Next` as a reference ot the next chunk.

In terms of concrete constriants, each EntryChunbk shouldstay below 4MB,
and a linked list of entry chunks should be no more than 400 chunks long. Above these constraints, the list of entries should be split into multiple advertisements. Practically, this means that each individidual advertisement can hold up to approximately 40 million multihashes.

#### Metadata

The reference provider currently supports Bitswap and Filecoin protocols. The structure of the metadata format for these protocols is defined in [the library](https://github.com/filecoin-project/index-provider/tree/main/metadata).

The network indexer nodes expect that metadata begins with a Uvar identifying the protocol, followed by protocol-specific metadata.

* Bitswap
  * uvarint protocol `0x3E0000`.
  * no following metadata.
* filecoin graphsync
  * uvarint protcol `0x3F0000`.
  * the following bytes should be a cbor encoded struct of:
    * PieceCID, a link
	* VerifiedDeal, boolean
    * FastRetrieval, boolean
* http
  * the proposed uvarint protocol is `0x3D0000`.
  * the following bytes are not yet defined.

### Advertisement transfer

There are two ways that the provider advertisement chain can be made available for consumption by network indexers.

1. As a [graphsync](https://github.com/ipfs/go-graphsync) endpoint on a libp2p host.
2. As a set of files fetched over HTTP.

There are two parts to the transfer protocol. The providing of the advertisement chain itself, and a 'head' protocol for indexers to query the provider on what it's most recent advertisement is.

#### Libp2p

On libp2p hosts, graphsync is used for providing the advertisement chain.

* Graphsync is configured on the common graphsync multiprotocol of the libp2p host.
* Requests for index advertisements can be identified by
    * The use of a ['Legs'](https://github.com/filecoin-project/go-legs/blob/main/dtsync/voucher.go#L17-L24) voucher in the request.
    * A CID of either the most recent advertisement, or a a specific Entries pointer.
    * A selector either for the advertisement chain, or for an entries list.

A reference implementation of the core graphsync provider is available in the [go-legs](https://github.com/filecoin-project/go-legs) repository, and it's integration into a full provider is available in [index-provider](https://github.com/filecoin-project/index-provider).

On these hosts, a custom `head` multiprotocol is exposed on the libp2p host as a way of learning the most recent current advertisement.
The multiprotocol is named [`/legs/head/<network-identifier>/<version>`](https://github.com/filecoin-project/go-legs/blob/main/p2p/protocol/head/head.go#L40). The protocol itself is implemented as an HTTP TCP stream, where a request is made for the `/head` resource, and the response body contains the string representation of the root CID.

#### HTTP

The IPLD objects of advertisements and entries are represented as files named as their CIDs in an HTTP directory. These files are immutible, so can be safely cached or stored on CDNs.

The head protocol is the same as above, but not wrapped in a libp2p multiprotocol.
A client wanting to know the latest advertisement CID will ask for the file named `head` in the same directory as the advertisements/entries, and will expect back a [signed response](https://github.com/filecoin-project/go-legs/blob/de87e8542506a86af3fef2026e9eb9b954251b8b/httpsync/message.go#L60-L64) for the current head.

## Announcements

Indexers may be notified of changes to advertisements as a way to reduce the latency of ingestion, and for discovery / registration of new providers.
Once indexers observe a new provider, they should adaptively poll the provider for new content, which provides the basis of understanding what content is currently available.

The indexer will maintain a policy for when advertisements from a provider are considered valid. An example policy may be
* A provider must be available for at least 2 days before its advertisements will be returned to clients.
* If a provider cannot be dialed for 3 days, it's advertisements will no longer be returned to clients.
* If a provider starts a new chain, previous advertisements now no longer referenced will not be returned after 1 day of not being referenced.
* If a provider cannot be dialed for 2 weeks, previous advertisements downloaded by the indexer will be garbage collected, and will need to be re-synced from the provider.

There are two ways that a provider may pro-actively alert indexer(s) of new content availability:

1. Gossipsub announcements
2. HTTP announcements

### Gossipsub

The announcment contains the CID of the head, and the multiaddr (either the libp2p host or the HTTP host) where it should be fetched from. The format is [here](https://github.com/filecoin-project/go-legs/blob/main/dtsync/message.go#L15).

It is sent over a gossip sub topic, that defaults to `/indexer/ingest/<network>`. For our production network, this is `/indexer/ingest/mainnet`.


The legs provider will generate gossip announcements automatically on it's host.

### HTTP

alternatively, an announcement can be sent to a specific known network indexer.
The network indexer may then relay that announcement over gossip sub to other indexers to allow broader discover of a provider chosing to selectively announce in this way.

Announcements are sent as HTTP PUT requests to [`/ingest/announce`](https://github.com/filecoin-project/storetheindex/blob/main/server/ingest/http/server.go#L50) on the index node's 'ingest' server.
Note that the ingest server is not the same http server as the primary publicly exposed query server. This is because the index node operator may choose not to expose it, or may protect it so that only selected providers are given access to this endpoint due to potential denial of service concerns.

The body of the request put to this endpoint should be the json serialization of the announcement [message](https://github.com/filecoin-project/go-legs/blob/main/dtsync/message.go#L15) that would be provided over gossip sub: a representation of the head CID, and the multiaddr of where to fetch the advertisement chain.