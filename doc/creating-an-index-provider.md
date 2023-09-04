# Creating an Index Provider

## An Overview of the Indexer system

There are a couple players here. I'll use specific examples to make it clearer,
but note that some of these can be generalized.

1. Filecoin Storage Provider – Hosts data for folks and proves it via the
Filecoin Network Chain. Aka Storage Provider.
2. Indexer (aka [storetheindex](https://github.com/ipni/storetheindex)) – A service that can answer the question:
"Given this CID, who has a copy?". This is little more than a lookup table.
3. Index Provider – A service that runs alongside a Storage Provider and tells
the Indexer what content this storage provider has.

The Index Provider serves as the interface between the storage provider and the
indexer. It can be used from within `Lotus` so that the publishing of new data
happens automatically. But it can also happen separately.

The Index Provider serves advertisements [Advertisement](https://github.com/ipni/go-libipni/blob/main/ingest/schema/schema.ipldsch) for the indexer to fetch using a "sync" operation.
Each advertisement references the previous advertisement so that they forms an advertisement chain. The state of the indexer is basically a function
of consuming this chain from the initial Advertisement to the latest one.

## HTTP Index Provider

An HTTP Index provider needs to provide two endpoints:

1. `GET /ipni/v1/ad/head`
    - This returns information about the latest advertisement that the index provider knows about.
2. `GET /ipni/v1/ad/<cid>`
    - This returns the content for a block identified by the given cid.

When using go-libipni, the `/ipni/v1/ad/` part of the URL path is implicit. It is automatically added by ipnisync clients, and expected by ipnisync publishers.

### On Syncing

If an indexer knows about an index provider, it will occasionally poll the provider to check if there is new content to index. To let the indexer know that there is a new change to content, the provider sends an announcement message to the `announce/` endpoint on the indexer, or boradcasts the announcement over gossib pubsub.

## libp2p Index Provider

In Go, it’s simplest to use [dagsync](https://github.com/ipni/storetheindex/blob/main/dagsync) to perform IPNI communications between providers and indexers.

For an index-provider you’ll want to setup a `dagsync` Publisher:

```go
pub, err := ipnisync.NewPublisher(linksys, publisherPrivKey, ipnisync.WithStreamHost(publisherHost), ipnisync.WithHeadTopic(topicName))
```

This Publisher can serve advertisements using ipnisync over HTTP, libp2p, or both, depending on the options passed.

When a new Advertisement is available the provider calls:

```go
// Tell the publisher what the latest advertisement it.
err = pub.UpdateRoot(ctx, newAdCid)
```

This will allow indexers to retrieve the latest advertisement. Next, broadcast an announcement to all indexers on a libp2p pubsub network or send an announcement directly to specific indexers via HTTP.
```go
// Create a p2p broadcast sender.
p2pSender, err := p2psender.New(pulisherbHost, defaultTestIngestConfig.PubSubTopic)

// Create an HTTP direct sender.
httpSender, err := httpsender.New([]*url.URL{recipientURL}, publisherHost.ID())

// Announce that a new advertisement is available.
err = announce.Send(ctx, newAdCid, pub.Addrs(), p2pSender, httpSender)
```

The dagsync publisher will also handle HTTP and libp2p requests from indexers.

## Testing your index Provider

After you finish building your index provider, you probably want to test it. You can run the indexer locally and tell it about your index provider. That should cause the indexer to sync from your index provider. Then you can query the indexer to see if it has some content that your index provider provided.

1. Make sure your index provider has actual content it is providing. You can do this by checking the `/head` endpoint if implementing an HTTP provider or by using [https://pkg.go.dev/github.com/ipni/go-libipni/dagsync/ipnisync#Syncer.GetHead](https://pkg.go.dev/github.com/ipni/go-libipni/dagsync/ipnisync#Syncer.GetHead) with dagsync.
    - HTTP:
        
        ```bash
        ❯ curl http://localhost:3105/ipni/v1/ad/head
        {"head":{"/":"baguqeerar6uhbs23pfhmtgspgbxk6k6vwu5ccvvudw22ikzh63beqwzuemyq"},"pubkey":{"/":{"bytes":"CAESINT5QWl1KSCFOVmk7hCT4qBit/oxGw8xcQza5EF+cSk4"}},"sig":{"/":{"bytes":"yODvBoXCpPR+xgNERUv18iqzsRCUO5Rj5axl2pVTUW6x7lxYRnypzi+/tfla3Y5qKjQ8hd9rZyCZAh3BpWedCg"}},"topic":"/indexer/ingest/testnet"}
        ```
        
2. Make sure you can also fetch that block. For an HTTP provider you should get the block back when hitting `/<cid>`. For a libp2p provider you should be able to call [https://pkg.go.dev/github.com/ipni/go-libipni/dagsync/ipnisync#Syncer.Sync](https://pkg.go.dev/github.com/ipni/go-libipni/dagsync/ipnisync#Syncer.Sync). 
    - HTTP: (using [dagconv](https://github.com/marcopolo/dagconv) to convert the dagcbor into readable dagjson)
        
        ```bash
        ❯ curl http://localhost:8070/bafy2bzaceaceibjf5pottpm4ghfnu7jo7sjcqjom4vhzm5jmq7domxun5vor4 | dagconv
          % Total    % Received % Xferd  Average Speed   Time    Time     Time  Current
                                         Dload  Upload   Total   Spent    Left  Speed
        100   387  100   387    0     0  16150      0 --:--:-- --:--:-- --:--:-- 24187
        {"Addresses":["/ip4/1.1.1.1/tcp/1234"],"ContextID":{"/":{"bytes":"Li90ZXN0ZGF0YS9zYW1wbGUtdjEtMi5jYXI"}},"Entries":{"/":"bafy2bzacedqhptd3zimn4s343zeo7kh3db5u6qhhkmj4c4cj3bb6qmx4hpoaa"},"IsRm":false,"Metadata":{"/":{"bytes":"kIDAAQ"}},"Provider":"12D3KooWGVwcVphAgpXjJWHoWyKZUrZsXyc34jeuUmG5nSRZyuQq","Signature":{"/":{"bytes":"CiQIARIgY0mWoKX808qfcUJY21aZYKPMnupvsKUllAmDe0cMOTYSGy9pbmRleGVyL2luZ2VzdC9hZFNpZ25hdHVyZRoiEiDz/sR3sFRE00i6BiMdR44x+gVoCZ2bNO0M4D/ij9zhLSpAnXO8PlnJf8OIVM5MVnn0GJezOge72+r09Tju5eXxFvA/isXwRc1OLdPsX6CFtRrMi1hufja56tJv6Zib8TghAQ"}}}
        ```
        
3. Now you can see if the indexer can fetch from the provider.
    1. Run the indexer in the background with
        
        ```bash
        storetheindex daemon
        ```
        
    2. Sync the provider with your local indexer using the indexer’s cli. Here’s an example, replace the peer id and addr with your provider’s value.
        
        ```bash
        storetheindex admin sync -p 12D3KooWGVwcVphAgpXjJWHoWyKZUrZsXyc34jeuUmG5nSRZyuQq --addr "/ip4/127.0.0.1/tcp/8070/http"
        ```
        
        You can also follow the log from the indexer to see if any errors pop up.
        
    3. Now query the indexer to see if it returns results for some multihash our indexer provided
        
        ```bash
        curl http://localhost:3000/multihash/QmSPbSLo26Lt3SZ5r9ccqtoM6zjuxobLxc7JM4VivCMCmB     
        {"MultihashResults":[{"Multihash":"EiA8L8pq463M/L6z3ZcMIggzqtXcJSB3RoZn9W9qT+cEvg==","ProviderResults":[{"ContextID":"Li90ZXN0ZGF0YS9zYW1wbGUtdjEtMi5jYXI=","Metadata":{"ProtocolID":3145744,"Data":""},"Provider":{"ID":"12D3KooWFtqYPKGKPJqtTAnNLR84SphEChUfRud3bbfskK6561r5","Addrs":["/ip4/1.1.1.1/tcp/1234"]}},{"ContextID":"Li90ZXN0ZGF0YS9zYW1wbGUtdjEtMi5jYXI=","Metadata":{"ProtocolID":3145744,"Data":""},"Provider":{"ID":"12D3KooWGVwcVphAgpXjJWHoWyKZUrZsXyc34jeuUmG5nSRZyuQq","Addrs":["/ip4/1.1.1.1/tcp/1234"]}}]}]}
        ```
        
        1. If you aren’t sure which multihash to use, you can use the [ipni-cli](https://github.com/ipni/ipni-cli#ipni-cli) to list some multihashes that your provider is advertising.
            
            ```bash
            ❯ ./ipni ads get -e --ai="/ip4/127.0.0.1/tcp/3105/http/p2p/12D3KooWGVwcVphAgpXjJWHoWyKZUrZsXyc34jeuUmG5nSRZyuQq" --head
            CID:          bafy2bzaceaceibjf5pottpm4ghfnu7jo7sjcqjom4vhzm5jmq7domxun5vor4
            PreviousCID:  l  
            ProviderID:   12D3KooWGVwcVphAgpXjJWHoWyKZUrZsXyc34jeuUmG5nSRZyuQq
            Addresses:    [/ip4/1.1.1.1/tcp/1234]
            Is Remove:    false
            Metadata:     kBKjaFBpZWNlQ0lE2CpYJgABkBIAIBWfmHBGFppM9e4jZXNGIurSMwHvMle8J1NxP1WRK6hbbFZlcmlmaWVkRGVhbPVtRmFzdFJldHJpZXZhbPU=
            Extended Providers:
               None
            Signature: ✅ valid
            Signed by: content provider
            Entries:
              QmSkVyuukaYv2N31hJHYssgXU4JSqHNz2Q9jyTKGj5oadr
              QmXFz92Uc9gCyAVGKkCzD84HEiR9fmrFzPSrvUypaN2Yzx
              QmXPvCNcs8r5crUiCGzWk38A3Fo4uj7x4nsvLSRbXwNxe2
              QmZ7zzosHJaBGt8bv4CEX2seuD2PmRsHh5KAumhyCnxd89
              QmZK3kuvii88MczDES25ZK7TTkmi6kzBGiGJwvtzDme6a6
              QmZf84r66mrUJeu55jc3GZ5TQb1NqYzq8J4UtaNzBLxPHd
              QmbMTBvn8B9j7n7J1c9odBoCVnPbx8kvq8cSgtNr7kuaqc
              Qmcyb6rLKgMnq9ennMhFvSBp7XTmxMF82zQqtbo1Cj5vfW
              QmdKTcTZAzwEkxmDYncE99ooyxFfJ3oJqbetrA4Av7u4zd
              QmenEURVwoCfoqzTFGGXoCdyVj8EEBTh1Bw6zZvW7uoZv8
              QmNryAyJfpYQmUAeV8evnjZJYEftcumkemh7d8xfWar3sZ
              QmP7fGCgp6gVCNrpLURk3mBfJzUWGjKUM5LiVWUx69YEsY
              QmPU5kEm3GVnGuCn39auui6SsjC1398UkQU5ZiG7Gof1Dv
              QmRGBwcqtkbRVg51ZVKJa6aTofv77dwCC9YXhNtRekuGnn
              QmRSxDQ7uyzvDitwfdVYRC3Qa1v7SZo8ofGnLMN4Vjbzst
              QmRcK9gDacmhmYQGGwYY1zEHqyVyWf4SCjuQyzCFSF7FXj
              QmRtbCX8UMcBnyBRjfAgXrDAWXiGQgk4ErMw63pJn8fguC
              QmSPbSLo26Lt3SZ5r9ccqtoM6zjuxobLxc7JM4VivCMCmB
              QmSU4yoCLrftXuGNwk7tPFe8aLNaBfQQ1FeTc5RdRdH945
              QmSWsougtKt15WW7X48ycFuEWJ2m5nJJ95aP2TEsbzyrEW
              ---------------------
              Chunk Count: 2
              Total Count: 20
            ```

4. If you can read the update on your local indexer, then your provider is ready to go. Reach out in the #storetheindex channel in the filecoin project slack to get added to the mainnet indexer.
