# Creating an Index Provider

## An Overview of the Indexer system

There are a couple players here. I'll use specific examples to make it clearer,
but note that some of these can be generalized.

1. Filecoin Storage Provider – Hosts data for folks and proves it via the
Filecoin Network Chain. Aka Storage Provider.
2. Indexer (aka [storetheindex](https://github.com/filecoin-project/storetheindex)) – A service that can answer the question:
"Given this CID, who has a copy?". This is little more than a lookup table.
3. Index Provider – A service that runs alongside a Storage Provider and tells
the Indexer what content this storage provider has.

The Index Provider serves as the interface between the storage provider and the
indexer. It can be used from within `Lotus` so that the publishing of new data
happens automatically. But it can also happen separately.

The Index Provider sends updates to the Indexer via a series of [Advertisement](https://github.com/filecoin-project/storetheindex/blob/main/api/v0/ingest/schema/schema.ipldsch)
messages. Each message references a previous advertisement so that as a whole it
forms an advertisement chain. The state of the indexer is basically a function
of consuming this chain from the initial Advertisement to the latest one.

## HTTP Index Provider

An HTTP Index provider needs to provide two endpoints:

1. `GET /head`
    - This returns the latest Advertisement that the index provider knows about.
2. `GET /<cid>`
    - This returns the content for a block identified by the given cid.

And that’s it. With those two endpoints an indexer should be able to sync with the provider.

### On Syncing

If an indexer has registered an index provider it will occasionally poll the provider to check if there is new content to index. If you want to let the indexer know you have new changes you may call the `Announce` endpoint on the indexer.

## libp2p Index Provider

While this should be possible in any language, Go has the best support here. If you’re trying to target another language I’d recommend building an HTTP Index provider.

In Go, it’s simplest to use [go-legs](https://github.com/filecoin-project/go-legs). Legs provides a simpler interface to go-data-transfer and graphsync. 

For an index-provider you’ll want to setup a go-legs Publisher:

```go
pub, err := dtsync.NewPublisher(pubLibp2pHost, datastore, linksys, topicName)
```

And when you have a new Advertisement you’ll call:

```go
err = pub.UpdateRoot(ctx, newAdCid)
```

That will automatically send a message on the gossipsub channel to let the indexer know there’s a new update for this provider.

The go-legs publisher will also handle the datatransfer requests from the indexer automatically.

## Testing your index Provider

After you finish building your index provider, you probably want to test it. You can run the indexer locally and tell it about your index provider. That should cause the indexer to sync from your index provider. Then you can query the indexer to see if it has some content that your index provider provided.

1. Make sure your index provider has actual content it is providing. You can do this by checking the `/head` endpoint if implementing an HTTP provider or by using [https://pkg.go.dev/github.com/filecoin-project/go-legs/dtsync#Syncer.GetHead](https://pkg.go.dev/github.com/filecoin-project/go-legs/dtsync#Syncer.GetHead) with go-legs.
    - HTTP:
        
        ```bash
        ❯ curl http://localhost:8070/head
        {"head":{"/":"bafy2bzaceaceibjf5pottpm4ghfnu7jo7sjcqjom4vhzm5jmq7domxun5vor4"},"sig":{"/":{"bytes":"nPm4HNlVZxOuNZ0ujKbP7YT7eGpenOHfSzrhRid0dTyz1HHKalLZwIHWL+sArsEVuMvKrL0hVqKkBwz/9aMvAA=="}},"pubkey":{"/":{"bytes":"CAESIGNJlqCl/NPKn3FCWNtWmWCjzJ7qb7ClJZQJg3tHDDk2"}}}
        ```
        
2. Make sure you can also fetch that block. For an HTTP provider you should get the block back when hitting `/<cid>`. For a libp2p provider you should be able to call [https://pkg.go.dev/github.com/filecoin-project/go-legs/dtsync#Syncer.Sync](https://pkg.go.dev/github.com/filecoin-project/go-legs/dtsync#Syncer.Sync). 
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
        storetheindex admin sync -p 12D3KooWGVwcVphAgpXjJWHoWyKZUrZsXyc34jeuUmG5nSRZyuQq --addr "/ip4/127.0.0.1/tcp/8070/http/p2p/12D3KooWGVwcVphAgpXjJWHoWyKZUrZsXyc34jeuUmG5nSRZyuQq"
        ```
        
        You can also follow the log from the indexer to see if any errors pop up.
        
    3. Now query the indexer to see if it returns results for some multihash our indexer provided
        
        ```bash
        curl http://localhost:3000/multihash/QmSPbSLo26Lt3SZ5r9ccqtoM6zjuxobLxc7JM4VivCMCmB     
        {"MultihashResults":[{"Multihash":"EiA8L8pq463M/L6z3ZcMIggzqtXcJSB3RoZn9W9qT+cEvg==","ProviderResults":[{"ContextID":"Li90ZXN0ZGF0YS9zYW1wbGUtdjEtMi5jYXI=","Metadata":{"ProtocolID":3145744,"Data":""},"Provider":{"ID":"12D3KooWFtqYPKGKPJqtTAnNLR84SphEChUfRud3bbfskK6561r5","Addrs":["/ip4/1.1.1.1/tcp/1234"]}},{"ContextID":"Li90ZXN0ZGF0YS9zYW1wbGUtdjEtMi5jYXI=","Metadata":{"ProtocolID":3145744,"Data":""},"Provider":{"ID":"12D3KooWGVwcVphAgpXjJWHoWyKZUrZsXyc34jeuUmG5nSRZyuQq","Addrs":["/ip4/1.1.1.1/tcp/1234"]}}]}]}
        ```
        
        1. If you aren’t sure which multihash to use, you can use the [index-provider cli](https://github.com/filecoin-project/index-provider/tree/main/cmd/provider) to list some multihashes that your provider is advertising.
            
            ```bash
            ❯ provider list -e -p "/ip4/127.0.0.1/tcp/8070/http/p2p/12D3KooWGVwcVphAgpXjJWHoWyKZUrZsXyc34jeuUmG5nSRZyuQq"
            ID:          bafy2bzaceaceibjf5pottpm4ghfnu7jo7sjcqjom4vhzm5jmq7domxun5vor4
            PreviousID:  b
            ProviderID:  12D3KooWGVwcVphAgpXjJWHoWyKZUrZsXyc34jeuUmG5nSRZyuQq
            Addresses:   [/ip4/1.1.1.1/tcp/1234]
            Is Remove:   false
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
