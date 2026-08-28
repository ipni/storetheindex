package httpserver

import (
	"encoding/json"
	"mime"
	"net/http"

	"github.com/ipni/go-libipni/announce/message"
)

const (
	// AnnounceEncodingJSON and AnnounceEncodingCBOR are the encoding labels
	// returned by DecodeAnnounceMessage.
	AnnounceEncodingJSON = "json"
	AnnounceEncodingCBOR = "cbor"

	// maxAnnounceBodySize is the limit on an announce request body. No request
	// body should be this large, so any request exceeding this size is clearly
	// in error.
	maxAnnounceBodySize = 1024 * 1024
)

// DecodeAnnounceMessage reads an IPNI announce Message from the request body.
// JSON is used when the Content-Type media type is application/json, including
// when a charset parameter is present. Any other Content-Type is decoded as
// CBOR.
func DecodeAnnounceMessage(w http.ResponseWriter, r *http.Request) (message.Message, string, error) {
	var an message.Message
	bodyReader := http.MaxBytesReader(w, r.Body, maxAnnounceBodySize)
	if jsonContentType(r.Header.Get("Content-Type")) {
		err := json.NewDecoder(bodyReader).Decode(&an)
		return an, AnnounceEncodingJSON, err
	}
	err := an.UnmarshalCBOR(bodyReader)
	return an, AnnounceEncodingCBOR, err
}

func jsonContentType(contentType string) bool {
	mediaType, _, err := mime.ParseMediaType(contentType)
	return err == nil && mediaType == MediaTypeJson
}
