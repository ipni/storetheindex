package adminserver

type HealthStatus struct {
	Server     string `json:"server"`
	ValueStore string `json:"valuestore"`
}

type ValueStoreFailReason string

const (
	VS_GEN_FAILURE        ValueStoreFailReason = "Unable to generate valuestore datum"
	VS_WRITE_FAILURE                           = "Cannot store value in valuestore"
	VS_READ_FAILURE                            = "Cannot get value from valuestore"
	VS_NO_DATA                                 = "Value not present in valuestore"
	VS_READWRITE_MISMATCH                      = "Value stored does not match value retrieved from valuestore"
	VS_REMOVE_FAILURE                          = "Unable to remove value from valuestore"
)
