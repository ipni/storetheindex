package node

import "net/http"

func (n *Node) HealthCheckHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("\"OK\""))
}
