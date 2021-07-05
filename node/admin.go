package node

import (
	"fmt"
	"net/http"
)

func (n *Node) HealthCheckHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, err := w.Write([]byte("\"OK\""))
	if err != nil {
		fmt.Println("cannot write HealthCheck response:", err)
		return
	}
}
