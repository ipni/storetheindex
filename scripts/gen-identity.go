package main

import (
	"crypto/rand"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
)

func main() {
	flag.Usage = usage
	flag.Parse()

	err := genIdentity()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func usage() {
	const encFileName = "identity.key.encrypted"
	_, name, _, _ := runtime.Caller(0)
	name = filepath.Base(name)
	fmt.Fprintln(os.Stderr, "NAME:")
	fmt.Fprintln(os.Stderr, "   ", name)
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "USAGE:")
	fmt.Fprintln(os.Stderr, "    go run", name, "[command options]")
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "DESCRIPTION:")
	fmt.Fprintln(os.Stderr, "    Generate an encrypted identity and write the data to stdout. The associated peer ID is printed to stderr.")
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "    Example use:")
	fmt.Fprintf(os.Stderr, "        sops -e <(go run %s) > %s\n", name, encFileName)
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "OPTIONS:")
	fmt.Fprintln(os.Stderr, "    -help, -h       Show help")
}

func genIdentity() error {
	k, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		return err
	}
	pid, err := peer.IDFromPrivateKey(k)
	if err != nil {
		return err
	}
	mk, err := crypto.MarshalPrivateKey(k)
	if err != nil {
		return err
	}
	os.Stdout.Write(mk)
	fmt.Fprintln(os.Stderr, "Peer ID for private key:", pid)
	return nil
}
