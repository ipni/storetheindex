package main

import (
	"crypto/rand"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
)

const (
	encFileName = "identity.key.encrypted"
	sopsConfig  = ".sops.yaml"
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
	name := filepath.Base(os.Args[0])
	fmt.Fprintln(os.Stderr, "NAME:")
	fmt.Fprintln(os.Stderr, "   ", name)
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "USAGE:")
	fmt.Fprintln(os.Stderr, "    go run", name, "[command options]")
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "DESCRIPTION:")
	fmt.Fprintln(os.Stderr, "    Generate an encrypted identity file, identity.key.encrypted, and output the assoicated peer ID.")
	fmt.Fprintln(os.Stderr, "    Run", name, "from a directory with a", sopsConfig, "file to generat the encrypted identity.")
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "    Example:")
	fmt.Fprintln(os.Stderr, "        cd deploy/manifests/dev/us-east-2/tenant/storetheindex")
	fmt.Fprintf(os.Stderr, "        go run ../../../../../../scripts/%s\n", name)
	fmt.Fprintln(os.Stderr, "        mv", encFileName, "instances/my-indexer/")
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "OPTIONS:")
	fmt.Fprintln(os.Stderr, "    -help, -h       Show help")
}

func genIdentity() error {
	if !fileExists(sopsConfig) {
		return fmt.Errorf("must be in directory with sops config %q", sopsConfig)
	}

	pkFileName, pid, err := genPrivateKeyFile()
	if err != nil {
		return err
	}
	defer os.Remove(pkFileName)

	// sops -e identity.key > identity.key.encrypted
	cmd := exec.Command("sops", "-e", pkFileName)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	if err = cmd.Start(); err != nil {
		return err
	}
	encFile, err := os.Create(encFileName)
	if err != nil {
		return err
	}
	defer encFile.Close()

	if _, err = io.Copy(encFile, stdout); err != nil {
		os.Remove(encFile.Name())
		return err
	}
	cmd.Wait()

	fmt.Println("Generated encrypted random private key file:", encFile.Name())
	fmt.Println("Peer ID for private key:", pid)

	return nil
}

func genPrivateKeyFile() (string, peer.ID, error) {
	k, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		return "", peer.ID(""), err
	}
	pid, err := peer.IDFromPrivateKey(k)
	if err != nil {
		return "", peer.ID(""), err
	}
	mk, err := crypto.MarshalPrivateKey(k)
	if err != nil {
		return "", peer.ID(""), err
	}
	tmpFile, err := os.CreateTemp("", "identity.key")
	if err != nil {
		return "", peer.ID(""), err
	}
	if _, err = tmpFile.Write(mk); err != nil {
		tmpFile.Close()
		os.Remove(tmpFile.Name())
		return "", peer.ID(""), err
	}
	if err = tmpFile.Close(); err != nil {
		os.Remove(tmpFile.Name())
		return "", peer.ID(""), err
	}
	return tmpFile.Name(), pid, nil
}

func fileExists(filename string) bool {
	_, err := os.Lstat(filename)
	return !errors.Is(err, os.ErrNotExist)
}
