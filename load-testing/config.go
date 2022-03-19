package main

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"go/constant"
	"go/parser"
	mathrand "math/rand"
	"os"
	"strconv"
	"strings"
)

type Config struct {
	AdsPerSec uint `json:"adsPerSec"`
	// A generator to specify how many entries per ad.
	// A function so the caller can define a distribution to follow.
	EntriesPerAdGenerator func() uint `json:"-"`
	// For json to be able to use a predefined distribution.
	EntriesPerAdType string `json:"entriesPerAdType"`
	EntriesPerChunk  uint   `json:"entriesPerChunk"`
	// How often is this ad a rm? Read this value as a 1 in N chance. Higher is less frequent.
	oddsOfRm uint `json:"oddsOfRm"`
	// Should this provider be an http provider?
	IsHttp         bool   `json:"isHttp"`
	HttpListenAddr string `json:"httpListenAddr"`
	// How many of the last N ads should be kept. 0 means every ad is kept.
	KeepNAds uint   `json:"keepNAds"`
	Seed     uint64 `json:"seed"`

	StopAfterNEntries uint64 `json:"stopAfterNEntries"`

	ListenMultiaddr string `json:"listenMultiaddr"`
	GossipSubTopic  string `json:"gossipSubTopic"`
}

func evalBasicLit(expr *ast.BasicLit) constant.Value {
	return constant.MakeFromLiteral(expr.Value, expr.Kind, 0)
}

func (c *Config) ParseEntriesPerAdGenerator() bool {
	astV, _ := parser.ParseExpr(c.EntriesPerAdType)
	distributionType, ok := astV.(*ast.CallExpr)
	if !ok {
		return false
	}
	switch distributionType.Fun.(*ast.Ident).Name {
	case "Normal":
		// Normal(stdev, mean)
		σ, ok := constant.Float64Val(evalBasicLit(distributionType.Args[0].(*ast.BasicLit)))
		if !ok {
			return false
		}
		μ, ok := constant.Float64Val(evalBasicLit(distributionType.Args[1].(*ast.BasicLit)))
		if !ok {
			return false
		}
		c.EntriesPerAdGenerator = func() uint {
			return uint(mathrand.NormFloat64()*σ + μ)
		}
	case "Uniform":
		// Uniform(start, end)
		start, ok := constant.Int64Val(evalBasicLit(distributionType.Args[0].(*ast.BasicLit)))
		if !ok {
			return false
		}
		end, ok := constant.Int64Val(evalBasicLit(distributionType.Args[1].(*ast.BasicLit)))
		if !ok {
			return false
		}
		c.EntriesPerAdGenerator = func() uint {
			return uint(mathrand.Intn(int(end-start)) + int(start))
		}
	case "Always":
		// Always(value)
		v, ok := constant.Uint64Val(evalBasicLit(distributionType.Args[0].(*ast.BasicLit)))
		if !ok {
			return false
		}
		c.EntriesPerAdGenerator = func() uint {
			return uint(v)
		}
	}
	return true
}

func DefaultConfig() Config {
	return Config{
		AdsPerSec: 4,
		oddsOfRm:  0,
		EntriesPerAdGenerator: func() uint {
			return uint(mathrand.NormFloat64()*10 + 70)
		},
		EntriesPerChunk: 10,
		IsHttp:          false,
		KeepNAds:        0,
		Seed:            1,
		// The actual listen address will be this plus the seed for the port
		ListenMultiaddr: "/ip4/127.0.0.1/tcp/9001",
		HttpListenAddr:  "127.0.0.1:9002",
		GossipSubTopic:  "indexer/ingest",
	}
}

func incrementListenMultiaddrPortBy(ma string, n uint) (string, error) {
	parts := strings.Split(ma, "/")
	port, err := strconv.Atoi(parts[len(parts)-1])
	if err != nil {
		return "", err
	}
	parts[len(parts)-1] = strconv.Itoa(port + int(n))
	return strings.Join(parts, "/"), nil
}

func incrementHttpListenPortBy(ma string, n uint) (string, error) {
	parts := strings.Split(ma, ":")
	port, err := strconv.Atoi(parts[len(parts)-1])
	if err != nil {
		return "", err
	}
	parts[len(parts)-1] = strconv.Itoa(port + int(n))
	return strings.Join(parts, ":"), nil
}

func LoadConfigFromFile(file string) (Config, error) {
	defaultConf := DefaultConfig()
	b, err := os.ReadFile(file)
	if err != nil {
		return defaultConf, err
	}

	c := &defaultConf
	err = json.Unmarshal(b, c)

	if err != nil {
		return defaultConf, err
	}

	if !c.ParseEntriesPerAdGenerator() {
		return defaultConf, fmt.Errorf("could not parse entries per ad generator")
	}
	return *c, nil
}
