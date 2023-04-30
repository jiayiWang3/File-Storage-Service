package surfstore

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
	// "strconv"
)

type ConsistentHashRing struct {
	ServerMap map[string]string
}

func (c ConsistentHashRing) GetResponsibleServer(blockId string) string {
	hashes := []string{}
	for h := range c.ServerMap {
		hashes = append(hashes, h)
	}
	sort.Strings(hashes)
	responsibleServer := ""
	for i := 0; i < len(hashes); i++ {
		if hashes[i] > blockId {
			responsibleServer = c.ServerMap[hashes[i]]
			break
		}
	}
	if responsibleServer == "" {
		responsibleServer = c.ServerMap[hashes[0]]
	}
	return responsibleServer
}

func (c ConsistentHashRing) Hash(addr string) string {
	h := sha256.New()
	h.Write([]byte(addr))
	return hex.EncodeToString(h.Sum(nil))
}

func NewConsistentHashRing(serverAddrs []string) *ConsistentHashRing {
	// panic("todo")
	hashring := make(map[string]string)
	for _, serverName := range serverAddrs {
		temp := "blockstore" + serverName
		// fmt.Println("hashkey is ", temp)
		h := sha256.New()
		h.Write([]byte(temp))
		key := hex.EncodeToString(h.Sum(nil))
		hashring[key] = serverName
	}
	// fmt.Println("Initialize new consistet hash ring")
	// for key, value := range hashring {
	// fmt.Println(key + ": " + value)
	// }
	// fmt.Println("Finish initializing NewConsistentHashRing")
	return &ConsistentHashRing{hashring}
}
