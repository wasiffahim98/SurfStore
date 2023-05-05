package surfstore

import (
	"crypto/sha256"
	"encoding/hex"
	"math/big"
	"sort"
)

type Node struct {
	Addr  string
	Index int
}

type ConsistentHashRing struct {
	RingSize int
	Nodes    []Node
}

// Perform a modulo operation on a hash string.
// The hash string is assumed to be hexadecimally encoded.
func HashMod(hashString string, ringSize int) int {
	hashBytes, _ := hex.DecodeString(hashString)
	hashInt := new(big.Int).SetBytes(hashBytes[:])
	ringSizeInt := big.NewInt(int64(ringSize))

	indexInt := new(big.Int).Mod(hashInt, ringSizeInt)

	return int(indexInt.Int64())
}

// Compute a block’s index on the ring from its hash value.
func (ms *ConsistentHashRing) ComputeBlockIndex(blockHash string) int {
	return HashMod(blockHash, ms.RingSize)
}

// Compute a node’s index on the ring from its address string.
func (ms *ConsistentHashRing) ComputeNodeIndex(nodeAddr string) int {
	hashBytes := sha256.Sum256([]byte(nodeAddr))
	hashString := hex.EncodeToString(hashBytes[:])
	return HashMod(hashString, ms.RingSize)
}

// Find the hosting node for the given ringIndex. It’s basically the first node on the ring with node.Index >= ringIndex (in a modulo sense).
func (ms *ConsistentHashRing) FindHostingNode(ringIndex int) Node {
	// Try to implement a O(log N) solution here using binary search.
	// It's also fine if you can't because we don't test your perforrmance.
	//panic("todo")
	newNodeList := ms.Nodes
	for i := 0; i < len(newNodeList); i++ {
		if newNodeList[i].Index >= ringIndex {
			return newNodeList[i]
		}
	}
	return newNodeList[0]
}

// Add the given nodeAddr to the ring.
func (ms *ConsistentHashRing) AddNode(nodeAddr string) {
	// O(N) solution is totally fine here.
	// O(log N) solution might be overly complicated.
	//panic("todo")
	var newNode Node
	result := []Node{}
	currList := ms.Nodes
	hashBytes := sha256.Sum256([]byte(nodeAddr))
	hashString := hex.EncodeToString(hashBytes[:])
	newNodeIndex := HashMod(hashString, ms.RingSize)
	newNode.Index = newNodeIndex
	newNode.Addr = nodeAddr
	if newNodeIndex < currList[0].Index {
		result = append(result, newNode)
	}
	for i := 0; i < len(currList)-1; i++ {
		if currList[i].Index < newNodeIndex && currList[i+1].Index > newNodeIndex {
			result = append(result, currList[i])
			result = append(result, newNode)
		} else {
			result = append(result, currList[i])
		}
	}
	result = append(result, currList[len(currList)-1])
	if newNodeIndex > currList[len(currList)-1].Index {
		result = append(result, newNode)
	}
	ms.Nodes = result
}

// Remove the given nodeAddr from the ring.
func (ms *ConsistentHashRing) RemoveNode(nodeAddr string) {
	// O(N) solution is totally fine here.
	// O(log N) solution might be overly complicated.
	//panic("todo")
	var badNode Node
	result := []Node{}
	currList := ms.Nodes
	hashBytes := sha256.Sum256([]byte(nodeAddr))
	hashString := hex.EncodeToString(hashBytes[:])
	badNodeIndex := HashMod(hashString, ms.RingSize)
	badNode.Index = badNodeIndex
	badNode.Addr = nodeAddr
	for i := 0; i < len(currList); i++ {
		if currList[i].Index != badNodeIndex {
			result = append(result, currList[i])
		}
	}
	ms.Nodes = result
}

func (ms *ConsistentHashRing) PrecedingNode(ringIndex int) Node {
	preNodeIndex := ms.Nodes[len(ms.Nodes)-1]
	for i := 0; i < len(ms.Nodes); i++ {
		if ms.Nodes[i].Index < ringIndex {
			preNodeIndex = ms.Nodes[i]
		} else {
			return preNodeIndex
		}
	}
	return ms.Nodes[len(ms.Nodes)-1]
}

// Create consistent hash ring struct with a list of blockstore addresses
func NewConsistentHashRing(ringSize int, blockStoreAddrs []string) ConsistentHashRing {
	// You can not use ComputeNodeIndex method to compute the ring index of blockStoreAddr in blockStoreAddrs here.
	// You will need to use HashMod function, remember to hash the blockStoreAddr before calling HashMod
	// Hint: refer to ComputeNodeIndex method on how to hash the blockStoreAddr before calling HashMod
	//panic("todo")
	result := []Node{}
	var res1 Node
	var res2 ConsistentHashRing
	for i := 0; i < len(blockStoreAddrs); i++ {
		hashBytes := sha256.Sum256([]byte(blockStoreAddrs[i]))
		hashString := hex.EncodeToString(hashBytes[:])
		modVal := HashMod(hashString, ringSize)
		res1.Index = modVal
		res1.Addr = blockStoreAddrs[i]
		result = append(result, res1)
	}
	sortMap := make(map[int]string)
	for i := 0; i < len(result); i++ {
		currResultNode := result[i]
		sortMap[currResultNode.Index] = currResultNode.Addr
	}
	resultIntVal := []int{}
	for i := 0; i < len(result); i++ {
		resultIntVal = append(resultIntVal, result[i].Index)
	}
	sort.Ints(resultIntVal)
	var newNode Node
	finalResult := []Node{}
	for i := 0; i < len(resultIntVal); i++ {
		newNode.Index = resultIntVal[i]
		newNode.Addr = sortMap[resultIntVal[i]]
		finalResult = append(finalResult, newNode)
	}

	res2.Nodes = finalResult
	res2.RingSize = ringSize
	return res2
}
