package surfstore

import (
	"fmt"
	"net/rpc"
)

type MetaStore struct {
	FileMetaMap    map[string]FileMetaData
	BlockStoreRing ConsistentHashRing
}

func (m *MetaStore) GetFileInfoMap(succ *bool, serverFileInfoMap *map[string]FileMetaData) error {
	for k, v := range m.FileMetaMap {
		(*serverFileInfoMap)[k] = v
	}

	return nil
}

func (m *MetaStore) UpdateFile(fileMetaData *FileMetaData, latestVersion *int) (err error) {
	oldFileMeta, exist := m.FileMetaMap[fileMetaData.Filename]
	if !exist {
		// Create a dummy old file meta if the file does not exist yet
		oldFileMeta = FileMetaData{
			Filename:      "",
			Version:       0,
			BlockHashList: nil,
		}
	}

	// Compare the Version and decide to update or not. Should be exactly 1 greater
	if oldFileMeta.Version+1 == fileMetaData.Version {
		m.FileMetaMap[fileMetaData.Filename] = *fileMetaData
	} else {
		err = fmt.Errorf("Unexpected file Version. Yours:%d, Expected:%d, Lastest on Server:%d\n",
			fileMetaData.Version, oldFileMeta.Version+1, oldFileMeta.Version)
	}

	*latestVersion = m.FileMetaMap[fileMetaData.Filename].Version

	return
}

// Given an input hashlist, returns a mapping of BlockStore addresses to hashlists.
func (m *MetaStore) GetBlockStoreMap(blockHashesIn []string, blockStoreMap *map[string][]string) error {
	// this should be different from your project 3 implementation. Now we have multiple
	// Blockstore servers instead of one Blockstore server in project 3. For each blockHash in
	// blockHashesIn, you want to find the BlockStore server it is in using consistent hash ring.
	//panic("todo")
	// result := make(map[string][]string)
	// for i := 0; i < len(blockHashesIn); i++ {
	// 	newIndex := m.BlockStoreRing.ComputeBlockIndex(blockHashesIn[i])
	// 	newNode := m.BlockStoreRing.FindHostingNode(newIndex)
	// 	currList := result[newNode.Addr]
	// 	currList = append(currList, blockHashesIn[i])
	// 	result[newNode.Addr] = currList
	// }
	// blockStoreMap = &result
	// return nil

	for i := 0; i < len(blockHashesIn); i++ {
		currIndex := HashMod(blockHashesIn[i], m.BlockStoreRing.RingSize)
		nextNode := m.BlockStoreRing.FindHostingNode(currIndex)
		(*blockStoreMap)[nextNode.Addr] = append((*blockStoreMap)[nextNode.Addr], blockHashesIn[i])
	}
	return nil
}

// Add the specified BlockStore node to the cluster and migrate the blocks
func (m *MetaStore) AddNode(nodeAddr string, succ *bool) error {
	// compute node index
	//panic("todo")
	currIndex := m.BlockStoreRing.ComputeNodeIndex(nodeAddr)

	//FOR PRECEEDING NODE
	// preNodeIndex := 0
	// for i := 0; i < m.BlockStoreRing.RingSize; i++ {
	// 	currNodeAddy := m.BlockStoreRing.Nodes[i].Addr
	// 	currNodeIndex := m.BlockStoreRing.ComputeNodeIndex(currNodeAddy)
	// 	if currNodeIndex > preNodeIndex && currNodeIndex < currIndex {
	// 		preNodeIndex = currNodeIndex
	// 	}
	// }

	preNodeIndex := m.BlockStoreRing.PrecedingNode(currIndex).Index

	// find successor node
	//panic("todo")
	successorNode := m.BlockStoreRing.FindHostingNode(currIndex + 1)

	// call RPC to migrate some blocks from successor node to this node
	inst := MigrationInstruction{
		LowerIndex: preNodeIndex,
		UpperIndex: currIndex,
		DestAddr:   nodeAddr,
	}

	// connect to the server
	conn, e := rpc.DialHTTP("tcp", successorNode.Addr)
	if e != nil {
		return e
	}

	// perform the call
	e = conn.Call("BlockStore.MigrateBlocks", inst, succ)
	if e != nil {
		conn.Close()
		return e
	}

	// deal with added node in BlockStoreRing
	//panic("todo")
	m.BlockStoreRing.AddNode(nodeAddr)

	// close the connection
	return conn.Close()
}

// Remove the specified BlockStore node from the cluster and migrate the blocks
func (m *MetaStore) RemoveNode(nodeAddr string, succ *bool) error {
	// compute node index
	//panic("todo")
	currIndex := m.BlockStoreRing.ComputeNodeIndex(nodeAddr)

	//FOR PRECEEDING NODE
	// preNodeIndex := 0
	// for i := 0; i < m.BlockStoreRing.RingSize; i++ {
	// 	currNodeAddy := m.BlockStoreRing.Nodes[i].Addr
	// 	currNodeIndex := m.BlockStoreRing.ComputeNodeIndex(currNodeAddy)
	// 	if currNodeIndex > preNodeIndex && currNodeIndex < currIndex {
	// 		preNodeIndex = currNodeIndex
	// 	}
	// }

	preNodeIndex := m.BlockStoreRing.PrecedingNode(currIndex).Index

	// find successor node
	//panic("todo")
	successorNode := m.BlockStoreRing.FindHostingNode(currIndex + 1)

	// call RPC to migrate all blocks from this node to successor node
	inst := MigrationInstruction{
		LowerIndex: preNodeIndex,
		UpperIndex: currIndex,
		DestAddr:   successorNode.Addr,
	}

	// connect to the server
	conn, e := rpc.DialHTTP("tcp", nodeAddr)
	if e != nil {
		return e
	}

	// perform the call
	e = conn.Call("BlockStore.MigrateBlocks", inst, succ)
	if e != nil {
		conn.Close()
		return e
	}

	// deal with removed node in BlockStoreRing
	//panic("todo")
	m.BlockStoreRing.RemoveNode(nodeAddr)

	// close the connection
	return conn.Close()
}

var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreRing ConsistentHashRing) MetaStore {
	return MetaStore{
		FileMetaMap:    map[string]FileMetaData{},
		BlockStoreRing: blockStoreRing,
	}
}
