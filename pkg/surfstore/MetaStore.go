package surfstore

import (
	context "context"

	"sync"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

var lock sync.Mutex

type MetaStore struct {
	FileMetaMap        map[string]*FileMetaData
	BlockStoreAddrs    []string
	ConsistentHashRing *ConsistentHashRing
	UnimplementedMetaStoreServer
}

func (m *MetaStore) GetFileInfoMap(ctx context.Context, _ *emptypb.Empty) (*FileInfoMap, error) {
	// panic("todo")
	// TODO: here i also not sure whether i should copy the whole file
	return &FileInfoMap{FileInfoMap: m.FileMetaMap}, nil
}

func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {
	// panic("todo")

	// 判断是否有这个文件
	// 1. if does not exist, if version == 1, add fileMetaData

	// 2. if does exist
	//      if version = origine version + 1, update
	//      else return fail
	lock.Lock()
	val, ok := m.FileMetaMap[fileMetaData.Filename]
	if !ok {
		if fileMetaData.Version == 1 {
			// TODO: here i am not sure whether i have to copy the fileMetaData
			m.FileMetaMap[fileMetaData.Filename] = fileMetaData
			lock.Unlock()
			return &Version{Version: 1}, nil
		} else {
			// return &Version{Version: -1}, fmt.Errorf("Failed during updateing, the unexisted file version should be 1")
			lock.Unlock()
			return &Version{Version: -1}, nil
		}
	} else {
		if fileMetaData.Version == val.Version+1 {
			// TODO: here i am not sure whether i have to copy the fileMetaData
			m.FileMetaMap[fileMetaData.Filename] = fileMetaData
			lock.Unlock()
			return &Version{Version: m.FileMetaMap[fileMetaData.Filename].Version}, nil
		} else {
			// return &Version{Version: -1}, fmt.Errorf("Failed to update the file, the file on the cloud is not the latest version")
			lock.Unlock()
			return &Version{Version: -1}, nil
		}
	}

}

//	func (m *MetaStore) GetBlockStoreAddr(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddr, error) {
//		// panic("todo")
//		return &BlockStoreAddr{Addr: m.BlockStoreAddr}, nil
//	}
func (m *MetaStore) GetBlockStoreMap(ctx context.Context, blockHashesIn *BlockHashes) (*BlockStoreMap, error) {
	// panic("todo")
	blockstoreMap := make(map[string]*BlockHashes)
	for _, blockhash := range blockHashesIn.Hashes {
		server := m.ConsistentHashRing.GetResponsibleServer(blockhash)
		_, ok := blockstoreMap[server]
		if !ok {
			blockstoreMap[server] = &BlockHashes{Hashes: make([]string, 0)}
		}
		blockstoreMap[server].Hashes = append(blockstoreMap[server].Hashes, blockhash)
	}
	return &BlockStoreMap{BlockStoreMap: blockstoreMap}, nil
}

func (m *MetaStore) GetBlockStoreAddrs(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddrs, error) {
	return &BlockStoreAddrs{BlockStoreAddrs: m.BlockStoreAddrs}, nil
}

// This line guarantees all method for MetaStore are implemented
var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreAddrs []string) *MetaStore {
	return &MetaStore{
		FileMetaMap:        map[string]*FileMetaData{},
		BlockStoreAddrs:    blockStoreAddrs,
		ConsistentHashRing: NewConsistentHashRing(blockStoreAddrs),
	}
}
