package surfstore

import (
	context "context"
	"fmt"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type BlockStore struct {
	BlockMap map[string]*Block
	UnimplementedBlockStoreServer
}

func (bs *BlockStore) GetBlock(ctx context.Context, blockHash *BlockHash) (*Block, error) {
	// panic("todo")
	// fmt.Println("Test GetBlock function")
	val, ok := bs.BlockMap[blockHash.Hash]
	if !ok {
		return &Block{}, fmt.Errorf("The blockhash does not exist in the map, so failed!")
	} else {
		return val, nil
	}
}

func (bs *BlockStore) PutBlock(ctx context.Context, block *Block) (*Success, error) {
	// panic("todo")
	// fmt.Println("Test PutBlock function")
	hash := GetBlockHashString(block.BlockData[:block.BlockSize])
	bs.BlockMap[hash] = block
	// TODO: whehter the block should be copied or not, here i didn't copy
	return &Success{Flag: true}, nil
}

// Given a list of hashes “in”, returns a list containing the
// subset of in that are stored in the key-value store
func (bs *BlockStore) HasBlocks(ctx context.Context, blockHashesIn *BlockHashes) (*BlockHashes, error) {
	panic("todo")
}

// This line guarantees all method for BlockStore are implemented
var _ BlockStoreInterface = new(BlockStore)

func NewBlockStore() *BlockStore {
	return &BlockStore{
		BlockMap: map[string]*Block{},
	}
}

// Return a list containing all blockHashes on this block server
func (bs *BlockStore) GetBlockHashes(ctx context.Context, _ *emptypb.Empty) (*BlockHashes, error) {
	// panic("todo")
	// fmt.Println("Test GetBlockHashes function")
	blockHashes := make([]string, 0)
	for h := range bs.BlockMap {
		blockHashes = append(blockHashes, h)
	}
	return &BlockHashes{Hashes: blockHashes}, nil
}
