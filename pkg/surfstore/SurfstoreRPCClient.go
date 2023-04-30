package surfstore

import (
	context "context"
	"fmt"

	// "fmt"
	"time"

	grpc "google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

type RPCClient struct {
	MetaStoreAddrs []string
	BaseDir        string
	BlockSize      int
}

func (surfClient *RPCClient) GetBlock(blockHash string, blockStoreAddr string, block *Block) error {
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	b, err := c.GetBlock(ctx, &BlockHash{Hash: blockHash})
	if err != nil {
		conn.Close()
		return err
	}
	block.BlockData = b.BlockData
	block.BlockSize = b.BlockSize

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) PutBlock(block *Block, blockStoreAddr string, succ *bool) error {
	// panic("todo")
	// connect to the server

	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	b, err := c.PutBlock(ctx, &Block{BlockData: block.BlockData, BlockSize: block.BlockSize})
	if err != nil {
		conn.Close()
		return err
	}
	*succ = b.Flag
	return conn.Close()
}

func (surfClient *RPCClient) HasBlocks(blockHashesIn []string, blockStoreAddr string, blockHashesOut *[]string) error {
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	hashes, err := c.HasBlocks(ctx, &BlockHashes{Hashes: blockHashesIn})
	if err != nil {
		conn.Close()
		return err
	}
	blockHashesOut = &hashes.Hashes
	return conn.Close()
}

func (surfClient *RPCClient) GetFileInfoMap(serverFileInfoMap *map[string]*FileMetaData) error {
	// panic("todo")
	// connect to the server
	// conn, err := grpc.Dial(surfClient.MetaStoreAddr, grpc.WithInsecure())
	fmt.Println("get into getfileinfo map function ")
	responses := make(chan string, len(surfClient.MetaStoreAddrs))
	for _, addr := range surfClient.MetaStoreAddrs {
		go helper(responses, addr)
	}
	count := 0
	metaStoreaddr := ""
	for {
		metaStoreAddrs := <-responses
		// fmt.Println("metaStoreAddrs:", metaStoreAddrs)
		count++
		if metaStoreAddrs != "false" {
			// fmt.Println(" ??? ")
			metaStoreaddr = metaStoreAddrs
			break
		}
		// fmt.Println("count = ", count)
		// fmt.Println("len(surfClient.MetaStoreAddrs = ", len(surfClient.MetaStoreAddrs))
		if count == len(surfClient.MetaStoreAddrs) {
			return fmt.Errorf("All server crashed.")
		}
	}
	conn, err := grpc.Dial(metaStoreaddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	// c := NewMetaStoreClient(conn)
	c := NewRaftSurfstoreClient(conn)

	// perform the call

	//TODO: does not know the meaning of empty
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	fileinfoMap, err := c.GetFileInfoMap(ctx, &emptypb.Empty{})
	if err != nil {
		conn.Close()
		return err
	}
	*serverFileInfoMap = fileinfoMap.FileInfoMap
	// close the connection
	return conn.Close()

}

func helper(responses chan string, addr string) {
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	c := NewRaftSurfstoreClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	fmt.Println("addr: ", addr, ", i guess it's okay here.")
	_, err = c.SendHeartbeat(ctx, &emptypb.Empty{})
	time.Sleep(100 * time.Millisecond)
	if err == nil {
		responses <- addr
	} else if err.Error() != "rpc error: code = Unknown desc = Server is crashed." && err.Error() != "rpc error: code = Unknown desc = Server is not the leader" {
		fmt.Println("addr: ", addr)
		fmt.Println("err :", err)
		responses <- addr
	} else {
		fmt.Println("addr: ", addr)
		responses <- "false"
	}
}

func (surfClient *RPCClient) UpdateFile(fileMetaData *FileMetaData, latestVersion *int32) error {
	fmt.Println("get into updatefile function")
	responses := make(chan string, len(surfClient.MetaStoreAddrs))
	for _, addr := range surfClient.MetaStoreAddrs {
		go helper(responses, addr)
	}
	count := 0
	metaStoreaddr := ""
	for {
		metaStoreAddrs := <-responses
		count++
		if metaStoreAddrs != "false" {
			metaStoreaddr = metaStoreAddrs
			break
		}
		if count == len(surfClient.MetaStoreAddrs) {
			return fmt.Errorf("All server crashed.")
		}
	}
	// metaStoreaddr := <-responses
	conn, err := grpc.Dial(metaStoreaddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	// c := NewMetaStoreClient(conn)
	c := NewRaftSurfstoreClient(conn)
	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	version, err := c.UpdateFile(ctx, fileMetaData)
	if err != nil {
		conn.Close()
		fmt.Println("Since the update operation is not successful, we just close the conn, so the latest version may not change")
		return err
	}
	*latestVersion = version.Version
	return conn.Close()
}

// func (surfClient *RPCClient) GetBlockStoreAddr(blockStoreAddr *string) error {
// 	// panic("todo")
// 	// connect to the server
// 	conn, err := grpc.Dial(surfClient.MetaStoreAddr, grpc.WithInsecure())
// 	if err != nil {
// 		return err
// 	}
// 	c := NewMetaStoreClient(conn)

// 	// perform the call
// 	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
// 	defer cancel()
// 	addr, err := c.GetBlockStoreAddr(ctx, &emptypb.Empty{})
// 	if err != nil {
// 		conn.Close()
// 		return err
// 	}
// 	*blockStoreAddr = addr.Addr
// 	// close the connection
// 	return conn.Close()
// }

func (surfClient *RPCClient) GetBlockStoreMap(blockHashesIn []string, blockStoreMap *map[string][]string) error {
	// fmt.Println("Get into the function GetBlockStoreMap !!!!!")
	// conn, err := grpc.Dial(surfClient.MetaStoreAddr, grpc.WithInsecure())
	fmt.Println("get into GetBlockStoreMap")
	responses := make(chan string, len(surfClient.MetaStoreAddrs))
	for _, addr := range surfClient.MetaStoreAddrs {
		go helper(responses, addr)
	}
	count := 0
	metaStoreaddr := ""
	for {
		metaStoreAddrs := <-responses
		fmt.Println("metaStoreAddrs: ", metaStoreAddrs)
		count++
		if metaStoreAddrs != "false" {
			metaStoreaddr = metaStoreAddrs
			break
		}
		if count == len(surfClient.MetaStoreAddrs) {
			return ERR_NOT_LEADER
		}
	}
	conn, err := grpc.Dial(metaStoreaddr, grpc.WithInsecure())
	fmt.Println("TEST 1111111111")
	fmt.Println("metaStoreaddr : ", metaStoreaddr)
	if err != nil {
		return err
	}
	// c := NewMetaStoreClient(conn)
	c := NewRaftSurfstoreClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	blockstoreMap, err := c.GetBlockStoreMap(ctx, &BlockHashes{Hashes: blockHashesIn})
	if err != nil {
		fmt.Println(err)
		fmt.Println("I want to make sure that it works")
		conn.Close()
		return err
	}
	temp := make(map[string][]string)
	for key, value := range blockstoreMap.BlockStoreMap {
		temp2 := make([]string, 0)
		for _, v := range value.Hashes {
			temp2 = append(temp2, v)
		}
		temp[key] = temp2
	}
	*blockStoreMap = temp
	return conn.Close()
}

func (surfClient *RPCClient) GetBlockStoreAddrs(blockStoreAddrs *[]string) error {
	// panic("TODO")
	// connect to the server
	// conn, err := grpc.Dial(surfClient.MetaStoreAddr, grpc.WithInsecure())
	fmt.Println("get into getblockstoreaddrs")
	responses := make(chan string, len(surfClient.MetaStoreAddrs))
	for _, addr := range surfClient.MetaStoreAddrs {
		go helper(responses, addr)
	}
	count := 0
	metaStoreaddr := ""
	for {
		metaStoreAddrs := <-responses
		count++
		if metaStoreAddrs != "false" {
			metaStoreaddr = metaStoreAddrs
			break
		}
		if count == len(surfClient.MetaStoreAddrs) {
			return ERR_NOT_LEADER
		}
	}

	conn, err := grpc.Dial(metaStoreaddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewRaftSurfstoreClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	blockstoreAddrs, err := c.GetBlockStoreAddrs(ctx, &emptypb.Empty{})
	if err != nil {
		conn.Close()
		return err
	}
	*blockStoreAddrs = blockstoreAddrs.BlockStoreAddrs
	return conn.Close()
}

func (surfClient *RPCClient) GetBlockHashes(blockStoreAddr string, blockHashes *[]string) error {
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	block_Hashes, err := c.GetBlockHashes(ctx, &emptypb.Empty{})
	if err != nil {
		conn.Close()
		return err
	}
	*blockHashes = block_Hashes.Hashes
	return conn.Close()
}

// This line guarantees all method for RPCClient are implemented
var _ ClientInterface = new(RPCClient)

// Create an Surfstore RPC client
func NewSurfstoreRPCClient(addrs []string, baseDir string, blockSize int) RPCClient {

	return RPCClient{
		MetaStoreAddrs: addrs,
		BaseDir:        baseDir,
		BlockSize:      blockSize,
	}
}
