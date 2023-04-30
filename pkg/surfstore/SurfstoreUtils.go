package surfstore

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strings"
)

func testCreateFileMetaDate(client RPCClient) *map[string]*FileMetaData {
	a := make(map[string]*FileMetaData)
	file1_name := "1.txt"
	file1_hashList := createBlockHashList(file1_name, client)
	file1_MetaData := &FileMetaData{Filename: file1_name, Version: 1, BlockHashList: file1_hashList}
	file2_name := "2.txt"
	file2_hashList := createBlockHashList(file2_name, client)
	file2_MetaData := &FileMetaData{Filename: file2_name, Version: 2, BlockHashList: file2_hashList}
	a["1.txt"] = file1_MetaData
	a["2.txt"] = file2_MetaData
	return &a
}

func testWriteMetaFile(client RPCClient) {
	a := testCreateFileMetaDate(client)
	path, err := os.Getwd()
	if err != nil {
		fmt.Println(err)
	}
	path = path + "/" + client.BaseDir
	// PrintMetaMap(*a)
	// println("=========")
	err = WriteMetaFile(*a, path)
	if err != nil {
		fmt.Println(err)
	}
}

// idea
// panic("todo")
// 1.whether local directory have db file
//   if not, generate db file
// 2. go through all the files in the local directory
//    for each, create metadata(filename, hashindex, hashvalue) to compare with db
//
//    case 1: db doesn't contain such file
//    case 2: the hashindex and hashvalue is different
// 3. case 3: the file was removed (file showed in db cannot be found in local directory)
//    go through the file in the db file and find which does not exist in local directory

//  Idea: use a map (called virtual map) to collect all the files needed to update during this period
//  For the mentioned three cases, do the block side operation
//  case 1 + case 2: put block
//  At the same time, create FileMetaData{Filename, version, blockHashList}
//  case 1 + case 2: create with Filename, version, blockHashList
//  case 3: creaet with Filename, version, blockHashList( []string = make([]string, 1), string[0] = "0")
//
// =====================================================================================
//  GetFileInfoMap: get the fileinfo map
//  compare the serverFileInfoMap with the local db, find the file with different version
//  In fact, only two scenarios can happen, server has new files or servers's version is larger
//  So just go through the serverFileInfoMap, decide whether 1.the file exist in local db and 2.the hashlist is different of not 3. the server index is larger
//  if the file does not exist in local db, download, remove the file from virtual map
//  if the file exist, server version is larger than db version, just download, remove the file from virtual map
//  For the rest file in virtual map,
//            upload theses files
//
// =============================================================================
//  Two types operation, upload vs. download
//  1. download, with given hashlist, get the corresponding hashblock
//				 write the file
//				 update the local db
//  2. upload, putBlock has already been done (not needed)
//     	       update FileMetaData{Filename, version, blockHashList} (created during previous steps, just upload)
//			   case 1(successful):
//						lastest version == update version, successful
//				       update the local db
// 			   case 2:(fail):
// 					   latest version == -1 (failed)
//                     down the fileInfoMap -> get the file -> get the information
//                     continue downloading operation
//					   rewrite the file
//					   update the local db

func helperGetServer(client RPCClient, hashlist []string) map[string]string {
	res := make(map[string][]string)
	res1 := &res
	err := client.GetBlockStoreMap(hashlist, res1)
	if err != nil && err.Error() == "All server crashed." {
		log.Fatal("server crashed")
		// return
	}
	dataServer := make(map[string]string)
	for key, val := range res {
		for _, v := range val {
			dataServer[v] = key
		}
	}
	return dataServer
}

// Implement the logic for sendBlock client syncing with the server here.
// TODO:
func ClientSync(client RPCClient) {
	// temp1 := ""
	// temp := make([]string, 0)
	// BlockStoreAddr := &temp

	// TODO : BlockStoreAddr all need changed

	// client.GetBlockStoreAddrs(&temp)
	// get db information
	local_fileMetaMap := make(map[string]*FileMetaData)
	// fmt.Println("initla local_fileMetaMap")

	pwd, err := os.Getwd()
	curDirPath := pwd + "/" + client.BaseDir
	local_fileMetaMap, err = LoadMetaFromMetaFile(curDirPath)

	// PrintMetaMap(local_fileMetaMap)

	if err != nil {
		fmt.Println("Error 1")
	}
	localFiles, err := ioutil.ReadDir(curDirPath)
	if err != nil {
		fmt.Println("Error 2")
	}
	modifiedFileMap := make(map[string]*FileMetaData)
	for _, localFile := range localFiles {
		if strings.Compare("index.db", localFile.Name()) == 0 {
			continue
		}
		// fmt.Println(localFile.Name() + " exist in the file")
		beforeFile, ok := local_fileMetaMap[localFile.Name()]
		if !ok {
			// fmt.Println(localFile.Name() + " is created")
			hashlist := createBlockHashList(localFile.Name(), client)
			metaData := &FileMetaData{Filename: localFile.Name(), Version: 1, BlockHashList: hashlist}
			modifiedFileMap[localFile.Name()] = metaData
			// fmt.Println("ModifiedFileMap is: ")
			// PrintMetaMap(modifiedFileMap)
			updateBlock(client, localFile.Name(), hashlist)
		} else {
			hashlist := createBlockHashList(localFile.Name(), client)
			if !compareStringArray(hashlist, beforeFile.BlockHashList) {
				// fmt.Println(localFile.Name() + " has been modified")
				metaData := &FileMetaData{Filename: localFile.Name(), Version: beforeFile.Version + 1, BlockHashList: hashlist}
				modifiedFileMap[localFile.Name()] = metaData
				updateBlock(client, localFile.Name(), hashlist)

				// fmt.Println("ModifiedFileMap is: ")
				// PrintMetaMap(modifiedFileMap)
			}
		}
	}
	// fmt.Println("ModifiedFileMap is: ")
	// PrintMetaMap(modifiedFileMap)
	for key, value := range local_fileMetaMap {
		_, err := os.Stat(curDirPath + "/" + key)
		if os.IsNotExist(err) {
			if len(value.BlockHashList) == 1 && value.BlockHashList[0] == "0" {
				continue
			}
			metaData := &FileMetaData{Filename: key, Version: value.Version + 1, BlockHashList: tombstone()}
			modifiedFileMap[key] = metaData
		}
	}

	// fmt.Println("This is the modifiedFileMap")
	// PrintMetaMap(modifiedFileMap)

	// get the server fileinfomap
	server_fileMetaMap := make(map[string]*FileMetaData)
	err = client.GetFileInfoMap(&server_fileMetaMap)
	if err != nil {
		if err.Error() == "All server crashed." {
			log.Fatal("server crashed")
			// return
		}
		fmt.Println(err)
		fmt.Println("Failed to get server FileInfoMap")
	}
	// fmt.Println("Server_fileMetaMap  is")
	// PrintMetaMap(server_fileMetaMap)

	for serverFileName, serverFileInfo := range server_fileMetaMap {
		localFileInfo, ok := local_fileMetaMap[serverFileName]
		if !ok {
			_, exist := local_fileMetaMap[serverFileName]
			if exist {
				delete(modifiedFileMap, serverFileName)
			}
			download(client, serverFileInfo, local_fileMetaMap)
		} else {
			if serverFileInfo.Version > localFileInfo.Version {
				_, exist := local_fileMetaMap[serverFileName]
				if exist {
					delete(modifiedFileMap, serverFileName)
				}
				download(client, serverFileInfo, local_fileMetaMap)
			}
		}
	}
	// fmt.Println("modifiedFileMap is")
	// PrintMetaMap(modifiedFileMap)
	for _, modifiedInfo := range modifiedFileMap {
		// updateBlock(client, modifiedName)
		upload(modifiedInfo, client, local_fileMetaMap)
	}

	// fmt.Println("finally local_fileMetaMap")
	// PrintMetaMap(local_fileMetaMap)

	err = client.GetFileInfoMap(&server_fileMetaMap)
	if err != nil {
		if err.Error() == "All server crashed." {
			log.Fatal("server crashed")
			return
		}
		fmt.Println("Failed to get server FileInfoMap")
	}
	// fmt.Println("finally server_fileMetaMap")
	// PrintMetaMap(server_fileMetaMap)

	WriteMetaFile(local_fileMetaMap, curDirPath)
}

func tombstone() []string {
	res := make([]string, 1)
	res[0] = "0"
	return res
}

func compareStringArray(a []string, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i, str := range a {
		if strings.Compare(str, b[i]) != 0 {
			return false
		}
	}
	return true
}

// TODO: localDBMap map[string]*FileMetaData jump now parameter
// fileMetaData is the metaData of the file that needs to download
func download(client RPCClient, fileMetaData *FileMetaData, localDBMap map[string]*FileMetaData) error {
	filePath := client.BaseDir + "/" + fileMetaData.Filename
	if _, err := os.Stat(filePath); err == nil {
		e := os.Remove(filePath)
		if e != nil {
			fmt.Println("Failed to remove the original file")
		}
	}
	if len(fileMetaData.BlockHashList) == 1 && strings.Compare(fileMetaData.BlockHashList[0], "0") == 0 {
		localDBMap[fileMetaData.Filename] = fileMetaData
		return nil
	}
	f, err := os.Create(filePath)
	defer f.Close()
	if err != nil {
		fmt.Println("Failed to create the local file")
	}
	hashMetaMap := helperGetServer(client, fileMetaData.BlockHashList)
	for _, blockHash := range fileMetaData.BlockHashList {
		receiveBlock := &Block{}
		err2 := client.GetBlock(blockHash, hashMetaMap[blockHash], receiveBlock)
		if err2 != nil {
			return fmt.Errorf("Failed to get the block")
		}
		f.Write(receiveBlock.BlockData[:receiveBlock.BlockSize])
	}
	localDBMap[fileMetaData.Filename] = fileMetaData
	return nil
}

func upload(fileMetaData *FileMetaData, client RPCClient, localDBMap map[string]*FileMetaData) error {
	temp := (int32)(0)
	var latestVersion *int32
	latestVersion = &temp
	err := client.UpdateFile(fileMetaData, latestVersion)
	if err != nil {
		if err.Error() == "All server crashed." {
			log.Fatal("server crashed")
			return err
		}
		return fmt.Errorf("Something went wrong during upload operation")
	}
	if fileMetaData.Version == *latestVersion {
		fmt.Println("Successfully upload the file to the server")
		localDBMap[fileMetaData.Filename] = fileMetaData
	} else if *latestVersion == -1 {
		fileInfoMap := make(map[string]*FileMetaData)
		err := client.GetFileInfoMap(&fileInfoMap)
		if err != nil && err.Error() == "All server crashed." {
			log.Fatal("server crashed")
			return err
		}
		download(client, fileInfoMap[fileMetaData.Filename], localDBMap)
		fmt.Println("Race condition happened during uploading files")
	} else {
		fmt.Println("The latestVersion is very wired, need to figure out the problem")
	}
	return nil
}

// test upload function

// function 1. initialize db, create the localMap
// parameter: baseDir
// return: Map: localMap
func initializeDbFile(baseDir string) error {
	dbPath := baseDir + "/index.db"
	_, err := os.Stat(dbPath)
	if err != nil {
		if os.IsNotExist(err) {
			_, err := os.Create(dbPath)
			if err != nil {
				return err
			}
			return nil
		}
		return fmt.Errorf("failed to initialize index.db")
	}
	return nil
}

// function 2. go throught the base directory, find the files are modified
// parameter: baseDir, virtualMap
// return: modified virtualMap

// function 3. create the metadata(filename, hashindex, hashvalue)

// PrintMetaMap
//

//Test createBlockHashList code
// res := createBlockHashList()
// for _, element := range res {
// 	fmt.Println("=========")
// 	fmt.Println(element)
// }

func testCase5(client RPCClient) *map[string]*FileMetaData {
	temp := map[string]*FileMetaData{}
	serverFileInfoMap := &temp
	err := client.GetFileInfoMap(serverFileInfoMap)
	if err != nil {
		fmt.Println("Something went wrong during GetFileInfoMap")
	}
	// PrintMetaMap(*serverFileInfoMap)
	return serverFileInfoMap
}

// handle the block, upload file content to the block
func updateBlock(client RPCClient, filename string, hashlist []string) {
	// var res []string
	path, err := os.Getwd()
	if err != nil {
		fmt.Println(err)
	}
	// fmt.Println(path)
	f, err := os.Open(path + "/" + client.BaseDir + "/" + filename)
	defer f.Close()
	if err != nil {
		// fmt.Println("Failed to open the local file: ")
		fmt.Println(err)
	}
	hash_Map := helperGetServer(client, hashlist)
	idx := 0
	// fmt.Println("Test helperGetServer")
	// for k, v := range hash_Map {
	// fmt.Println("block_hash:" + k + ", server: " + v)
	// }
	// fmt.Println("Finish testing helperGetServer")
	for {
		body := make([]byte, client.BlockSize)
		n1, err := f.Read(body)
		if err != nil {
			// fmt.Println("Something went wrong during reading file")
			// fmt.Println(err)
		}
		if err == io.EOF {
			fmt.Println("Finished reading the file.")
			break
		}
		// fmt.Println(" n = ", n1)
		if n1 != client.BlockSize {
			helper := false
			succ := &helper
			sendBlock := &Block{BlockData: body, BlockSize: int32(n1)}
			fmt.Println("???????????????? last")
			fmt.Println("GetBlockHashString(body): ", GetBlockHashString(body))
			fmt.Println("hashlist[idx]: ", hashlist[idx])
			fmt.Println("================")
			err = client.PutBlock(sendBlock, hash_Map[hashlist[idx]], succ)
			idx++
			if err != nil || *succ != true {
				fmt.Println("case1")
				fmt.Print(err)
				fmt.Println("Blcokstore adddrs: ")
				// fmt.Println("Wait Oh no, that's so sad!!!, failed to upload the block")
			}
			if *succ != true {
				fmt.Println("failed to upload the block")
			}
			break
		}
		// fmt.Println("body = ", body)
		// hash := GetBlockHashString(body)
		// fmt.Println("hash = " + hash)
		// res = append(res, hash)
		helper := false
		succ := &helper
		sendBlock := &Block{BlockData: body, BlockSize: int32(n1)}
		fmt.Println("????????????????")
		fmt.Println("GetBlockHashString(body): ", GetBlockHashString(body))
		fmt.Println("hashlist[idx]: ", hashlist[idx])
		fmt.Println("================")
		// hashValue := GetBlockHashString(body)
		err = client.PutBlock(sendBlock, hash_Map[hashlist[idx]], succ)
		idx++
		if err != nil || *succ != true {
			fmt.Println("case2")
			fmt.Print(err)
			// fmt.Println("Wait Oh no, that's so sad!!!, failed to upload the block")
		}
		if *succ != true {
			fmt.Println("failed to upload the block")
		}

	}
}

// read a file and create the BlockHashList
// n1 is the blocksize(probably)
func createBlockHashList(filename string, client RPCClient) []string {
	var res []string
	path, err := os.Getwd()
	if err != nil {
		fmt.Println(err)
	}
	// fmt.Println(path)
	f, err := os.Open(path + "/" + client.BaseDir + "/" + filename)
	if err != nil {
		// fmt.Println("Failed to open the local file: ")
		fmt.Println(err)
	}
	for {
		body := make([]byte, client.BlockSize)
		n1, err := f.Read(body)
		if err != nil {
			// fmt.Println("Something went wrong during reading file")
			fmt.Println(err)
		}
		if err == io.EOF {
			fmt.Println("Finished reading the file.")
			break
		}
		// fmt.Println(" n = ", n1)
		if n1 != client.BlockSize {
			// fmt.Println("body = ", body)
			hash := GetBlockHashString(body[:n1])
			// fmt.Println("hash = " + hash)
			res = append(res, hash)
			break
		}
		// fmt.Println("body = ", body)
		hash := GetBlockHashString(body)
		// fmt.Println("hash = " + hash)
		res = append(res, hash)
	}
	fmt.Println("test createblockhashlist")
	for _, v := range res {
		fmt.Println(v)
	}
	fmt.Println("finish testing createblockhashlist")
	return res
}

// testcase 4: UpdateFile
func testCase4(client RPCClient) {
	file1_name := "1.txt"
	file1_hashList := createBlockHashList(file1_name, client)
	file1_MetaData := &FileMetaData{Filename: file1_name, Version: 5, BlockHashList: file1_hashList}
	temp1 := (int32)(0)
	var file1_latestVersion *int32
	file1_latestVersion = &temp1
	err1 := client.UpdateFile(file1_MetaData, file1_latestVersion)
	// fmt.Println("Latest version is", *file1_latestVersion)
	if err1 != nil {
		if err1.Error() == "All server crashed." {
			log.Fatal("server crashed")
			return
		}
		fmt.Println(err1)
		fmt.Println("file1 update failed")
	}
	// if *file1_latestVersion != (int32)(1) {
	// 	fmt.Println("file1 the latest version is wrong")
	// }
	fmt.Println("file1 cloud latest version is ", *file1_latestVersion)
	// fmt.Println("file1 update successfully!")

	file2_name := "2.txt"
	file2_hashList := createBlockHashList(file2_name, client)
	file2_MetaData := &FileMetaData{Filename: file2_name, Version: 4, BlockHashList: file2_hashList}
	temp2 := (int32)(0)
	var file2_latestVersion *int32
	file2_latestVersion = &temp2
	err2 := client.UpdateFile(file2_MetaData, file2_latestVersion)
	// fmt.Println("Lastest version is", *file2_latestVersion)
	if err2 != nil {
		fmt.Println(err2)
		fmt.Println("file2 update failed")
	}
	fmt.Println("file2 cloud latest version is ", *file2_latestVersion)
	// if *file2_latestVersion != (int32)(1) {
	// 	fmt.Println("file1 the latest version is wrong")
	// }
	// fmt.Println("file2 update successfully!")
	file3_name := "3.txt"
	file3_hashList := createBlockHashList(file3_name, client)
	file3_MetaData := &FileMetaData{Filename: file3_name, Version: 1, BlockHashList: file3_hashList}
	temp3 := (int32)(0)
	var file3_latestVersion *int32
	file3_latestVersion = &temp3
	err3 := client.UpdateFile(file3_MetaData, file3_latestVersion)
	// fmt.Println("Lastest version is", *file3_latestVersion)
	if err3 != nil {
		fmt.Println(err3)
		fmt.Println("file3 update failed")
	}
	fmt.Println("file3 cloud latest version is ", *file3_latestVersion)
}

// testcase 3: getBlockStoreAddr
// func testCase3(client RPCClient) {
// 	addr := ""
// 	blockStoreAddr := &addr
// 	err := client.GetBlockStoreAddr(blockStoreAddr)
// 	if err != nil {
// 		fmt.Println("Something going wrong")
// 	}
// 	fmt.Println(*blockStoreAddr)
// 	if *blockStoreAddr != "localhost:8081" {
// 		fmt.Println("The blockstoreAddr is wrong")
// 	}
// }

// testcase 1: put block and get block
func testCase1(client RPCClient) {
	// test PutBlock
	data := make([]byte, 5)
	for i := 0; i < 5; i++ {
		data[i] = byte(i)
	}
	sendBlock := &Block{BlockData: data, BlockSize: 5}
	helper := false
	succ := &helper
	err := client.PutBlock(sendBlock, "localhost:8081", succ)
	if err != nil {
		fmt.Println("There's some problem with function!")
	}
	if succ == nil {
		fmt.Println("nil pointer")
	}
	if *succ == true {
		fmt.Println("the block is put into blockstore successfully!")
	} else {
		fmt.Println("Failed")
	}

	// test GetBlock
	blockhash := GetBlockHashString(sendBlock.BlockData)
	receiveBlock := &Block{}
	err2 := client.GetBlock(blockhash, "localhost:8081", receiveBlock)
	if err2 != nil {
		fmt.Println("There's some problem with function2!")
	}
	// fmt.Println(receiveBlock.BlockData)
	// fmt.Println(receiveBlock.BlockSize)
	if bytes.Compare(receiveBlock.BlockData, sendBlock.BlockData) != 0 {
		fmt.Println("The blockData is wrong!")
	}
	if receiveBlock.BlockSize != sendBlock.BlockSize {
		fmt.Println("The block size is wrong!")
	}
}

// testcase 2: get unexisted block, expected return err != nil
func testCase2(client RPCClient) {
	blockhash := "123"
	receiveBlock := &Block{}
	err2 := client.GetBlock(blockhash, "localhost:8081", receiveBlock)
	if err2 != nil {
		fmt.Println(err2)
	}
}
