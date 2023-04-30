package SurfTest

import (
	// "cse224/proj5/pkg/surfstore"
	"cse224/proj5/pkg/surfstore"
	"fmt"
	"testing"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

func TestRaftMyself(t *testing.T) {
	//Setup
	cfgPath := "./config_files/3nodes.txt"
	test := InitTest(cfgPath)
	defer EndTest(test)
	fmt.Println("Set Leader 0=========")
	test.Clients[0].SetLeader(test.Context, &emptypb.Empty{})
	_, err := test.Clients[0].GetInternalState(test.Context, &emptypb.Empty{})
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("Set Leader 0 End =======")
	fmt.Println("SendHeartbeat ========")
	success, err := test.Clients[0].SendHeartbeat(test.Context, &emptypb.Empty{})
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("result of sendHeartbeat is ", success.Flag)
	fmt.Println("SendHeartbeat End========")
	// ----------------------------------
	fmt.Println("UpdateFile ========")
	file1_name := "1.txt"
	file1_metaData := &surfstore.FileMetaData{
		Filename:      file1_name,
		Version:       1,
		BlockHashList: make([]string, 1),
	}
	// file1_MetaData := &FileMetaData{Filename: file1_name, Version: 5, BlockHashList: file1_hashList}

	res, err := test.Clients[0].UpdateFile(test.Context, file1_metaData)
	if err != nil {
		fmt.Println(err)
	}
	// fmt.Println("result of sendHeartbeat is ", success.Flag)
	fmt.Println("Version is: ", res)
	fmt.Println("UpdateFile End========")

	fmt.Println("SendHeartbeat ========")
	success, err = test.Clients[0].SendHeartbeat(test.Context, &emptypb.Empty{})
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("result of sendHeartbeat is ", success.Flag)
	fmt.Println("SendHeartbeat End========")
	// ---------------------------

	fmt.Println("Server 1 crashed")
	_, _ = test.Clients[1].Crash(test.Context, &emptypb.Empty{})
	fmt.Println("Now Server 1 crashed")

	fmt.Println("SendHeartbeat ========")
	success, err = test.Clients[0].SendHeartbeat(test.Context, &emptypb.Empty{})
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("result of sendHeartbeat is ", success.Flag)
	fmt.Println("SendHeartbeat End========")

	fmt.Println("UpdateFile ========")
	file2_name := "2.txt"
	file2_metaData := &surfstore.FileMetaData{
		Filename:      file2_name,
		Version:       1,
		BlockHashList: make([]string, 1),
	}
	// file1_MetaData := &FileMetaData{Filename: file1_name, Version: 5, BlockHashList: file1_hashList}

	res, err = test.Clients[0].UpdateFile(test.Context, file2_metaData)
	if err != nil {
		fmt.Println(err)
	}
	// fmt.Println("result of sendHeartbeat is ", success.Flag)
	fmt.Println("Version is: ", res)
	fmt.Println("UpdateFile End========")

	fmt.Println("SendHeartbeat ========")
	success, err = test.Clients[0].SendHeartbeat(test.Context, &emptypb.Empty{})
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("result of sendHeartbeat is ", success.Flag)
	fmt.Println("SendHeartbeat End========")

	fmt.Println("UpdateFile ========")
	file3_name := "3.txt"
	file3_metaData := &surfstore.FileMetaData{
		Filename:      file3_name,
		Version:       1,
		BlockHashList: make([]string, 1),
	}
	// file1_MetaData := &FileMetaData{Filename: file1_name, Version: 5, BlockHashList: file1_hashList}

	res, err = test.Clients[0].UpdateFile(test.Context, file3_metaData)
	if err != nil {
		fmt.Println(err)
	}
	// fmt.Println("result of sendHeartbeat is ", success.Flag)
	fmt.Println("Version is: ", res)
	fmt.Println("UpdateFile End========")

	fmt.Println("SendHeartbeat ========")
	success, err = test.Clients[0].SendHeartbeat(test.Context, &emptypb.Empty{})
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("result of sendHeartbeat is ", success.Flag)
	fmt.Println("SendHeartbeat End========")

	fmt.Println("Server 1 restore ========")
	_, _ = test.Clients[1].Restore(test.Context, &emptypb.Empty{})
	// fmt.Println("result of sendHeartbeat is ", success.Flag)
	fmt.Println("Server 1 has restored========")

	fmt.Println("Set Leader 2=========")
	test.Clients[2].SetLeader(test.Context, &emptypb.Empty{})
	fmt.Println("Set Leader 2 End =======")

	fmt.Println("SendHeartbeat ========")
	success, err = test.Clients[2].SendHeartbeat(test.Context, &emptypb.Empty{})
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("result of sendHeartbeat is ", success.Flag)
	fmt.Println("SendHeartbeat End========")

	fmt.Println("SendHeartbeat ========")
	success, err = test.Clients[2].SendHeartbeat(test.Context, &emptypb.Empty{})
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("result of sendHeartbeat is ", success.Flag)
	fmt.Println("SendHeartbeat End========")
	fmt.Println("SendHeartbeat ========")
	success, err = test.Clients[2].SendHeartbeat(test.Context, &emptypb.Empty{})
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("result of sendHeartbeat is ", success.Flag)
	fmt.Println("SendHeartbeat End========")

	// fmt.Println("Server 0 crashed")
	// _, _ = test.Clients[0].Crash(test.Context, &emptypb.Empty{})
	// fmt.Println("Now Server 0 crashed")

	// fmt.Println("UpdateFile ========")
	// file2_name := "2.txt"
	// file2_metaData := &surfstore.FileMetaData{
	// 	Filename:      file2_name,
	// 	Version:       1,
	// 	BlockHashList: make([]string, 1),
	// }
	// file1_MetaData := &FileMetaData{Filename: file1_name, Version: 5, BlockHashList: file1_hashList}

	// res, err = test.Clients[1].UpdateFile(test.Context, file2_metaData)
	// if err != nil {
	// 	fmt.Println(err)
	// }
	// // fmt.Println("result of sendHeartbeat is ", success.Flag)
	// fmt.Println("Version is: ", res)
	// fmt.Println("UpdateFile End========")

	// fmt.Println("SendHeartbeat ========")
	// success, err = test.Clients[1].SendHeartbeat(test.Context, &emptypb.Empty{})
	// if err != nil {
	// 	fmt.Println(err)
	// }
	// fmt.Println("result of sendHeartbeat is ", success.Flag)

	// fmt.Println("SendHeartbeat ========")
	// success, err = test.Clients[1].SendHeartbeat(test.Context, &emptypb.Empty{})
	// if err != nil {
	// 	fmt.Println(err)
	// }
	// fmt.Println("result of sendHeartbeat is ", success.Flag)

}

func TestRaftSetLeader(t *testing.T) {
	//Setup
	cfgPath := "./config_files/3nodes.txt"
	test := InitTest(cfgPath)
	defer EndTest(test)

	fmt.Println("Set Leader =========")
	test.Clients[0].SetLeader(test.Context, &emptypb.Empty{})
	_, err := test.Clients[0].GetInternalState(test.Context, &emptypb.Empty{})
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("Set Leader End =======")

	fmt.Println("SendHeartbeat ========")
	success, err := test.Clients[0].SendHeartbeat(test.Context, &emptypb.Empty{})
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("result of sendHeartbeat is ", success.Flag)
	fmt.Println("SendHeartbeat End========")

	fmt.Println("Server 1 crashed")
	_, _ = test.Clients[1].Crash(test.Context, &emptypb.Empty{})
	fmt.Println("Now Server 1 crashed")

	fmt.Println("UpdateFile ========")
	file1_name := "1.txt"
	file1_metaData := &surfstore.FileMetaData{
		Filename:      file1_name,
		Version:       1,
		BlockHashList: make([]string, 1),
	}
	// file1_MetaData := &FileMetaData{Filename: file1_name, Version: 5, BlockHashList: file1_hashList}

	res, err := test.Clients[0].UpdateFile(test.Context, file1_metaData)
	if err != nil {
		fmt.Println(err)
	}
	// fmt.Println("result of sendHeartbeat is ", success.Flag)
	fmt.Println("Version is: ", res)
	fmt.Println("UpdateFile End========")

	fmt.Println("SendHeartbeat ========")
	success, err = test.Clients[0].SendHeartbeat(test.Context, &emptypb.Empty{})
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("result of sendHeartbeat is ", success.Flag)
	fmt.Println("SendHeartbeat End========")

	fmt.Println("UpdateFile ========")
	file2_name := "2.txt"
	file2_metaData := &surfstore.FileMetaData{
		Filename:      file2_name,
		Version:       1,
		BlockHashList: make([]string, 1),
	}
	// file1_MetaData := &FileMetaData{Filename: file1_name, Version: 5, BlockHashList: file1_hashList}

	res, err = test.Clients[0].UpdateFile(test.Context, file2_metaData)
	if err != nil {
		fmt.Println(err)
	}
	// fmt.Println("result of sendHeartbeat is ", success.Flag)
	fmt.Println("Version is: ", res)
	fmt.Println("UpdateFile End========")

	fmt.Println("SendHeartbeat ========")
	success, err = test.Clients[0].SendHeartbeat(test.Context, &emptypb.Empty{})
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("result of sendHeartbeat is ", success.Flag)

	fmt.Println("SendHeartbeat End========")

	fmt.Println("Server 1 restore ========")
	_, _ = test.Clients[1].Restore(test.Context, &emptypb.Empty{})
	// fmt.Println("result of sendHeartbeat is ", success.Flag)
	fmt.Println("Server 1 has restored========")

	fmt.Println("SendHeartbeat ========")
	success, err = test.Clients[0].SendHeartbeat(test.Context, &emptypb.Empty{})
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("result of sendHeartbeat is ", success.Flag)
	fmt.Println("SendHeartbeat End========")

	// =====================================================
	// ==================Print Function=====================
	internalstate0, err := test.Clients[0].GetInternalState(test.Context, &emptypb.Empty{})
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("server0:", ", leader:", internalstate0.IsLeader, ", term:", internalstate0.Term)
	printLog(internalstate0.Log)
	internalstate1, err := test.Clients[1].GetInternalState(test.Context, &emptypb.Empty{})
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("server1:", ", leader:", internalstate1.IsLeader, ", term:", internalstate1.Term)
	printLog(internalstate1.Log)
	internalstate2, err := test.Clients[2].GetInternalState(test.Context, &emptypb.Empty{})
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("server2:", ", leader:", internalstate2.IsLeader, ", term:", internalstate2.Term)
	printLog(internalstate2.Log)
	// =====================================================
	// =====================================================
	// panic("todo")

	// fmt.Println("Server 1 recovered")
	// _, _ = test.Clients[1].Restore(test.Context, &emptypb.Empty{})
	// fmt.Println("Now Server 1 recovered")

	// fmt.Println("SendHeartbeat ========")
	// success, err = test.Clients[0].SendHeartbeat(test.Context, &emptypb.Empty{})
	// if err != nil {
	// 	fmt.Println(err)
	// }
	// fmt.Println("result of sendHeartbeat is ", success.Flag)
	// //=====================================================
	// //==================Print Function=====================
	// internalstate0, err = test.Clients[0].GetInternalState(test.Context, &emptypb.Empty{})
	// if err != nil {
	// 	fmt.Println(err)
	// }
	// fmt.Println("server0:", ", leader:", internalstate0.IsLeader, ", term:", internalstate0.Term)
	// printLog(internalstate0.Log)
	// internalstate1, err = test.Clients[1].GetInternalState(test.Context, &emptypb.Empty{})
	// if err != nil {
	// 	fmt.Println(err)
	// }
	// fmt.Println("server1:", ", leader:", internalstate1.IsLeader, ", term:", internalstate1.Term)
	// printLog(internalstate1.Log)
	// internalstate2, err = test.Clients[2].GetInternalState(test.Context, &emptypb.Empty{})
	// if err != nil {
	// 	fmt.Println(err)
	// }
	// fmt.Println("server2:", ", leader:", internalstate2.IsLeader, ", term:", internalstate2.Term)
	// printLog(internalstate2.Log)
	// //=====================================================
	// //=====================================================
	// fmt.Println("SendHeartbeat ========")
	// success, err = test.Clients[0].SendHeartbeat(test.Context, &emptypb.Empty{})
	// if err != nil {
	// 	fmt.Println(err)
	// }
	// fmt.Println("result of sendHeartbeat is ", success.Flag)

}

// how to write testcase on your own
func TestRaftFollowerUpdates(t *testing.T) {
	t.Log("leader1 gets a request")
	cfgPath := "./config_files/3nodes.txt"
	test := InitTest(cfgPath)
	defer EndTest(test)

	// set node 0 to be the leader
	leaderIdx := 0
	test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})

	// update a file on node 0
	filemeta := &surfstore.FileMetaData{
		Filename:      "testfile",
		Version:       1,
		BlockHashList: nil,
	}
	test.Clients[leaderIdx].UpdateFile(test.Context, filemeta)
	test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})

	// one final call to sendheartbeat (from spec)
	test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})

	// check that all the nodes are in the right state
	// leader and all followers should have the entry in the log
	// everything should be term 1
	// entry should be term 1
	// entry should be applied to all metastores
	// only node 0 should be leader
	goldenMeta := make(map[string]*surfstore.FileMetaData)
	goldenMeta[filemeta.Filename] = filemeta

	goldenLog := make([]*surfstore.UpdateOperation, 0)
	goldenLog = append(goldenLog, &surfstore.UpdateOperation{
		Term:         1,
		FileMetaData: filemeta,
	})

	term := int64(1)
	var leader bool
	for idx, server := range test.Clients {
		if idx == leaderIdx {
			leader = bool(true)
		} else {
			leader = bool(false)
		}
		_, err := CheckInternalState(&leader, &term, nil, nil, server, test.Context)

		if err != nil {
			t.Fatalf("Error checking state for server %d: %s", idx, err.Error())
		}
	}
}
