package surfstore

import (
	context "context"
	"fmt"
	"math"

	// "min"
	"sync"
	"time"

	"google.golang.org/grpc"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

// TODO Add fields you need here
type RaftSurfstore struct {
	wholeLock     *sync.Mutex
	isLeader      bool
	isLeaderMutex *sync.RWMutex
	term          int64
	log           []*UpdateOperation

	metaStore *MetaStore

	// for all server
	id                  int64
	peers               []string
	commitIndex         int64
	lastApplied         int64
	pendingCommits      []*chan bool
	pendingCommitsMutex *sync.RWMutex

	// for leader
	nextIndex  []int64
	matchIndex []int64

	/*--------------- Chaos Monkey --------------*/
	isCrashed      bool
	isCrashedMutex *sync.RWMutex
	UnimplementedRaftSurfstoreServer
}

// res[0]: whether is crashed, res[1]: whether is leader
func (s *RaftSurfstore) initialJudge() []bool {
	res := make([]bool, 2)
	s.isCrashedMutex.Lock()
	if s.isCrashed {
		res[0] = true
	}
	s.isCrashedMutex.Unlock()
	s.isLeaderMutex.Lock()
	if s.isLeader {
		res[1] = true
	}
	s.isLeaderMutex.Unlock()
	return res
}

// TODO: synchronization problem, when result is get, how keep sending information
// to those server who haven't replied
func (s *RaftSurfstore) GetFileInfoMap(ctx context.Context, empty *emptypb.Empty) (*FileInfoMap, error) {
	fmt.Println("Get into GetFileInfoMap.")
	state := s.initialJudge()
	if state[0] {
		return nil, ERR_SERVER_CRASHED
	}
	if !state[1] {
		return nil, ERR_NOT_LEADER
	}

	commitChan := make(chan bool)
	s.pendingCommits = append(s.pendingCommits, &commitChan)
	go s.sendAllFollowersInParllel_Infinite(ctx, commitChan)

	commit := <-commitChan

	if commit {
		return s.metaStore.GetFileInfoMap(ctx, &emptypb.Empty{})
	} else {
		return nil, fmt.Errorf("The server %d is no longer a leader", s.id)
	}
}

func (s *RaftSurfstore) GetBlockStoreMap(ctx context.Context, hashes *BlockHashes) (*BlockStoreMap, error) {
	state := s.initialJudge()
	if state[0] {
		return nil, ERR_SERVER_CRASHED
	}
	if !state[1] {
		return nil, ERR_NOT_LEADER
	}

	commitChan := make(chan bool)
	s.pendingCommits = append(s.pendingCommits, &commitChan)
	go s.sendAllFollowersInParllel_Infinite(ctx, commitChan)

	commit := <-commitChan
	if commit {
		return s.metaStore.GetBlockStoreMap(ctx, hashes)
	} else {
		return nil, fmt.Errorf("The server %d is no longer a leader", s.id)
	}
}

func (s *RaftSurfstore) GetBlockStoreAddrs(ctx context.Context, empty *emptypb.Empty) (*BlockStoreAddrs, error) {
	state := s.initialJudge()
	if state[0] {
		return nil, ERR_SERVER_CRASHED
	}
	if !state[1] {
		return nil, ERR_NOT_LEADER
	}

	commitChan := make(chan bool)
	s.pendingCommits = append(s.pendingCommits, &commitChan)
	go s.sendAllFollowersInParllel_Infinite(ctx, commitChan)

	commit := <-commitChan
	if commit {
		return s.metaStore.GetBlockStoreAddrs(ctx, empty)
	} else {
		return nil, fmt.Errorf("The server %d is no longer a leader", s.id)
	}
}

func (s *RaftSurfstore) sendToFollower_Infinite_updatefile(ctx context.Context, idx int, addr string, responses chan bool, curCommit int64, term int64) {
	// fmt.Println("server id:", idx, ", go into TEST sendToFollower _Infinite_updatefile, idx = ", idx)
	for {
		// s.wholeLock.Lock()
		fmt.Println("server id:", idx, ", go into TEST sendToFollower _Infinite_updatefile, idx = ", idx)
		ni := s.nextIndex[idx]
		fmt.Println("idx = ", idx, ", ni= ", ni)
		prevLogTerm := int64(-1)
		prevLogIndex := ni - 1
		if prevLogIndex >= 0 {
			prevLogTerm = s.log[prevLogIndex].Term
		}
		entry := s.log[ni:]
		// leaderCommit := s.commitIndex
		// endpoint := len(s.log)
		dummyAppendEntriesInput := AppendEntryInput{
			Term:         term,
			PrevLogTerm:  prevLogTerm,
			PrevLogIndex: prevLogIndex,
			Entries:      entry,
			LeaderCommit: s.commitIndex,
		}
		// s.wholeLock.Unlock()
		conn, _ := grpc.Dial(addr, grpc.WithInsecure())
		client := NewRaftSurfstoreClient(conn)
		output, err := client.AppendEntries(ctx, &dummyAppendEntriesInput)
		// s.wholeLock.Lock()
		time.Sleep(50 * time.Millisecond)
		if output == nil || err != nil {
			if err.Error() == "rpc error: code = Canceled desc = context canceled" {
				fmt.Println("server id : ", idx, ", It sounds works for rpc error")
				return
			} else if output == nil || err.Error() == "rpc error: code = Unknown desc = Server is crashed." {
				// fmt.Println("server idx", idx, "IS now right?")
				// fmt.Println("whehter output is nil: ", output == nil)
				// fmt.Println("whether server is crashed: ", err.Error() == "rpc error: code = Unknown desc = Server is crashed.")
				// fmt.Println(err)
				time.Sleep(50 * time.Millisecond)
				continue
			}
		}
		// if output == nil {
		// 	fmt.Println("server id = ", idx, " The output is nil.")
		// }
		// fmt.Println("idx = ", idx, ", Probably here something went wrong.")
		if output.Term > s.term {
			fmt.Println("Server Become a follower.")
			responses <- false
			// s.wholeLock.Unlock()
			break
		}
		if output.Success {
			s.nextIndex[idx] = ni + int64(len(entry))
			fmt.Println("server id: ", idx, ", s.nextIndex[idx] = ", s.nextIndex)
			if s.nextIndex[idx] >= 0 {
				s.matchIndex[idx] = s.nextIndex[idx] - 1
			}
			responses <- true
			break
		} else {
			fmt.Println("server id:", idx, ", test2.")
			// s.nextIndex[idx] = int64(math.Max(float64(0), float64(ni-1)))
			fmt.Println("Modified here.")
			s.nextIndex[idx] = int64(math.Max(float64(0), float64(output.MatchedIndex+1)))
			// s.matchIndex[output.ServerId] = output.MatchedIndex
			fmt.Println("server id:", idx, ", there might be some problems with the indexes!")
			// s.wholeLock.Unlock()
			continue
		}
	}
}

func (s *RaftSurfstore) sendAllFollowersInParllel_Infinite_updatefile(ctx context.Context, commitChan chan bool, curCommit int64, term int64) {
	// fmt.Println("server id:", s.id, ", go into TEST sendAllFollowersInParllel_Infinite_updatefile")
	responses := make(chan bool, len(s.peers)-1)
	for idx, addr := range s.peers {
		if int64(idx) == s.id {
			continue
		}
		go s.sendToFollower_Infinite_updatefile(ctx, idx, addr, responses, curCommit, term)
	}
	time.Sleep(50 * time.Millisecond)
	totalResponses := 1
	totalAppends := 1

	for {
		result := <-responses
		totalResponses++
		if result {
			totalAppends++
		}
		if totalAppends > len(s.peers)/2 {
			break
		}
	}
	if totalAppends > len(s.peers)/2 {
		commitChan <- true
		// s.commitIndex++
	}
}

func (s *RaftSurfstore) UpdateFile(ctx context.Context, filemeta *FileMetaData) (*Version, error) {
	// fmt.Println("server id:", s.id, ", go into TEST UpdateFile")
	state := s.initialJudge()
	if state[0] {
		return nil, ERR_SERVER_CRASHED
	}
	if !state[1] {
		return nil, ERR_NOT_LEADER
	}
	s.log = append(s.log, &UpdateOperation{
		Term:         s.term,
		FileMetaData: filemeta,
	})
	commitChan := make(chan bool)
	s.pendingCommits = append(s.pendingCommits, &commitChan)
	// temp_LeaderCommit := s.commitIndex
	temp_curCommit := len(s.log) - 1
	temp_term := s.term
	go s.sendAllFollowersInParllel_Infinite_updatefile(ctx, commitChan, int64(temp_curCommit), temp_term)

	commit := <-commitChan
	if commit {
		s.commitIndex++
		s.lastApplied++
		return s.metaStore.UpdateFile(ctx, filemeta)
		// UpdateFile(ctx, filemeta)
	} else {
		return nil, fmt.Errorf("The server %d is no longer a leader", s.id)
	}
}

// 1. Reply false if term < currentTerm (§5.1)
// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term
// matches prevLogTerm (§5.3)
// 3. If an existing entry conflicts with a new one (same index but different
// terms), delete the existing entry and all that follow it (§5.3)
// 4. Append any new entries not already in the log
// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index
// of last new entry)
// TODO: to call this function, the server must not crash
func (s *RaftSurfstore) AppendEntries(ctx context.Context, input *AppendEntryInput) (*AppendEntryOutput, error) {
	// panic("todo")
	// TODO:  if leader changed
	// fmt.Println("server id = ", s.id, " go into AppendEntries")
	state := s.initialJudge()
	if state[0] {
		fmt.Println("server id = ", s.id, "I guess here might exist some problems.")
		return &AppendEntryOutput{ServerId: s.id, Term: s.term, Success: false, MatchedIndex: int64(-1)}, ERR_SERVER_CRASHED
	}
	if input.Term > s.term {
		s.isLeaderMutex.Lock()
		defer s.isLeaderMutex.Unlock()
		s.isLeader = false
		s.term = input.Term
	}
	// fmt.Println("Leader, input.Term:", input.Term, ", input.PreLogTerm: ", input.PrevLogTerm, ", input.PreLogIndex: ", input.PrevLogIndex, ", input.LeaderCommit: ", input.LeaderCommit)
	// fmt.Println("server id = ", s.id, ", s.term: ", s.term, ", len(s.log): ", len(s.log), ", s.commitIdx: ", s.commitIndex, ", s.lastApplied: ", s.lastApplied)
	// 1. Reply false if term < currentTerm (§5.1)
	if input.Term < s.term {
		return &AppendEntryOutput{ServerId: s.id, Term: s.term, Success: false, MatchedIndex: int64(-1)}, nil
	}
	// fmt.Println("111111111")
	// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	// TODO: not sure about the index here
	if input.PrevLogIndex != -1 {
		if int64(len(s.log)) <= input.PrevLogIndex || input.PrevLogTerm != s.log[input.PrevLogIndex].Term {
			fmt.Println("I guess should go into this line")

			return &AppendEntryOutput{ServerId: s.id, Term: s.term, Success: false, MatchedIndex: s.commitIndex}, nil
		}

		// 3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)
		// if int64(len(s.log)) > input.PrevLogIndex && input.PrevLogTerm != s.log[input.PrevLogIndex].Term {
		s.log = s.log[:input.PrevLogIndex+1]
		// }

		// 4. Append any new entries not already in the log
		s.log = append(s.log, input.Entries...)
	} else {
		if len(input.Entries) != 0 {
			s.log = make([]*UpdateOperation, 0)
			s.log = append(s.log, input.Entries...)
		}
	}
	// fmt.Println("2222222222")

	// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if input.LeaderCommit > s.commitIndex {
		s.commitIndex = int64(math.Min(float64(input.LeaderCommit), float64(len(s.log)-1)))
	}
	// fmt.Println("3333333333")
	// if commitIndex > lastApplied, increment lastApplied, and apply the operation to the machine
	for s.lastApplied < s.commitIndex {
		entry := s.log[s.lastApplied+1]
		s.metaStore.UpdateFile(ctx, entry.FileMetaData)
		s.lastApplied++
	}
	// fmt.Println("44444444444")
	return &AppendEntryOutput{ServerId: s.id, Term: s.term, Success: true, MatchedIndex: s.commitIndex}, nil
}

func (s *RaftSurfstore) SetLeader(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	state := s.initialJudge()
	if state[0] {
		return &Success{Flag: false}, ERR_SERVER_CRASHED
	}
	s.isLeaderMutex.Lock()
	defer s.isLeaderMutex.Unlock()
	s.isLeader = true
	s.term++

	s.nextIndex = make([]int64, len(s.peers))
	for i := range s.nextIndex {
		s.nextIndex[i] = s.commitIndex + 1
	}
	s.matchIndex = make([]int64, len(s.peers))
	for i := range s.matchIndex {
		s.matchIndex[i] = -1
	}
	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) sendToFollower_finite(ctx context.Context, idx int, addr string, responses chan bool, term int64) {
	fmt.Println("server id:", idx, " now is going to recieve sendtoFollower request")
	ni := s.nextIndex[idx]
	// fmt.Println("idx = ", idx, ", ni= ", ni)
	prevLogTerm := int64(-1)
	prevLogIndex := ni - 1
	if prevLogIndex >= 0 {
		prevLogTerm = s.log[prevLogIndex].Term
	}
	entry := s.log[ni:]
	// leaderCommit := s.commitIndex
	// endpoint := len(s.log)
	dummyAppendEntriesInput := AppendEntryInput{
		Term:         term,
		PrevLogTerm:  prevLogTerm,
		PrevLogIndex: prevLogIndex,
		Entries:      entry,
		LeaderCommit: s.commitIndex,
	}
	// fmt.Println("server id: ", idx, "prevlogterm:", prevLogTerm, "prevlogindex")
	// s.wholeLock.Unlock()
	conn, _ := grpc.Dial(addr, grpc.WithInsecure())
	client := NewRaftSurfstoreClient(conn)
	output, err := client.AppendEntries(ctx, &dummyAppendEntriesInput)
	// s.wholeLock.Lock()
	if output == nil {
		fmt.Println("server id = ", idx, "The output is nil.")
		responses <- false
		return
	}
	if err != nil {
		if err == context.Canceled {
			responses <- false
			return
		} else if err.Error() == "rpc error: code = Canceled desc = context canceled" {
			fmt.Println("It sounds works for rpc error")
			responses <- false
			return
		} else if err == ERR_SERVER_CRASHED {
			responses <- false
			return
		}
		// s.wholeLock.Unlock()
	}
	// fmt.Println("idx = ", idx, ", Probably here something went wrong.")
	if output.Term > s.term {
		fmt.Println("Server Become a follower.")
		responses <- false
		// s.wholeLock.Unlock()
	}
	if output.Success {
		s.nextIndex[idx] = ni + int64(len(entry))
		if s.nextIndex[idx] >= 0 {
			s.matchIndex[idx] = s.nextIndex[idx] - 1
		}
		responses <- true
	} else {
		fmt.Println("server id: ", idx, ", test2.")
		fmt.Println("Modified")
		// s.nextIndex[idx] = int64(math.Max(float64(0), float64(ni-1)))
		s.nextIndex[idx] = int64(math.Max(float64(0), float64(output.MatchedIndex+1)))
		// s.matchIndex[output.ServerId] = output.MatchedIndex
		fmt.Println("server id:", idx, ", there might be some problems with the indexes!")
		responses <- false
	}

}

func (s *RaftSurfstore) sendToFollower_Infinite(ctx context.Context, idx int, addr string, responses chan bool) {
	for {
		prevLogTerm := int64(-1)
		prevLogIndex := s.nextIndex[idx] - 1
		if prevLogIndex >= 0 {
			prevLogTerm = s.log[s.nextIndex[idx]-1].Term
		}
		dummyAppendEntriesInput := AppendEntryInput{
			Term:         s.term,
			PrevLogTerm:  prevLogTerm,
			PrevLogIndex: prevLogIndex,
			Entries:      s.log[prevLogIndex+1:],
			LeaderCommit: s.commitIndex,
		}
		// fmt.Println("TEST s.commitIndex: ", s.commitIndex)
		conn, _ := grpc.Dial(addr, grpc.WithInsecure())
		client := NewRaftSurfstoreClient(conn)
		output, err := client.AppendEntries(ctx, &dummyAppendEntriesInput)
		if output == nil || err != nil {
			// TODO: there may exist some problems
			if err == context.Canceled {
				break
			}
			if err.Error() == "rpc error: code = Canceled desc = context canceled" {
				fmt.Println("It sounds works for rpc error")
				return
			}
			if output == nil || err == ERR_SERVER_CRASHED {
				time.Sleep(50 * time.Millisecond)
				continue
			}
			fmt.Print(err)
			fmt.Println("Failed to use sendToFollower to connect to server")
			break
		}
		// fmt.Println(ctx)
		// fmt.Println("is there exist something ? ")
		// fmt.Println("the result is", output.Success)
		if output.Term > s.term {
			fmt.Println("Server Become a follower.")
			responses <- false
			break
		}
		if output.Success {
			// s.nextIndex[output.ServerId] = output.MatchedIndex + 1
			s.matchIndex[output.ServerId] = output.MatchedIndex
			responses <- true
			break
		} else {
			s.nextIndex[output.ServerId] = output.MatchedIndex + 1
			s.matchIndex[output.ServerId] = output.MatchedIndex
			fmt.Println("there might be some problems with the indexes!")
		}
	}
}

func (s *RaftSurfstore) sendAllFollowersInParllel_finite(ctx context.Context, commitChan chan bool) {
	// fmt.Println("go into sendAllFollowersInParllel_finite")
	fmt.Println("server id : ", s.id, " go intosendAllFollowersInParllel_finite function")
	temp_term := s.term
	responses := make(chan bool, len(s.peers)-1)
	for idx, addr := range s.peers {
		if int64(idx) == s.id {
			continue
		}
		go s.sendToFollower_finite(ctx, idx, addr, responses, int64(temp_term))
	}
	time.Sleep(50 * time.Millisecond)
	totalResponses := 1
	totalAppends := 1

	for {
		result := <-responses
		totalResponses++
		if result {
			totalAppends++
		}
		// if totalAppends > len(s.peers)/2 {
		// 	break
		// }
		if totalResponses == len(s.peers) {
			break
		}
	}
	if totalAppends > len(s.peers)/2 {
		commitChan <- true
		// s.commitIndex++
	} else {
		commitChan <- false
	}
}

func (s *RaftSurfstore) sendAllFollowersInParllel_Infinite(ctx context.Context, commitChan chan bool) {
	responses := make(chan bool, len(s.peers)-1)
	for idx, addr := range s.peers {
		if int64(idx) == s.id {
			continue
		}
		go s.sendToFollower_Infinite(ctx, idx, addr, responses)
	}
	time.Sleep(50 * time.Millisecond)
	totalResponses := 1
	totalAppends := 1

	for {
		result := <-responses
		totalResponses++
		if result {
			totalAppends++
		}
		if totalAppends > len(s.peers)/2 {
			break
		}
	}
	if totalAppends > len(s.peers)/2 {
		commitChan <- true
		// s.commitIndex++
	}
}

func (s *RaftSurfstore) SendHeartbeat(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	// panic("todo")
	// the implementation should be similiar to updatefile
	fmt.Println("server id : ", s.id, " go into SendHeartbeat function")
	state := s.initialJudge()
	if state[0] {
		return &Success{Flag: false}, ERR_SERVER_CRASHED
	}
	if !state[1] {
		return &Success{Flag: false}, ERR_NOT_LEADER
	}
	fmt.Println("server id : ", s.id, " remain in SendHeartbeat function")
	commitChan := make(chan bool)
	s.pendingCommits = append(s.pendingCommits, &commitChan)
	go s.sendAllFollowersInParllel_finite(ctx, commitChan)

	fmt.Println("point 1")
	commit := <-commitChan
	if commit {
		fmt.Println("At least here finished.")
		fmt.Println("1")
		return &Success{Flag: true}, nil
	} else {
		fmt.Println("At least here finished.")
		fmt.Println("2")
		return &Success{Flag: true}, fmt.Errorf("SendHearbeat function has some problems, althought the Success is true.")
	}
}

// ========== DO NOT MODIFY BELOW THIS LINE =====================================

func (s *RaftSurfstore) Crash(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = true
	s.isCrashedMutex.Unlock()
	fmt.Println("TEST crash")
	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) Restore(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = false
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) GetInternalState(ctx context.Context, empty *emptypb.Empty) (*RaftInternalState, error) {
	fileInfoMap, _ := s.metaStore.GetFileInfoMap(ctx, empty)
	s.isLeaderMutex.RLock()
	state := &RaftInternalState{
		IsLeader: s.isLeader,
		Term:     s.term,
		Log:      s.log,
		MetaMap:  fileInfoMap,
	}
	s.isLeaderMutex.RUnlock()

	return state, nil
}

var _ RaftSurfstoreInterface = new(RaftSurfstore)
