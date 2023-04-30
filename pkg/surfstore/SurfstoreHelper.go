package surfstore

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"path/filepath"

	_ "github.com/mattn/go-sqlite3"
)

/* Hash Related */
func GetBlockHashBytes(blockData []byte) []byte {
	h := sha256.New()
	h.Write(blockData)
	return h.Sum(nil)
}

func GetBlockHashString(blockData []byte) string {
	blockHash := GetBlockHashBytes(blockData)
	return hex.EncodeToString(blockHash)
}

/* File Path Related */
func ConcatPath(baseDir, fileDir string) string {
	return baseDir + "/" + fileDir
}

/*
	Writing Local Metadata File Related
*/

const createTable string = `create table if not exists indexes (
		fileName TEXT, 
		version INT,
		hashIndex INT,
		hashValue TEXT
	);`

const insertTuple string = `insert into indexes (fileName, version, hashIndex, hashValue) VALUES (?,?,?,?);`

// TODO: the client baseDir is dataA/index.db, no pwd
// TODO: This function has already been tested
// WriteMetaFile writes the file meta map back to local metadata file index.db
func WriteMetaFile(fileMetas map[string]*FileMetaData, baseDir string) error {
	// remove index.db file if it exists
	outputMetaPath := ConcatPath(baseDir, DEFAULT_META_FILENAME)
	fmt.Println(outputMetaPath)
	if _, err := os.Stat(outputMetaPath); err == nil {
		e := os.Remove(outputMetaPath)
		if e != nil {
			log.Fatal("Error During Meta Write Back")
		}
	}
	db, err := sql.Open("sqlite3", outputMetaPath)
	if err != nil {
		log.Fatal("Error During Meta Write Back")
	}
	statement, err := db.Prepare(createTable)
	if err != nil {
		log.Fatal("Error During Meta Write Back")
	}
	statement.Exec()

	for fileName, metaData := range fileMetas {
		for idx, hashValue := range metaData.BlockHashList {
			statement, err = db.Prepare(insertTuple)
			statement.Exec(fileName, metaData.Version, idx, hashValue)
		}
	}
	return nil
}

/*
Reading Local Metadata File Related
*/

// const createTable string = `create table if not exists indexes (
//
//	fileName TEXT,
//	version INT,
//	hashIndex INT,
//	hashValue TEXT
//
// );`
// select department, code from courses where department = ? order by code;
const getDistinctFileName string = `select distinct filename, version from indexes;`

const getTuplesByFileName string = `select fileName, version, hashIndex, hashValue from indexes where fileName = ? order by hashIndex`

// LoadMetaFromMetaFile loads the local metadata file into a file meta map.
// The key is the file's name and the value is the file's metadata.
// You can use this function to load the index.db file in this project.
func LoadMetaFromMetaFile(baseDir string) (fileMetaMap map[string]*FileMetaData, e error) {
	metaFilePath, _ := filepath.Abs(ConcatPath(baseDir, DEFAULT_META_FILENAME))
	fileMetaMap = make(map[string]*FileMetaData)
	metaFileStats, e := os.Stat(metaFilePath)
	if e != nil || metaFileStats.IsDir() {
		return fileMetaMap, nil
	}
	db, err := sql.Open("sqlite3", metaFilePath)
	if err != nil {
		log.Fatal("Error When Opening Meta")
	}
	// panic("todo")
	fileMetaMap = make(map[string]*FileMetaData, 0)
	files, err := db.Query(getDistinctFileName)
	for files.Next() {
		var file string
		var ver int
		files.Scan(&file, &ver)
		// file_hashlist := make([]string, 0)
		temp := &FileMetaData{Filename: file, Version: int32(ver)}

		rows, _ := db.Query(getTuplesByFileName, file)
		file_hashlist := make([]string, 0)
		for rows.Next() {
			var filename string
			var v int
			var hashindex int
			var hashvalue string
			rows.Scan(&filename, &v, &hashindex, &hashvalue)
			// fmt.Println(filename + " " + strconv.Itoa(v) + " " + strconv.Itoa(hashindex) + " " + hashvalue)
			file_hashlist = append(file_hashlist, hashvalue)
		}
		temp.BlockHashList = file_hashlist
		fileMetaMap[file] = temp
	}

	return fileMetaMap, nil
}

/*
	Debugging Related
*/

// PrintMetaMap prints the contents of the metadata map.
// You might find this function useful for debugging.
func PrintMetaMap(metaMap map[string]*FileMetaData) {

	fmt.Println("--------BEGIN PRINT MAP--------")

	for _, filemeta := range metaMap {
		fmt.Println("\t", filemeta.Filename, filemeta.Version)
		for _, blockHash := range filemeta.BlockHashList {
			fmt.Println("\t", blockHash)
		}
	}

	fmt.Println("---------END PRINT MAP--------")

}
