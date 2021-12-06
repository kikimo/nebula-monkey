package gonebula

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"

	"github.com/golang/glog"
)

// walPath := "/Users/wenlinwu/src/toss_integration/data/store1/nebula/1/wal/1"

type walIndexPair struct {
	walFile string
	index   int
}

type LogEntry struct {
	Term      uint64
	MsgSz     uint32
	ClusterID uint64
	Msg       []byte
	Foot      uint32
}

func ParseSingleWal(walDir string) ([]LogEntry, error) {
	glog.Infof("parsing wal %s", walDir)
	files, err := ioutil.ReadDir(walDir)
	if err != nil {
		return nil, err
	}

	walList := []walIndexPair{}
	for _, f := range files {
		if strings.HasSuffix(f.Name(), ".wal") {
			index, _ := strconv.Atoi(strings.Replace(f.Name(), ".wal", "", 1))
			walList = append(walList, walIndexPair{
				walFile: path.Join(walDir, f.Name()),
				index:   index,
			})
		}
	}

	sort.Slice(walList, func(i, j int) bool {
		return walList[i].index < walList[j].index
	})

	logs := []LogEntry{}
	for _, walIdx := range walList {
		fmt.Printf("%+v\n", walIdx)
		glog.Infof("parsing wal file: %s", walIdx.walFile)
		f, err := os.Open(walIdx.walFile)
		if err != nil {
			return nil, err
		}
		defer f.Close()

		bf := bufio.NewReader(f)
		log := LogEntry{
			// Term:      term,
			// MsgSz:     msgSz,
			// ClusterID: clusterID,
			// Msg:       msg,
			// Foot:      foot,
		}

		// parse term
		if err := binary.Read(bf, binary.LittleEndian, &log.Term); err != nil {
			return nil, err
		}

		if err := binary.Read(bf, binary.LittleEndian, &log.Msg); err != nil {
			return nil, err
		}

		if err := binary.Read(bf, binary.LittleEndian, &log.ClusterID); err != nil {
			return nil, err
		}

		log.Msg = make([]byte, log.MsgSz)
		if log.MsgSz > 0 {
			if err := binary.Read(bf, binary.LittleEndian, &log.Msg); err != nil {
				return nil, err
			}
		}

		if err := binary.Read(bf, binary.LittleEndian, &log.Foot); err != nil {
			return nil, err
		}

		logs = append(logs, log)
		fmt.Printf("%+v\n", log)
	}

	return logs, nil
}

func ParseWal(walDirs []string, outDir string) error {
	glog.Infof("parsing wal dirs: %+v", walDirs)
	for i, wd := range walDirs {
		outFile := fmt.Sprintf("outwal.%d", i)
		if outDir != "" {
			outFile = path.Join(outDir, outFile)
		}

		logs, err := ParseSingleWal(wd)
		if err != nil {
			return err
		}

		if len(logs) > 0 {
			fmt.Printf("log: %+v\n", logs[0])
		}
	}

	return nil
}
