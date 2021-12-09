package gonebula

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/golang/glog"
)

// walPath := "/Users/wenlinwu/src/toss_integration/data/store1/nebula/1/wal/1"

type walIndexPair struct {
	walFile string
	index   int
}

type LogEntry struct {
	Index     int64
	Term      uint64
	MsgSz     uint32
	ClusterID uint64
	Msg       []byte
	Foot      uint32
}

func IndexFromWalFile(wf string) (int, error) {
	wf = path.Base(wf)
	if !strings.HasSuffix(wf, ".wal") {
		return 0, fmt.Errorf("wal file %s does not ends with .wal", wf)
	}

	idxStr := strings.Replace(wf, ".wal", "", 1)
	idx, err := strconv.Atoi(idxStr)
	if err != nil {
		return 0, fmt.Errorf("failed extracting index form wal file %s: %+v", wf, err)
	}

	return idx, nil
}

func ParseSingleWalFile(wf string) ([]LogEntry, error) {
	logs := []LogEntry{}
	firstIndex, err := IndexFromWalFile(wf)
	if err != nil {
		return nil, err
	}

	glog.Infof("parsing wal file: %s", wf)
	f, err := os.Open(wf)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	bf := bufio.NewReader(f)
	first := true
	for {
		log := LogEntry{}

		// parse index
		err := binary.Read(bf, binary.LittleEndian, &log.Index)
		if err == io.EOF {
			break
		}

		if err != nil {
			return nil, fmt.Errorf("failed parsing wal %s: %+v", wf, err)
		}

		// parse term
		err = binary.Read(bf, binary.LittleEndian, &log.Term)
		if err != nil {
			return nil, fmt.Errorf("failed parsing wal %s: %+v", wf, err)
		}

		// parse msg size
		if err := binary.Read(bf, binary.LittleEndian, &log.MsgSz); err != nil {
			return nil, fmt.Errorf("failed parsing wal %s: %+v", wf, err)
		}

		// parse cluster id
		if err := binary.Read(bf, binary.LittleEndian, &log.ClusterID); err != nil {
			return nil, fmt.Errorf("failed parsing wal %s: %+v", wf, err)
		}

		// parse msg
		log.Msg = make([]byte, log.MsgSz)
		if log.MsgSz > 0 {
			if err := binary.Read(bf, binary.LittleEndian, &log.Msg); err != nil {
				return nil, fmt.Errorf("failed parsing wal %s: %+v", wf, err)
			}
		}

		// parse foot
		if err := binary.Read(bf, binary.LittleEndian, &log.Foot); err != nil {
			return nil, fmt.Errorf("failed parsing wal %s: %+v", wf, err)
		}

		// verify log: firstLog.index == index(walFile)
		if first {
			first = false
			if log.Index != int64(firstIndex) {
				return nil, fmt.Errorf("illegal wal file %s, first index %d mismatch", wf, log.Index)
			}
		}
		logs = append(logs, log)
		glog.V(2).Infof("%+v", log)
		// fmt.Printf("%+v\n", log)
	}

	return logs, nil

}

func ParseSingleWalDir(walDir string, outFile string) error {
	of, err := os.Create(outFile)
	if err != nil {
		return fmt.Errorf("failed opening out file %s: %+v", outFile, err)
	}
	defer func() {
		glog.Info("closing wal parsing output file")
		of.Close()
	}()

	glog.Infof("parsing wal in dir: %s", walDir)
	files, err := ioutil.ReadDir(walDir)
	if err != nil {
		return err
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

	prevEntries := []LogEntry{}
	for _, walIdx := range walList {
		entries, err := ParseSingleWalFile(walIdx.walFile)
		if err != nil {
			return fmt.Errorf("failed parsing wal: %+v", err)
		}

		if len(entries) > 0 {
			prevEntries = truncateFrom(prevEntries, entries[0].Index)
			if err := writeEntries(of, prevEntries); err != nil {
				return err
			}

			prevEntries = entries
		}
	}

	// don't forget to append the rest
	if err := writeEntries(of, prevEntries); err != nil {
		return err
	}

	return nil
}

func writeEntries(of *os.File, entries []LogEntry) error {
	glog.Infof("writing %d entries", len(entries))
	bof := bufio.NewWriter(of)
	for _, e := range entries {
		_, err := bof.WriteString(fmt.Sprintf("index: %d, term: %d, logsz: %d\n", e.Index, e.Term, e.MsgSz))
		if err != nil {
			return err
		}
	}
	bof.Flush()

	return nil
}

func truncateFrom(entries []LogEntry, index int64) []LogEntry {
	if len(entries) == 0 {
		return entries
	}

	pos := 0
	for pos < len(entries) {
		if entries[pos].Index == index {
			break
		}

		pos++
	}

	return entries[:pos]
}

func ParseWal(walDirs []string, outDir string) error {
	glog.Infof("parsing wal dirs: %+v", walDirs)
	var wg sync.WaitGroup
	for i, wd := range walDirs {
		wg.Add(1)
		go func(idx int, walDir string) {
			outFile := fmt.Sprintf("%d.outwal", idx)
			if outDir != "" {
				outFile = path.Join(outDir, outFile)
			}

			err := ParseSingleWalDir(walDir, outFile)
			if err != nil {
				glog.Errorf("failed parsing wal %s: %+v", walDir, err)
			}

			wg.Done()
		}(i, wd)
	}

	wg.Wait()

	return nil
}
