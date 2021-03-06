package filewatch

import (
	"os"
	"time"
	"io/ioutil"
	"path/filepath"
	"encoding/binary"
)

type FileWatch struct {
	pathList []string
	fileList map[string] *File
	sincedb map[uint64] int64
	DiscoverInterval time.Duration
	fr *FileReader
	Interval time.Duration
	lastDiscoverTime time.Time
	pipe *Pipe
	SincedbPath string
}

func NewFileWatch(path []string, sincedbPath string) (fw *FileWatch) {
	pipe := new(Pipe)
	pipe.wWait.L = &pipe.l
	pipe.rWait.L = &pipe.l
	fw = &FileWatch {
		fileList: make(map[string] *File, 128),
		sincedb: make(map[uint64] int64, 128),
		fr: &FileReader{Pipe: pipe},
		pipe: pipe,
		Interval: 1,
		lastDiscoverTime: time.Now(),
		DiscoverInterval: 10,
		SincedbPath: sincedbPath,
	}
	fw.fr.fw = fw
	for _, p := range path {
		fw.register(p)
	}
	fw.restoreSincedb()
	go fw.discover()
	go fw.watch()
	go func() {
		fw.StoreSincedb()
		time.Sleep(15 * time.Second)
	}()
	return fw
}


func (fw *FileWatch) AddFileOffset(f *File, offset int) {
	f.AddOffset(offset)
	ino := f.Ino()
	fw.sincedb[ino] = f.Offset()
}

func (f *FileWatch) register(path ...string) {
	f.pathList = append(f.pathList, path...)
}

func (fw *FileWatch) watch() {
	for {
		isNew := false
		newStat := make(StatSlice, len(fw.fileList))
		length := 0
		for p, f := range fw.fileList {
			stat, err := getStat(p)
			if err != nil {
				// log.Println("delete file", p)
				fw.RemoveFile(p)
				continue
			}
			newStat[length] = StatList{f, stat, p}
			length++
		}
		newStat = newStat[:length]
		newStat.Sort()
		for _, stat := range newStat {
			isNew = fw.guessWhatChange(stat.Path, stat.File, stat.Stat) || isNew
		}
		if ! isNew {
			time.Sleep(fw.Interval * time.Second)
		}
	}
}

func (fw *FileWatch) discover() {
	for {
		for _, p := range fw.getFileList() {
			fw.onDiscoverFile(p)
		}
		fw.lastDiscoverTime = time.Now()
		time.Sleep(fw.DiscoverInterval*time.Second)
	}
}

func (f *FileWatch) getFileList() (pathes []string) {
	for _, p := range f.pathList {
		fps, err := filepath.Glob(p)
		if err != nil { continue }
		pathes = append(pathes, fps...)
	}
	return
}

func (fw *FileWatch) StoreSincedb() (err error) {
	buf := make([]byte, 16*len(fw.sincedb))
	idx := 0
	for k, v := range fw.sincedb {
		binary.BigEndian.PutUint64(buf[(idx*16): (idx*16)+8], k)
		binary.PutVarint(buf[(idx*16)+8: (idx*16)+16], v)
		idx ++
	}
	err = ioutil.WriteFile(fw.SincedbPath, buf, os.ModePerm)
	return
}

func (fw *FileWatch) restoreSincedb() {
	buf, err := ioutil.ReadFile(fw.SincedbPath)
	if err != nil { return }
	m := make(map[uint64] int64, len(buf)/8)
	for i:=0; i<len(buf)/16; i++ {
		k := binary.BigEndian.Uint64(buf[i*16: i*16+8])
		v, _ := binary.Varint(buf[i*16+8: (i+1)*16])
		m[k] = v
	}
	fw.sincedb = m
}

func (fw *FileWatch) notifyIncreate(f *File) (err error) {
	fw.pipe.wl.Lock()
	defer fw.pipe.wl.Unlock()

	fw.pipe.l.Lock()
	defer fw.pipe.l.Unlock()
	fw.fr.f = f
	fw.pipe.rWait.Signal()
	fw.pipe.wWait.Wait()
	if fw.fr.rErr != nil {
		err = fw.fr.rErr
		fw.fr.rErr = nil
	}
	return
}

func (fw *FileWatch) guessWhatChange(p string, f *File, pathStat *Stat) (isNew bool) {
	// the same filename with diff inode, file moved
	// log.Printf("guess %+v %+v %+v\n", p, f.Offset(), pathStat.Size)
	if f.Ino() != pathStat.Ino {
		fw.RemoveFile(p)
		fw.AddFile(p, 0)
		// log.Printf("%v, %+v, pathStat: %+v, old file move! reopen file\n", p, fw.fileList[p], pathStat)
		return true
	}
	offset := f.Offset()
	if offset == pathStat.Size {
		return
	}
	if offset > pathStat.Size {
		// file rewrite
		// log.Println("seek 0", offset,pathStat.Size)
		f.UpdateStat(pathStat)
		f.Seek(0)
		return true
	}

	// increate
	// log.Printf("%+v %+v", pathStat, f, f.CacheStat())
	f.UpdateStat(pathStat)
	err := fw.notifyIncreate(f)
	if err != nil {
		return false
	}
	return true
}

func (fw *FileWatch) RemoveFile(p string) {
	f, ok := fw.fileList[p]
	if ok {
		defer f.Close()
		ino := f.Ino()
		delete(fw.sincedb, ino)
	}
	delete(fw.fileList, p)
}

func (fw *FileWatch) AddFile(p string, offset int64) {
	autoChooseOffset := offset < 0
	if offset < 0 {
		offset = 0
	}
	f, err := NewFile(p, offset)
	if err != nil { return }
	fw.fileList[p] = f
	if ! autoChooseOffset {
		fw.sincedb[f.Ino()] = offset
		return
	}
	if f.CreateBefore(fw.lastDiscoverTime) {
		// new file
		foffset, ok := fw.sincedb[f.Ino()]
		if ok {
			// log.Printf("%+v %+v", foffset, f)
			f.Seek(foffset)
			return
		}
		// log.Println("addFile: old file and not in sincedb", fw.lastDiscoverTime, f.stat.Ctime)
		fw.sincedb[f.Ino()] = f.CacheStat().Size
		f.Seek(fw.sincedb[f.Ino()])
		// log.Printf("addFile: %+v", f)
		return
	}
	// old file
	// if exist in sincedb, move to the position
	foffset, _ := fw.sincedb[f.Ino()]
	f.Seek(foffset)
	fw.sincedb[f.Ino()] = foffset
}

func (fw *FileWatch) onDiscoverFile(p string) (isNew bool) {
	if _, ok := fw.fileList[p]; ok { return }
	fw.AddFile(p, -1)
	return true
}

func (f *FileWatch) Subtitute() *FileReader {
	return f.fr
}
