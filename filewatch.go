package main

import (
	"os/signal"
	"sync"
	"path/filepath"
	"io/ioutil"
	"bufio"
	"time"
	"encoding/binary"
	"os"
	"log"
	"syscall"
)

type Stat struct {
	syscall.Stat_t
	Ctime time.Time
}

type File struct {
	*os.File
	stat *Stat
	buf *bufio.Reader
	offset int64
	Path string
}

func NewFile(p string, defOffset int64) (f *File, err error) {
	of, err := os.Open(p)
	if err != nil { return }
	stat, err := fstat(of.Fd())
	if err != nil { return }
	offset := defOffset
	if offset < 0 {
		offset = stat.Size
	}
	f = &File {
		File: of,
		stat: stat,
		offset: stat.Size,
		Path: p,
	}
	f.buf = bufio.NewReader(f)
	return
}

func (f *File) ReadLine() (path string, b []byte, err error) {
	b, _, err = f.buf.ReadLine()
	if err != nil { return }
	path = f.Path
	return
}

func (f *File) Ino() (s uint64) {
	return f.stat.Ino
}

func (f *File) CacheStat() (s *Stat) {
	return f.stat
}

func (f *File) AddOffset(offset int) {
	f.offset += int64(offset)
}

func (f *File) Read(buf []byte) (n int, err error) {
	n, err = f.ReadAt(buf, f.offset)
	return
}

func (f *File) Seek(offset int64) {
	// log.Println("seek", f.Path, "to", offset)
	f.offset = offset
}

func (f *File) Offset() int64 {
	return f.offset
}

func (f *File) UpdateStat(s *Stat) {
	f.stat = s
}

func (f *File) CreateBefore(t time.Time) bool {
	return f.stat.Ctime.Before(t)
}

// ----------------------------------------------------------------------------

type LogInfo struct {
	Msg string
	Path string
}

type Pipe struct {
	rl, wl, l sync.Mutex
	wWait sync.Cond
	rWait sync.Cond
}

type FileReader struct {
	*Pipe
	fw *FileWatch
	f *File
}

func (fr *FileReader) ReadLine() (path string, b []byte, err error) {
	fr.rl.Lock()
	defer fr.rl.Unlock()

	fr.l.Lock()
	defer fr.l.Unlock()
	if fr.f == nil {
		fr.rWait.Wait()
	}
	for {
		path, b, err = fr.f.ReadLine()
		if err != nil {
			fr.wWait.Signal()
			fr.rWait.Wait()
			continue
		}
		break
	}
	path = fr.f.Path
	// readline eat \n
	fr.fw.AddFileOffset(fr.f, len(b)+1)
	return
}

// ----------------------------------------------------------------------------

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

func NewFileWatch(path string, sincedbPath string) (fw *FileWatch) {
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
	fw.register(path)
	fw.restoreSincedb()
	go fw.watch()
	go fw.interval()
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

func (fw *FileWatch) interval() {
	for {
		for p, f := range fw.fileList {
			stat, err := getStat(p)
			if err != nil {
				// log.Println("delete file", p)
				fw.RemoveFile(p)
				continue
			}
			fw.guessWhatChange(p, f, stat)
		}
		time.Sleep(fw.Interval * time.Second)
	}
}

func (fw *FileWatch) watch() {
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

func (fw *FileWatch) notifyIncreate(f *File) {
	fw.pipe.wl.Lock()
	defer fw.pipe.wl.Unlock()

	fw.pipe.l.Lock()
	defer fw.pipe.l.Unlock()
	fw.fr.f = f
	fw.pipe.rWait.Signal()
	fw.pipe.wWait.Wait()
}

func (fw *FileWatch) guessWhatChange(p string, f *File, pathStat *Stat) {
	// the same filename with diff inode, file moved
	if f.Ino() != pathStat.Ino {
		fw.RemoveFile(p)
		fw.AddFile(p, 0)
		// log.Printf("%v, %+v, pathStat: %+v, old file move! reopen file\n", p, fw.fileList[p], pathStat)
		return
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
		return
	}

	// log.Println("increase")
	// increate
	// log.Printf("%+v %+v", pathStat, f, f.CacheStat())
	f.UpdateStat(pathStat)
	fw.notifyIncreate(f)
	return
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
	defer func() {
		
	}()
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

func (fw *FileWatch) onDiscoverFile(p string) {
	f, ok := fw.fileList[p]
	if ok {
		pathStat, err := getStat(p)
		if err != nil {
			// file not exists
			fw.RemoveFile(p)
			// log.Println("file delete", p)
			return
		}
		fw.guessWhatChange(p, f, pathStat)
		return
	}
	// log.Println("file found", p)
	fw.AddFile(p, -1)
}

func fstat(fd uintptr) (stat *Stat, err error) {
	var s_stat syscall.Stat_t
	err = syscall.Fstat(int(fd), &s_stat)

	ctime := time.Unix(s_stat.Birthtimespec.Sec, s_stat.Birthtimespec.Nsec)
	stat = &Stat{s_stat, ctime}
	return
}

func getStat(name string) (stat *Stat, err error) {
	var s_stat syscall.Stat_t
	err = syscall.Stat(name, &s_stat)
	ctime := time.Unix(s_stat.Birthtimespec.Sec, s_stat.Birthtimespec.Nsec)
	stat = &Stat{s_stat, ctime}
	return
}

func (f *FileWatch) Subtitute() *FileReader {
	return f.fr
}

func catchKillSignal(fw *FileWatch) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	signal.Notify(c, os.Kill)
	go func(){
		<- c
		fw.StoreSincedb()
		os.Exit(2)
	}()
}

func main() {
	watchPath := "/Users/cheney/Projects/logstash/*.log"
	sincedbPath := "/Users/cheney/Projects/logstash/sincedb"
	f := NewFileWatch(watchPath, sincedbPath)
	catchKillSignal(f)
	fr := f.Subtitute()
	for {
		path, b, err := fr.ReadLine()
		if err != nil { return }
		log.Println(path, string(b))
	}
}

