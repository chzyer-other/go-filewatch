package filewatch

import (
	"os"
	"time"
	"bufio"
)

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
