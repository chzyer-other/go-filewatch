package filewatch

import (
	"time"
	"sort"
	"syscall"
)

type Stat struct {
	*syscall.Stat_t
	Ctime time.Time
	Mtime time.Time
}

type StatList struct {
	File *File
	Stat *Stat
	Path string
}

type StatSlice []StatList

func (p StatSlice) Len() int { return len(p) }
func (p StatSlice) Less(i, j int) bool { return p[i].Stat.Mtime.Before(p[j].Stat.Mtime) }
func (p StatSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

// Sort is a convenience method.
func (p StatSlice) Sort() { sort.Sort(p) }

func fstat(fd uintptr) (stat *Stat, err error) {
	var s_stat syscall.Stat_t
	err = syscall.Fstat(int(fd), &s_stat)

	stat = statEncode(&s_stat)
	return
}

func getStat(name string) (stat *Stat, err error) {
	var s_stat syscall.Stat_t
	err = syscall.Stat(name, &s_stat)
	stat = statEncode(&s_stat)
	return
}
