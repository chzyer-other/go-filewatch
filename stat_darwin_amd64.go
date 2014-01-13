package filewatch

import (
	"syscall"
	"time"
)

func statEncode(s_stat *syscall.Stat_t) (stat *Stat) {
	ctime := time.Unix(s_stat.Birthtimespec.Sec, s_stat.Birthtimespec.Nsec)
	mtime := time.Unix(s_stat.Mtimespec.Sec, s_stat.Mtimespec.Nsec)
	return &Stat{
		Stat_t: s_stat,
		Ctime: ctime,
		Mtime: mtime,
	}
}
