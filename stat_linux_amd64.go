package filewatch

import (
	"syscall"
	"time"
)

func statEncode(s_stat *syscall.Stat_t) (stat *Stat) {
	ctime := time.Unix(s_stat.Ctim.Sec, s_stat.Ctim.Nsec)
	mtime := time.Unix(s_stat.Mtim.Sec, s_stat.Mtim.Nsec)
	return &Stat{
		Stat_t: s_stat,
		Ctime: ctime,
		Mtime: mtime,
	}
}
