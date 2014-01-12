package filewatch

import (
	"syscall"
	"time"
)

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

type Stat struct {
	syscall.Stat_t
	Ctime time.Time
}
