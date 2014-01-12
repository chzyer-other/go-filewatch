package filewatch

import (
	"sync"
)

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
