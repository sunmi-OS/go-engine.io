package engineio

import (
	"io"
	"sync"

	"github.com/sunmi-OS/go-engine.io/parser"
)

type connReader struct {
	*parser.PacketDecoder
	closeChan chan struct{}
}

func newConnReader(d *parser.PacketDecoder, closeChan chan struct{}) *connReader {
	return &connReader{
		PacketDecoder: d,
		closeChan:     closeChan,
	}
}

func (r *connReader) Close() error {
	if r.closeChan == nil {
		return nil
	}

	// // 使用select来安全地发送数据，避免向已关闭的channel发送
	// select {
	// case r.closeChan <- struct{}{}:
	// 	// 成功发送
	// case <-r.closeChan:
	// 	// channel已关闭，不需要发送
	// default:
	// 	// channel已满或已关闭，忽略
	// }

	r.closeChan = nil
	return nil
}

type connWriter struct {
	io.WriteCloser
	locker *sync.Mutex
}

func newConnWriter(w io.WriteCloser, locker *sync.Mutex) *connWriter {
	return &connWriter{
		WriteCloser: w,
		locker:      locker,
	}
}

func (w *connWriter) Close() error {
	defer func() {
		if w.locker != nil {
			w.locker.Unlock()
			w.locker = nil
		}
	}()
	return w.WriteCloser.Close()
}
