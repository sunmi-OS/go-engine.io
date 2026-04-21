package engineio

import (
	"io"
	"sync"

	"github.com/sunmi-OS/go-engine.io/parser"
)

type connReader struct {
	*parser.PacketDecoder
	closeChan chan struct{}
	notify    sync.Once
}

func newConnReader(d *parser.PacketDecoder, closeChan chan struct{}) *connReader {
	return &connReader{
		PacketDecoder: d,
		closeChan:     closeChan,
	}
}

func (r *connReader) Close() error {
	if r == nil || r.closeChan == nil {
		return nil
	}

	// 只通知一次。closeChan 必须由创建方使用带缓冲 channel 且不得 close，
	// 否则在 OnPacket 超时与 socket 侧 decoder.Close 竞态下会出现 send on closed channel。
	ch := r.closeChan
	r.notify.Do(func() {
		select {
		case ch <- struct{}{}:
		default:
			// 带缓冲(1)且已有信号：忽略
		}
	})
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
