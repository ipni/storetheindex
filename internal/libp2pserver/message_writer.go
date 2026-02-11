package libp2pserver

import (
	"bufio"
	"errors"
	"io"
	"sync"

	"github.com/libp2p/go-msgio/pbio"
	"google.golang.org/protobuf/proto"
)

// The Protobuf writer performs multiple small writes when writing a message.
// We need to buffer those writes, to make sure that we're not sending a new
// packet for every single write.
type bufferedDelimitedWriter struct {
	*bufio.Writer
	pbio.WriteCloser
}

var writerPool = sync.Pool{
	New: func() any {
		w := bufio.NewWriter(nil)
		return &bufferedDelimitedWriter{
			Writer:      w,
			WriteCloser: pbio.NewDelimitedWriter(w),
		}
	},
}

// writeMsg handles sending messages through the wire.
func writeMsg(w io.Writer, msg proto.Message) error {
	if w == nil {
		return errors.New("io writer is nil")
	}
	if msg == nil {
		return errors.New("msg is nil")
	}
	bw := writerPool.Get().(*bufferedDelimitedWriter)
	bw.Reset(w)
	err := bw.WriteMsg(msg)
	if err == nil {
		err = bw.Flush()
	}
	bw.Reset(nil)
	writerPool.Put(bw)
	return err
}
