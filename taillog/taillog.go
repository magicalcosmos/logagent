package taillog

import (
	"context"
	"fmt"

	"github.com/hpcloud/tail"
	"kakfa.com/kafka"
)

// 专门从日志文件搜集日志模块
var (
	tailObj *tail.Tail
)

// TailTask tail task
type TailTask struct {
	path     string
	topic    string
	instance *tail.Tail
	// 为了实现退出t.run
	ctx        context.Context
	cancelFunc context.CancelFunc
}

// NewTailTask new tail task
func NewTailTask(path, topic string) (tailTask *TailTask) {
	ctx, cancel := context.WithCancel(context.Background())
	tailTask = &TailTask{
		path:       path,
		topic:      topic,
		ctx:        ctx,
		cancelFunc: cancel,
	}
	tailTask.init()
	return
}

// Init init
func (t *TailTask) init() {
	config := tail.Config{
		ReOpen:    true,
		Follow:    true,
		Location:  &tail.SeekInfo{Offset: 0, Whence: 2},
		MustExist: false,
		Poll:      false,
	}
	tails, err := tail.TailFile(t.path, config)
	t.instance = tails
	if err != nil {
		fmt.Printf("tail %s failed, err: %v \n", t.path, err)
		return
	}
	go t.run()
}

// Init init
// func Init(filename string) (err error) {
// 	config := tail.Config{
// 		ReOpen:    true,
// 		Follow:    true,
// 		Location:  &tail.SeekInfo{Offset: 0, Whence: 2},
// 		MustExist: false,
// 		Poll:      false,
// 	}
// 	tailObj, err = tail.TailFile(filename, config)
// 	if err != nil {
// 		fmt.Printf("tail %s failed, err: %v \n", filename, err)
// 		return
// 	}
// 	return
// }

// ReadChan read log
func (t *TailTask) ReadChan() <-chan *tail.Line {
	return t.instance.Lines
	// var (
	// 	msg *tail.Line
	// 	ok  bool
	// )
	// for {
	// 	msg, ok = <-tailObj.Lines
	// 	if !ok {
	// 		fmt.Printf("tail file close reopen, filename: %s\n", tailObj.Filename)
	// 		time.Sleep(time.Second)
	// 		continue
	// 	}
	// 	fmt.Println("msg:", msg.Text)
	// }
}

func (t *TailTask) run() {
	for {
		select {
		case <-t.ctx.Done():
			fmt.Printf("tail task: %s_%s is over.", t.path, t.topic)
			return
		case line := <-t.ReadChan():
			kafka.SendToChan(t.topic, line.Text)
			// kafka.SendToKafka(t.topic, line.Text)
		}
	}
}

// // TailLog tail log
// func TailLog() {
// 	filename := `xx.log`
// 	config := tail.Config{
// 		ReOpen:    true,
// 		Follow:    true,
// 		Location:  &tail.SeekInfo{Offset: 0, Whence: 2},
// 		MustExist: false,
// 		Poll:      false,
// 	}
// 	tails, err := tail.TailFile(filename, config)
// 	if err != nil {
// 		fmt.Printf("tail %s failed, err: %v \n", filename, err)
// 		return
// 	}
// 	var (
// 		msg *tail.Line
// 		ok  bool
// 	)
// 	for {
// 		msg, ok = <-tails.Lines
// 		if !ok {
// 			fmt.Printf("tail file close reopen, filename: %s\n", tails.Filename)
// 			time.Sleep(time.Second)
// 			continue
// 		}
// 		fmt.Println("msg:", msg.Text)
// 	}
// }
