package taillog

import (
	"fmt"
	"time"

	"kakfa.com/etcd"
)

var taskMgr *tailLogMgr

type tailLogMgr struct {
	logEntry    []*etcd.LogEntry
	taskMap     map[string]*TailTask
	newConfChan chan []*etcd.LogEntry
}

// Init init
func Init(logEntryConf []*etcd.LogEntry) {
	taskMgr = &tailLogMgr{
		logEntry:    logEntryConf,
		taskMap:     make(map[string]*TailTask, 32),
		newConfChan: make(chan []*etcd.LogEntry), // 无缓冲区通道
	}
	for _, logEntry := range logEntryConf {
		task := NewTailTask(logEntry.Path, logEntry.Topic)
		mk := fmt.Sprintf("%s_%s", logEntry.Path, logEntry.Topic)
		taskMgr.taskMap[mk] = task
	}
	go taskMgr.run()
}

// listen newConfChan, add or remove config
func (t *tailLogMgr) run() {
	for {
		select {
		case newConf := <-t.newConfChan:
			for _, conf := range newConf {
				mk := fmt.Sprintf("%s_%s", conf.Path, conf.Topic)
				if _, ok := t.taskMap[mk]; ok {
					continue
				} else {
					task := NewTailTask(conf.Path, conf.Topic)
					taskMgr.taskMap[mk] = task
				}
			}
			for _, c1 := range t.logEntry {
				isDelete := true
				for _, c2 := range newConf {
					if c1.Path == c2.Path && c1.Topic == c2.Topic {
						isDelete = false
						continue
					}
				}
				if isDelete {
					mk := fmt.Sprintf("%s_%s", c1.Path, c1.Topic)
					t.taskMap[mk].cancelFunc()
				}
			}
			fmt.Println(newConf)
		default:
			time.Sleep(time.Second)
		}
	}
}

// NewConfChan 向外暴露一个函数
func NewConfChan() chan<- []*etcd.LogEntry {
	return taskMgr.newConfChan
}
