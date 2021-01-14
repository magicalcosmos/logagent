package main

import (
	"fmt"
	"sync"
	"time"

	"gopkg.in/ini.v1"
	"kakfa.com/conf"
	"kakfa.com/etcd"
	"kakfa.com/kafka"
	"kakfa.com/taillog"
	"kakfa.com/utils"
)

var (
	cfg = new(conf.AppConf)
)

func main() {
	// 0 加载配置文件
	// cfg, err := ini.Load("./conf/config.ini")
	// if err != nil {
	// 	fmt.Printf("Failed to read file: %v", err)
	// 	return
	// }
	err := ini.MapTo(cfg, "./conf/config.ini")
	if err != nil {
		fmt.Printf("Failed to load config: %v \n", err)
		return
	}
	// 1. 初始化kafka连接
	err = kafka.Init([]string{cfg.KafkaConf.Address}, cfg.KafkaConf.ChanMaxSize)
	if err != nil {
		fmt.Printf("init kafka failed, error: %v \n", err)
		return
	}
	fmt.Println("Init kafka succeed")
	// 2.初始化etcd
	err = etcd.Init(cfg.EtcdConf.Address, time.Duration(cfg.EtcdConf.Timeout)*time.Second)
	if err != nil {
		fmt.Printf("init etcd failed, error: %v \n", err)
		return
	}
	fmt.Println("Init etcd succeed")
	ip, err := utils.GetOutBoundIP()
	if err != nil {
		fmt.Printf("get ip failed, err: %v\n", err)
	}
	etcdConfKey := fmt.Sprintf(cfg.EtcdConf.Key, ip)
	// 2.1 从etcd中获取日志收集项的配置信息
	logEntryConf, err := etcd.GetConf(etcdConfKey)
	if err != nil {
		fmt.Printf("get conf failed, err: %v\n", err)
	}
	fmt.Printf("get conf from etcd succeed, %v\n", logEntryConf)

	// 3 收集日志发往kafka
	taillog.Init(logEntryConf)

	// 2.2 派一个哨兵去日志项的变化，及时通知logagent, 实现热加载
	newConfChan := taillog.NewConfChan()
	var wg sync.WaitGroup
	wg.Add(1)
	go etcd.WatchConf(etcdConfKey, newConfChan)
	wg.Wait()
}

func run() {
	// 1. 读日志
	// for {
	// 	select {
	// 	case line := <-taillog.ReadChan():
	// 		// 2. 发送到kafka
	// 		kafka.SendToKafka(cfg.KafkaConf.Topic, line.Text)
	// 	default:
	// 		time.Sleep(time.Second)
	// 	}
	// }

}
