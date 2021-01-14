package conf

// AppConf app conf
type AppConf struct {
	KafkaConf `ini:"kafka"`
	EtcdConf  `ini:"etcd"`
}

// KafkaConf ka
type KafkaConf struct {
	Address     string `ini:"address"`
	ChanMaxSize int    `ini:"chan_max_size"`
}

// EtcdConf etcd
type EtcdConf struct {
	Address string `ini:"address"`
	Key     string `ini:"collect_log_key"`
	Timeout int    `ini:"timeout"`
}

// TailLogConf ta
type TailLogConf struct {
	FileName string `ini:"filename"`
}
