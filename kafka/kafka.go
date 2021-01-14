package kafka

import (
	"fmt"
	"time"

	"github.com/Shopify/sarama"
)

type logData struct {
	topic string
	data  string
}

var (
	client      sarama.SyncProducer
	logDataChan chan *logData
)

// Init init
func Init(addrs []string, maxSize int) (err error) {
	// 1. 生产者配置
	config := sarama.NewConfig()
	// 发送数据leader与follower都需要确认 （ACK）
	config.Producer.RequiredAcks = sarama.WaitForAll
	// 新选出一个Partition
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	// 确认
	config.Producer.Return.Successes = true

	// 2.连接kafka
	client, err = sarama.NewSyncProducer(addrs, config)

	if err != nil {
		fmt.Println("Producer closed, err: ", err)
		return
	}
	logDataChan = make(chan *logData, maxSize)
	go sendToKafka()
	return
}

// SendToKafka send to kafka
func sendToKafka() {
	for {
		select {
		case log := <-logDataChan:
			msg := &sarama.ProducerMessage{}
			msg.Topic = log.topic
			msg.Value = sarama.StringEncoder(log.data)
			pid, offset, err := client.SendMessage(msg)
			if err != nil {
				fmt.Println("Send message failed, err: ", err)
				return
			}
			fmt.Printf("pid: %v offset: %v\n", pid, offset)
		default:
			time.Sleep(time.Microsecond * 50)
		}
	}

}

// func SendToKafka(topic, data string) {
// 	msg := &sarama.ProducerMessage{}
// 	msg.Topic = topic
// 	msg.Value = sarama.StringEncoder(data)
// 	pid, offset, err := client.SendMessage(msg)
// 	if err != nil {
// 		fmt.Println("Send message failed, err: ", err)
// 		return
// 	}
// 	fmt.Printf("pid: %v offset: %v\n", pid, offset)
// }

// SendToChan send to chan
func SendToChan(topic, data string) {
	msg := &logData{
		topic: topic,
		data:  data,
	}
	logDataChan <- msg
}

// // Inits test
// func Inits() {
// 	// 1. 生产者配置
// 	config := sarama.NewConfig()
// 	// 发送数据leader与follower都需要确认 （ACK）
// 	config.Producer.RequiredAcks = sarama.WaitForAll
// 	// 新选出一个Partition
// 	config.Producer.Partitioner = sarama.NewRandomPartitioner
// 	// 确认
// 	config.Producer.Return.Successes = true

// 	// 2.连接kafka
// 	client, err := sarama.NewSyncProducer([]string{"127.0.0.1:9092"}, config)

// 	if err != nil {
// 		fmt.Println("Producer closed, err: ", err)
// 		return
// 	}
// 	defer client.Close()
// 	// 3. 封装消息
// 	msg := &sarama.ProducerMessage{}
// 	msg.Topic = "web_log"
// 	msg.Value = sarama.StringEncoder("this is a test log")

// 	// 4. 发送消息
// 	pid, offset, err := client.SendMessage(msg)
// 	if err != nil {
// 		fmt.Println("Send message failed, err: ", err)
// 		return
// 	}
// 	fmt.Printf("pid: %v offset: %v\n", pid, offset)
// }
