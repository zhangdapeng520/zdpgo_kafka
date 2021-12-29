package zdpgo_kafka

import (
	"fmt"

	"github.com/Shopify/sarama"
)

// 创建生产者
func (k *Kafka) createProducer() {
	// 保证单例
	if k.Producer != nil {
		return
	}

	config := sarama.NewConfig()
	// 设置
	// ack应答机制
	config.Producer.RequiredAcks = sarama.WaitForAll

	// 发送分区
	config.Producer.Partitioner = sarama.NewRandomPartitioner

	// 回复确认
	config.Producer.Return.Successes = true

	// 连接kafka
	address := fmt.Sprintf("%s:%d", k.host, k.port)
	client, err := sarama.NewSyncProducer([]string{address}, config)
	if err != nil {
		k.log.Error("创建生产者失败：", err)
	}
	k.Producer = &client
}

// 生产者发送消息
func (k *Kafka) SendMessage(topic, value string) (partition int32, offset int64) {
	k.createProducer() // 创建生产者

	// 构造一个消息
	msg := &sarama.ProducerMessage{}
	msg.Topic = topic // topic只能是英文的
	msg.Value = sarama.StringEncoder(value)

	// 发送消息
	pid, offset, err := (*k.Producer).SendMessage(msg)
	if err != nil {
		k.log.Error("发送消息失败：", err)
	}
	return pid, offset
}
