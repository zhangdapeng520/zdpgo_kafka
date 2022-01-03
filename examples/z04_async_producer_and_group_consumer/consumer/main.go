package main

import (
	"github.com/Shopify/sarama"
	"github.com/zhangdapeng520/zdpgo_kafka"
	"github.com/zhangdapeng520/zdpgo_log"
)

// 创建一个自己的组处理器
type Handler struct {
	zdpgo_kafka.ConsumerGroupHandler // 继承原本的组处理器
}

// 重写消费方法
func (h Handler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	// 遍历得到的消息
	for msg := range claim.Messages() {
		h.Log.Info("消息记录111", "key", msg.Key, "value", msg.Value, "time", msg.Timestamp, "topic", msg.Topic)
		session.MarkMessage(msg, "")
	}
	return nil
}

func main() {
	// 消费者
	config := zdpgo_kafka.KafkaConfig{
		Host: "192.168.18.101",
		Port: 9092,
	}
	k := zdpgo_kafka.New(config)

	// 创建处理器
	// var handler zdpgo_kafka.ConsumerGroupHandler // 自带的处理器

	var handler Handler // 继承处理器，重写消费方法
	logConfig := zdpgo_log.LogConfig{
		Debug:       true,
		LogFilePath: "consumer.log",
	}
	handler.Log = zdpgo_log.New(logConfig)

	go k.GroupConsumer("my-group", []string{"my_topic"}, handler)

	// 模拟真实场景中，服务一直处于运行中
	for {

	}
}
