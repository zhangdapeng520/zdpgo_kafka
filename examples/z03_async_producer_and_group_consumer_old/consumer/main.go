package main

import (
	"context"
	"fmt"

	"github.com/Shopify/sarama"
)

// 消费组处理器
type ConsumerGroupHandler struct {
}

func (ConsumerGroupHandler) Setup(sarama.ConsumerGroupSession) error {
	return nil
}
func (ConsumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

// ConsumeClaim 消费者组的处理器
func (c ConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	// 遍历得到的消息
	for msg := range claim.Messages() {
		fmt.Println("消息记录", "key", msg.Key, "value", string(msg.Value), "time", msg.Timestamp, "topic", msg.Topic)
		session.MarkMessage(msg, "")
	}
	return nil
}

func GroupConsumer() {
	var err error

	// 配置信息
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = false
	config.Version = sarama.V0_10_2_0                     // 指定版本号
	config.Consumer.Offsets.Initial = sarama.OffsetOldest // 未找到组消费位移的时候从哪边开始消费

	// 创建消费组
	address := fmt.Sprintf("%s:%d", "192.168.18.101", 9092)
	group, err := sarama.NewConsumerGroup([]string{address}, "group1", config)
	if err != nil {
		panic(err)
	}
	defer func() { _ = group.Close() }()

	// 打印错误信息
	go func() {
		for err = range group.Errors() {
			fmt.Println("组错误：", "err", err.Error())
		}
	}()
	fmt.Println("开始消费")

	ctx := context.Background()
	for {
		topics := []string{"message"}
		handler := ConsumerGroupHandler{}

		// 组消费
		err = group.Consume(ctx, topics, handler)
		if err != nil {
			fmt.Println("消费数据错误：", "err", err.Error())
		}
	}
}
func main() {
	go GroupConsumer()

	// 模拟真实场景中，服务一直处于运行中
	for {

	}
}
