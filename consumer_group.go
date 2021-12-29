package zdpgo_kafka

import (
	"context"
	"fmt"

	"github.com/Shopify/sarama"
	"github.com/zhangdapeng520/zdpgo_log"
)

// 消息处理接口
type ConsumerGroupHandlerInterface interface {
	ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error
	Setup(sarama.ConsumerGroupSession) error
	Cleanup(sarama.ConsumerGroupSession) error
}

// 消费组处理器
type ConsumerGroupHandler struct {
	Log *zdpgo_log.Log // 日志记录对象
}

func (ConsumerGroupHandler) Setup(sarama.ConsumerGroupSession) error {
	return nil
}
func (ConsumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

// ConsumeClaim 消费者组的处理器
func (c ConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	// 遍历得到的消息
	for msg := range claim.Messages() {
		c.Log.Info("消息记录", "key", msg.Key, "value", msg.Value, "time", msg.Timestamp, "topic", msg.Topic)
		session.MarkMessage(msg, "")
	}
	return nil
}

// 处理消息
type HandleGroupMessage func(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim)

// 消费者组
func (k *Kafka) GroupConsumer(groupName string, topics []string, handler ConsumerGroupHandlerInterface) {
	var err error

	// 配置信息
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = false
	config.Version = sarama.V0_10_2_0                     // 指定版本号
	config.Consumer.Offsets.Initial = sarama.OffsetOldest // 未找到组消费位移的时候从哪边开始消费

	// 创建消费组
	address := fmt.Sprintf("%s:%d", k.host, k.port)
	group, err := sarama.NewConsumerGroup([]string{address}, groupName, config)
	if err != nil {
		panic(err)
	}
	defer func() { _ = group.Close() }()

	// 打印错误信息
	go func() {
		for err = range group.Errors() {
			k.log.Error("组错误：", "err", err.Error())
		}
	}()
	k.log.Info("开始消费")

	ctx := context.Background()
	for {
		// topics := []string{"my_topic"}
		// handler := ConsumerGroupHandler{
		// 	Log: k.log,
		// }

		// 组消费
		err = group.Consume(ctx, topics, handler)
		if err != nil {
			k.log.Panic("消费数据错误：", "err", err.Error())
		}
	}
}
