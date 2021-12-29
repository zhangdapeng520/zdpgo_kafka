package zdpgo_kafka

import (
	"errors"
	"fmt"

	"github.com/Shopify/sarama"
)

// 创建消费者
func (k *Kafka) createConsumer() {
	address := fmt.Sprintf("%s:%d", k.host, k.port)
	consumer, err := sarama.NewConsumer([]string{address}, nil)
	if err != nil {
		k.log.Error("创建消费者失败：", err)
	}
	k.Consumer = &consumer
}

// 获取所有分区列表
func (k *Kafka) Partitions(topic string) ([]int32, error) {
	partitionList, err := (*k.Consumer).Partitions(topic)
	if err != nil {
		msg := fmt.Sprintf("根据topic获取分区列表失败：%s", err.Error())
		k.log.Error(msg)
		return nil, errors.New(msg)
	}
	return partitionList, nil
}

// 根据分区创建消费者
func (k *Kafka) ConsumePartition(topic string, partition int) (sarama.PartitionConsumer, error) {
	pc, err := (*k.Consumer).ConsumePartition(topic, int32(partition), sarama.OffsetNewest)
	if err != nil {
		msg := fmt.Sprintf("根据分区创建消费者失败：%s", err.Error())
		k.log.Error(msg)
		return nil, errors.New(msg)
	}
	return pc, nil
}
