package main

import (
	"fmt"

	"github.com/Shopify/sarama"
	"github.com/zhangdapeng520/zdpgo_kafka"
)

func main() {
	config := zdpgo_kafka.KafkaConfig{
		Host: "192.168.18.101",
		Port: 9092,
	}
	k := zdpgo_kafka.New(config)
	partitionList, _ := k.Partitions("test")
	fmt.Println(partitionList)

	// 遍历所有的分区
	for p := range partitionList {
		// 针对每一个分区创建一个对应分区的消费者
		pc, _ := k.ConsumePartition("test", p)
		defer pc.AsyncClose()

		// 异步从每个分区消费信息
		go func(sarama.PartitionConsumer) {
			for msg := range pc.Messages() {
				fmt.Printf("partition:%d Offse:%d Key:%v Value:%s \n",
					msg.Partition, msg.Offset, msg.Key, msg.Value)
			}
		}(pc)
	}
	for {

	}
}
