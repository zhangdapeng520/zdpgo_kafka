package main

import (
	"fmt"
	"sync"

	"github.com/Shopify/sarama"
)

var wg sync.WaitGroup

func main() {
	//创建新的消费者
	consumer, err := sarama.NewConsumer([]string{"192.168.18.101:9092"}, nil)
	if err != nil {
		fmt.Println("创建消费者失败：", err)
	}

	//根据topic获取所有的分区列表
	partitionList, err := consumer.Partitions("message")
	if err != nil {
		fmt.Println("根据topic获取分区列表失败：", err)
	}
	fmt.Println(partitionList)

	//遍历所有的分区
	for p := range partitionList {
		//针对每一个分区创建一个对应分区的消费者
		pc, err := consumer.ConsumePartition("message", int32(p), sarama.OffsetNewest)
		if err != nil {
			fmt.Printf("消费者消费指定分区失败： 分区 %d, 错误:%v\n", p, err)
		}
		defer pc.AsyncClose()
		wg.Add(1)

		//异步从每个分区消费信息
		go func(sarama.PartitionConsumer) {
			for msg := range pc.Messages() {
				fmt.Printf("partition:%d offset:%d key:%v value:%s \n",
					msg.Partition, msg.Offset, msg.Key, msg.Value)
			}
		}(pc)
	}
	wg.Wait()
}
