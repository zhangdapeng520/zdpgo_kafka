package main

import (
	"fmt"

	"github.com/Shopify/sarama"
)

func main() {
	// 创建配置信息
	config := sarama.NewConfig()

	// 设置
	// ack应答机制：ack可以看做一种信号，用于消费者来确认消息是否落盘
	// WaitForAll waits for all in-sync replicas to commit before responding. The minimum number of in-sync replicas is configured on the broker via the `min.insync.replicas` configuration key.
	// WaitForAll等待所有同步副本在响应之前提交。同步副本的最小数量通过' min.insync '在代理上配置。副本的关键配置。
	config.Producer.RequiredAcks = sarama.WaitForAll

	// 发送分区
	// Generates partitioners for choosing the partition to send messages to (defaults to hashing the message key). Similar to the `partitioner.class` setting for the JVM producer.
	// 生成用于选择要将消息发送到的分区的分区标识符(默认为散列消息键)。类似于JVM生成器的' partitioner.class '设置。
	config.Producer.Partitioner = sarama.NewRandomPartitioner

	// 回复确认
	// Return specifies what channels will be populated. If they are set to true, you must read from the respective channels to prevent deadlock. If, however, this config is used to create a `SyncProducer`, both must be set to true and you shall not read from the channels since the producer does this internally.
	// Return指定将填充哪些通道。如果将它们设置为true，则必须从相应的通道读取以防止死锁。然而，如果这个配置被用来创建一个“SyncProducer”，两者都必须设置为true，并且你不能从通道中读取，因为生产者内部会这样做。
	config.Producer.Return.Successes = true

	// 构造一个消息
	msg := &sarama.ProducerMessage{}
	msg.Topic = "message"
	msg.Value = sarama.StringEncoder("【DAL科技】您正在注册DAL科技信息短信平台账号，验证码是：888888，3分钟内有效，请及时输入。")

	// 连接kafka
	client, err := sarama.NewSyncProducer([]string{"192.168.18.101:9092"}, config)
	if err != nil {
		fmt.Println("创建生产者失败：", err)
	}
	defer client.Close()

	// 发送消息
	pid, offset, err := client.SendMessage(msg)
	if err != nil {
		fmt.Println("发送消息失败：", err)
		return
	}
	fmt.Printf("pid:%v offset:%v \n ", pid, offset)

}
