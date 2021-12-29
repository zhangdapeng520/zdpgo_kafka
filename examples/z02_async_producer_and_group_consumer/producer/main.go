package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/zhangdapeng520/zdpgo_kafka"
)

// 异步生产者Goroutines
func AsyncProducer() {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	producer, err := sarama.NewAsyncProducer([]string{"192.168.18.101:9092"}, config)
	if err != nil {
		panic(err)
	}

	// Trap SIGINT to trigger a graceful shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	var (
		wg                                  sync.WaitGroup
		enqueued, successes, producerErrors int
	)

	// 成功次数
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range producer.Successes() {
			successes++
		}
	}()

	// 错误次数
	wg.Add(1)
	go func() {
		defer wg.Done()
		for err := range producer.Errors() {
			log.Println(err)
			producerErrors++
		}
	}()

	// 生产循环
ProducerLoop:
	for {
		// 构建消息
		message := &sarama.ProducerMessage{Topic: "my_topic", Value: sarama.StringEncoder("这是一条测试消息。。。")}

		// 监听不同的goroutine
		select {
		// 将消息写入生产者输入队列
		case producer.Input() <- message:
			enqueued++
			time.Sleep(1 * time.Second) // 不让生产过快
		// 通知信息
		case <-signals:
			producer.AsyncClose() // 关闭生产者
			break ProducerLoop    // 关闭循环
		}
	}

	wg.Wait() // 等待goroutine组都执行完毕

	log.Printf("执行完毕。 成功次数: %d; 失败次数: %d\n", successes, producerErrors)
}

// 异步生产者Goroutines
func AsyncProducer1(topic string, msg <-chan string) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	producer, err := sarama.NewAsyncProducer([]string{"192.168.18.101:9092"}, config)
	if err != nil {
		panic(err)
	}

	// Trap SIGINT to trigger a graceful shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	var (
		wg                                  sync.WaitGroup
		enqueued, successes, producerErrors int
	)

	// 成功次数
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range producer.Successes() {
			successes++
		}
	}()

	// 错误次数
	wg.Add(1)
	go func() {
		defer wg.Done()
		for err := range producer.Errors() {
			log.Println(err)
			producerErrors++
		}
	}()

	// 生产循环
ProducerLoop:
	for {
		// 构建消息
		msgNow := <-msg
		message := &sarama.ProducerMessage{Topic: topic, Value: sarama.StringEncoder(msgNow)}

		// 监听不同的goroutine
		select {
		// 将消息写入生产者输入队列
		case producer.Input() <- message:
			enqueued++
			time.Sleep(1 * time.Second) // 不让生产过快
		// 通知信息
		case <-signals:
			producer.AsyncClose() // 关闭生产者
			break ProducerLoop    // 关闭循环
		}
	}

	wg.Wait() // 等待goroutine组都执行完毕

	log.Printf("执行完毕。 成功次数: %d; 失败次数: %d\n", successes, producerErrors)
}

func main() {
	config := zdpgo_kafka.KafkaConfig{
		Host: "192.168.18.101",
		Port: 9092,
	}
	k := zdpgo_kafka.New(config)

	// 消息管道
	msg := make(chan string)

	// 制造消息
	go func(msg chan<- string) {
		count := 0
		for {
			count += 1
			msg <- fmt.Sprintf("这是第%d条测试数据", count)
			fmt.Println("正在生产数据：", count)

			// 防止消息生产过快
			time.Sleep(time.Second)
		}

	}(msg)

	// 异步生产消息
	go k.AsyncProducer("my_topic", msg)

	// 模拟真实环境中，服务一直运行
	for {

	}
}
