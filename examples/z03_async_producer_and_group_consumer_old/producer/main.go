package main

import (
	"log"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/Shopify/sarama"
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
		message := &sarama.ProducerMessage{Topic: "message", Value: sarama.StringEncoder("【DAL科技】您正在注册DAL科技信息短信平台账号，验证码是：888888，3分钟内有效，请及时输入。")}

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
	// 异步生产消息
	go AsyncProducer()

	// 模拟真实环境中，服务一直运行
	for {

	}
}
