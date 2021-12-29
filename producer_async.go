package zdpgo_kafka

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/Shopify/sarama"
)

// 异步生产者Goroutines
func (k *Kafka) AsyncProducer(topic string, msg <-chan string) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	address := fmt.Sprintf("%s:%d", k.host, k.port)
	producer, err := sarama.NewAsyncProducer([]string{address}, config)
	if err != nil {
		panic(err)
	}

	// 使用信号量实现优雅退出
	signals := make(chan os.Signal)
	signal.Notify(signals, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

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
			// time.Sleep(1 * time.Second) // 不让生产过快
		// 通知信息
		case <-signals:
			producer.AsyncClose() // 关闭生产者
			break ProducerLoop    // 关闭循环
		}
	}

	wg.Wait() // 等待goroutine组都执行完毕

	log.Printf("执行完毕。 成功次数: %d; 失败次数: %d\n", successes, producerErrors)
}
