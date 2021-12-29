package zdpgo_kafka

import (
	"github.com/Shopify/sarama"
	"github.com/zhangdapeng520/zdpgo_log"
)

// 核心类
type Kafka struct {
	host        string               // 主机地址
	port        int                  // 端口号
	log         *zdpgo_log.Log       // 日志对象
	logFilePath string               // 日志文件路径
	debug       bool                 // 是否的调试模式
	Consumer    *sarama.Consumer     // 消费者
	Producer    *sarama.SyncProducer // 生成者
}

// kafka配置信息
type KafkaConfig struct {
	Host        string // 主机地址
	Port        int    // 端口号
	LogFilePath string // 日志文件路径
	Debug       bool   // 是否为调试模式
}

// 设置debug模式
func (k *Kafka) SetDebug(debug bool) {
	k.debug = debug
	k.log = zdpgo_log.New(k.logFilePath)
	k.log.SetDebug(k.debug)
}

// 是否为debug模式
func (k *Kafka) IsDebug() bool {
	return k.debug
}

// 创建kafka对象的方法
func New(config KafkaConfig) *Kafka {
	k := Kafka{}

	// 设置debug模式
	k.debug = config.Debug

	// 初始化日志
	if config.LogFilePath == "" {
		k.logFilePath = "zdpgo_kafka.log"
	}
	k.log = zdpgo_log.New(k.logFilePath)
	k.log.SetDebug(k.debug)

	// 初始化配置
	k.host = config.Host
	k.port = config.Port

	// 创建生产者
	k.createProducer()

	// 创建消费者
	k.createConsumer()

	return &k
}

// 关闭kafka
func (k *Kafka) Close() {
	// 关闭生产者
	if k.Producer != nil {
		err := (*k.Producer).Close()
		if err != nil {
			k.log.Error("关闭生产者失败：", err)
			return
		}
	}

	// 关闭消费者
	if k.Consumer != nil {
		err := (*k.Consumer).Close()
		if err != nil {
			k.log.Error("关闭消费者失败：", err)
			return
		}
	}

	// 重置日志
	k.log = nil
}
