package main

import (
	"fmt"

	"github.com/zhangdapeng520/zdpgo_kafka"
)

func main() {
	config := zdpgo_kafka.KafkaConfig{
		Host: "192.168.18.101",
		Port: 9092,
	}
	k := zdpgo_kafka.New(config)
	pid, offset := k.SendMessage("test", "你好啊！")
	fmt.Println("发送消息成功：", pid, offset)
	k.Close()
}
