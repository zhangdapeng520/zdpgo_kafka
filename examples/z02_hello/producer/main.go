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
	defer k.Close()

	pid, offset := k.SendMessage("message", "【DAL科技】您正在注册DAL科技信息短信平台账号，验证码是：888888，3分钟内有效，请及时输入。")
	fmt.Println("发送消息成功：", pid, offset)
}
