package collector

import (
	"fmt"
	kafka "github.com/Shopify/sarama"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/wupeaking/logrus"
	"io/ioutil"
	"os"
	"strings"
)

type collector struct {
	g            *prometheus.Desc
	KafkaDataDir string
	BrokerAddr   string
}

func Init(k string, b string) *collector {
	return &collector{
		g: prometheus.NewDesc(
			prometheus.BuildFQName("kafka", "topic", "disk"),
			"disk for topic",
			[]string{"hostname", "topic_name"}, nil,
		),
		KafkaDataDir: k,
		BrokerAddr:   b,
	}
}

func (c collector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.g
}

func (c collector) Collect(ch chan<- prometheus.Metric) {
	TopicsPartis := GetTopicsPartis(c.BrokerAddr)
	metrics := GetTopicDisk(c.KafkaDataDir, TopicsPartis)
	Hostname, _ := os.Hostname()
	for k, v := range metrics {
		ch <- prometheus.MustNewConstMetric(c.g, prometheus.GaugeValue, float64(v), Hostname, k)

	}

}

func GetTopicsPartis(ad string) map[string][]int32 {
	kafkaCli, err := kafka.NewClient(strings.Split(ad, ","), nil)
	if err != nil {
		log.Error("连接kafka失败：", err)
	}
	defer kafkaCli.Close()

	topics, err := kafkaCli.Topics()
	if err != nil {
		log.Error("获取kafka的topics失败: ", err)
	}

	topicsPartis := make(map[string][]int32)

	for _, value := range topics {
		partis, err := kafkaCli.Partitions(value)
		if err != nil {
			log.Error("获取partition异常：", err)
			continue
		}
		topicsPartis[value] = partis

	}
	return topicsPartis
}

func GetTopicDisk(KafkaDataDir string, TopicsPartis map[string][]int32) map[string]int64 {
	topicDisk := make(map[string]int64)
	for topic, partis := range TopicsPartis {
		// 计算单个topic的大小
		var topicSize int64
		for _, parti := range partis {
			// 拼接partition数据目录
			filePath := fmt.Sprintf("%s%s-%d", KafkaDataDir, topic, parti)
			files, e := ioutil.ReadDir(filePath)
			if e != nil {
				log.Error("获取partition目录异常：", e)
				continue
			}

			// 计算单个partition的大小
			var partitionsSize int64
			for _, v := range files {
				partitionsSize += v.Size()
			}
			topicSize += partitionsSize

		}
		topicDisk[topic] = topicSize
	}
	return topicDisk
}
