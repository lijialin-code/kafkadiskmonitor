package main

import (
	"flag"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/weifeng1990/prometheus_exporter_demo/collector"
	"net/http"
)

var (
	KafkaDataDir string
	BrokerAddr   string
)

func handler(w http.ResponseWriter, r *http.Request) {
	// 新建自定义的注册表
	registry := prometheus.NewRegistry()
	c := collector.Init(KafkaDataDir, BrokerAddr)
	// 注册到自定义注册表
	registry.MustRegister(c)
	h := promhttp.HandlerFor(registry, promhttp.HandlerOpts{})
	h.ServeHTTP(w, r)
}

func main() {
	flag.StringVar(&KafkaDataDir, "KafkaDataDir", "/data/kafka/data/", "kafka data path")
	flag.StringVar(&BrokerAddr, "BrokerAddr", "127.0.0.1:9292", "kafka broker address")
	flag.Parse()
	http.HandleFunc("/metrics", handler)
	http.ListenAndServe(":38080", nil)
}
