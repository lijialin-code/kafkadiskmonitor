# KafkaDiskMonitor
Build:
    CGO_ENABLED=0  GOOS=linux  GOARCH=amd64  go build -o KafkaDiskMonitor  main.go



Run:
    KafkaDiskMonitor  -BrokerAddr="127.0.0.1:9292" -KafkaDataDir="/data/kafka/data/"