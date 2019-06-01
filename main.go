package main

import (
	"context"
	"flag"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/mum4k/termdash"
	"github.com/mum4k/termdash/cell"
	"github.com/mum4k/termdash/container"
	"github.com/mum4k/termdash/linestyle"
	"github.com/mum4k/termdash/terminal/termbox"
	"github.com/mum4k/termdash/terminal/terminalapi"
	"github.com/mum4k/termdash/widgets/linechart"
)

type topicPartitions map[string][]int32

var brokersAddress, topic, consumerGroup string
var lags []float64

func ensureNotError(err error) {
	if err != nil {
		panic(err)
	}
}

func getTopicPartitions(brokers []string, topic string, config *sarama.Config) topicPartitions {
	client, err := sarama.NewClient(brokers, config)
	ensureNotError(err)

	partitions, err := client.Partitions(topic)
	ensureNotError(err)

	topicPartitions := make(map[string][]int32)
	topicPartitions[topic] = partitions

	return topicPartitions
}

func getConfig() *sarama.Config {
	config := sarama.NewConfig()
	config.Version = sarama.V2_0_0_0
	return config
}

func getConsumerOffsets(brokers []string, group string, topicPartitions topicPartitions, config *sarama.Config) *sarama.OffsetFetchResponse {
	client, err := sarama.NewClusterAdmin(brokers, config)
	ensureNotError(err)
	defer client.Close()

	offsetResponse, err := client.ListConsumerGroupOffsets(group, topicPartitions)
	ensureNotError(err)

	return offsetResponse
}

func getLatestOffsets(brokers []string, topic string, partitions []int32, config *sarama.Config) map[int32]int64 {
	client, err := sarama.NewClient(brokers, config)
	ensureNotError(err)

	latestOffsets := make(map[int32]int64)
	wg := &sync.WaitGroup{}
	wg.Add(len(partitions))
	lock := &sync.Mutex{}

	for _, partition := range partitions {
		go func(partition int32) {
			latest, err := client.GetOffset(topic, partition, -1) // -1 means latest offset
			ensureNotError(err)

			lock.Lock()
			latestOffsets[partition] = latest
			lock.Unlock()

			wg.Done()
		}(partition)
	}

	wg.Wait()
	return latestOffsets
}

func renderLineChart(ctx context.Context, lc *linechart.LineChart, delay time.Duration) {
	ticker := time.NewTicker(delay)
	defer ticker.Stop()

	brokers := strings.Split(brokersAddress, ",")
	config := getConfig()
	topicPartitions := getTopicPartitions(brokers, topic, config)

	var lag float64
	for {
		select {
		case <-ticker.C:

			commitedOffsets := getConsumerOffsets(brokers, consumerGroup, topicPartitions, config)
			latestOffsets := getLatestOffsets(brokers, topic, topicPartitions[topic], config)

			lag = 0
			for _, partitionsInfo := range commitedOffsets.Blocks {
				for partition, partitionInfo := range partitionsInfo {
					lag += float64(latestOffsets[partition] - partitionInfo.Offset)
				}
			}
			lags = append(lags, lag)

			if err := lc.Series("first", lags,
				linechart.SeriesCellOpts(cell.FgColor(cell.ColorBlue)),
				linechart.SeriesXLabels(map[int]string{
					0: "zero",
				}),
			); err != nil {
				panic(err)
			}

		case <-ctx.Done():
			return
		}
	}
}

func main() {

	flag.StringVar(&brokersAddress, "brokers", "localhost:9092", "The server(s) to connect to")
	flag.StringVar(&topic, "topic", "", "Topic name")
	flag.StringVar(&consumerGroup, "group", "", "The consumer group name")
	flag.Parse()
	lags = make([]float64, 1)
	lags[0] = 0

	renderUI()

}

func renderUI() {
	t, err := termbox.New()
	ensureNotError(err)
	defer t.Close()

	ctx, cancel := context.WithCancel(context.Background())
	lc, err := linechart.New(
		linechart.AxesCellOpts(cell.FgColor(cell.ColorRed)),
		linechart.YLabelCellOpts(cell.FgColor(cell.ColorGreen)),
		linechart.XLabelCellOpts(cell.FgColor(cell.ColorCyan)),
	)
	ensureNotError(err)

	go renderLineChart(ctx, lc, 1*time.Second)
	c, err := container.New(
		t,
		container.Border(linestyle.Light),
		container.BorderTitle("PRESS Q TO QUIT"),
		container.PlaceWidget(lc),
	)
	ensureNotError(err)

	quitter := func(k *terminalapi.Keyboard) {
		if k.Key == 'q' || k.Key == 'Q' {
			cancel()
		}
	}

	if err := termdash.Run(ctx, t, c, termdash.KeyboardSubscriber(quitter), termdash.RedrawInterval(1*time.Second)); err != nil {
		panic(err)
	}
}
