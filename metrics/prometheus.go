// Package metrics 提供 Prometheus metrics 支持
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"

	"go-slim.dev/sdq"
)

// Collector 实现 prometheus.Collector 接口
// 用于收集 SDQ 队列的监控指标
type Collector struct {
	queue *sdq.Queue

	// Gauge metrics（当前状态）
	jobsTotal    *prometheus.Desc
	topicsTotal  *prometheus.Desc
	waitingTotal *prometheus.Desc

	// Counter metrics（累计值）
	putsTotal     *prometheus.Desc
	reservesTotal *prometheus.Desc
	deletesTotal  *prometheus.Desc
	releasesTotal *prometheus.Desc
	buriesTotal   *prometheus.Desc
	kicksTotal    *prometheus.Desc
	timeoutsTotal *prometheus.Desc
	touchesTotal  *prometheus.Desc
}

// NewCollector 创建新的 Prometheus Collector
func NewCollector(queue *sdq.Queue) *Collector {
	return &Collector{
		queue: queue,

		// Gauge metrics
		jobsTotal: prometheus.NewDesc(
			"sdq_jobs_total",
			"Total number of jobs by state",
			[]string{"state"},
			nil,
		),
		topicsTotal: prometheus.NewDesc(
			"sdq_topics_total",
			"Total number of topics",
			nil,
			nil,
		),
		waitingTotal: prometheus.NewDesc(
			"sdq_waiting_workers_total",
			"Total number of waiting workers by topic",
			[]string{"topic"},
			nil,
		),

		// Counter metrics
		putsTotal: prometheus.NewDesc(
			"sdq_puts_total",
			"Total number of put operations",
			nil,
			nil,
		),
		reservesTotal: prometheus.NewDesc(
			"sdq_reserves_total",
			"Total number of reserve operations",
			nil,
			nil,
		),
		deletesTotal: prometheus.NewDesc(
			"sdq_deletes_total",
			"Total number of delete operations",
			nil,
			nil,
		),
		releasesTotal: prometheus.NewDesc(
			"sdq_releases_total",
			"Total number of release operations",
			nil,
			nil,
		),
		buriesTotal: prometheus.NewDesc(
			"sdq_buries_total",
			"Total number of bury operations",
			nil,
			nil,
		),
		kicksTotal: prometheus.NewDesc(
			"sdq_kicks_total",
			"Total number of kick operations",
			nil,
			nil,
		),
		timeoutsTotal: prometheus.NewDesc(
			"sdq_timeouts_total",
			"Total number of job timeouts",
			nil,
			nil,
		),
		touchesTotal: prometheus.NewDesc(
			"sdq_touches_total",
			"Total number of touch operations",
			nil,
			nil,
		),
	}
}

// Describe 实现 prometheus.Collector 接口
func (c *Collector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.jobsTotal
	ch <- c.topicsTotal
	ch <- c.waitingTotal
	ch <- c.putsTotal
	ch <- c.reservesTotal
	ch <- c.deletesTotal
	ch <- c.releasesTotal
	ch <- c.buriesTotal
	ch <- c.kicksTotal
	ch <- c.timeoutsTotal
	ch <- c.touchesTotal
}

// Collect 实现 prometheus.Collector 接口
func (c *Collector) Collect(ch chan<- prometheus.Metric) {
	// 获取队列状态统计
	stats := c.queue.Stats()

	// Jobs by state (Gauge)
	ch <- prometheus.MustNewConstMetric(c.jobsTotal, prometheus.GaugeValue, float64(stats.ReadyJobs), "ready")
	ch <- prometheus.MustNewConstMetric(c.jobsTotal, prometheus.GaugeValue, float64(stats.DelayedJobs), "delayed")
	ch <- prometheus.MustNewConstMetric(c.jobsTotal, prometheus.GaugeValue, float64(stats.ReservedJobs), "reserved")
	ch <- prometheus.MustNewConstMetric(c.jobsTotal, prometheus.GaugeValue, float64(stats.BuriedJobs), "buried")

	// Topics total (Gauge)
	ch <- prometheus.MustNewConstMetric(c.topicsTotal, prometheus.GaugeValue, float64(stats.Topics))

	// Waiting workers by topic (Gauge)
	waitingStats := c.queue.StatsWaiting()
	for _, ws := range waitingStats {
		ch <- prometheus.MustNewConstMetric(c.waitingTotal, prometheus.GaugeValue, float64(ws.WaitingWorkers), ws.Topic)
	}

	// 获取操作统计（Counter）
	queueStats := c.queue.QueueStats()
	snapshot := queueStats.Snapshot()

	ch <- prometheus.MustNewConstMetric(c.putsTotal, prometheus.CounterValue, float64(snapshot.Puts))
	ch <- prometheus.MustNewConstMetric(c.reservesTotal, prometheus.CounterValue, float64(snapshot.Reserves))
	ch <- prometheus.MustNewConstMetric(c.deletesTotal, prometheus.CounterValue, float64(snapshot.Deletes))
	ch <- prometheus.MustNewConstMetric(c.releasesTotal, prometheus.CounterValue, float64(snapshot.Releases))
	ch <- prometheus.MustNewConstMetric(c.buriesTotal, prometheus.CounterValue, float64(snapshot.Buries))
	ch <- prometheus.MustNewConstMetric(c.kicksTotal, prometheus.CounterValue, float64(snapshot.Kicks))
	ch <- prometheus.MustNewConstMetric(c.timeoutsTotal, prometheus.CounterValue, float64(snapshot.Timeouts))
	ch <- prometheus.MustNewConstMetric(c.touchesTotal, prometheus.CounterValue, float64(snapshot.Touches))
}
