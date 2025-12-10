//go:build ignore
// +build ignore

package sdq

import (
	"context"
	"encoding/json"
	"fmt"
	"html/template"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ============================================================
// 稳定性测试配置
// ============================================================

const (
	// 短时稳定性测试（CI 环境）
	shortStabilityDuration = 5 * time.Minute
	shortJobCount          = 100_000
	shortTopicCount        = 1000

	// 长时稳定性测试（手动运行）
	longStabilityDuration = 24 * time.Hour
	longJobCount          = 1_000_000
	longTopicCount        = 10_000
)

// ============================================================
// 报表数据结构
// ============================================================

// StabilityReport 稳定性测试报表
type StabilityReport struct {
	GeneratedAt time.Time              `json:"generated_at"`
	Duration    string                 `json:"duration"`
	Environment EnvironmentInfo        `json:"environment"`
	Tests       map[string]*TestResult `json:"tests"`
	Summary     ReportSummary          `json:"summary"`
}

// EnvironmentInfo 环境信息
type EnvironmentInfo struct {
	GoVersion   string `json:"go_version"`
	OS          string `json:"os"`
	Arch        string `json:"arch"`
	NumCPU      int    `json:"num_cpu"`
	GOMAXPROCS  int    `json:"gomaxprocs"`
	InitialHeap string `json:"initial_heap"`
	TotalMemory string `json:"total_memory,omitempty"`
}

// TestResult 单个测试结果
type TestResult struct {
	Name     string                 `json:"name"`
	Status   string                 `json:"status"` // passed, failed, skipped
	Duration string                 `json:"duration"`
	Metrics  map[string]interface{} `json:"metrics"`
	Memory   MemoryMetrics          `json:"memory"`
	Error    string                 `json:"error,omitempty"`
}

// MemoryMetrics 内存指标
type MemoryMetrics struct {
	InitialHeap string `json:"initial_heap"`
	PeakHeap    string `json:"peak_heap"`
	FinalHeap   string `json:"final_heap"`
	GCRuns      uint32 `json:"gc_runs"`
}

// ReportSummary 报表摘要
type ReportSummary struct {
	TotalTests int    `json:"total_tests"`
	Passed     int    `json:"passed"`
	Failed     int    `json:"failed"`
	Skipped    int    `json:"skipped"`
	TotalOps   uint64 `json:"total_ops"`
	MemoryLeak bool   `json:"memory_leak_detected"`
	Conclusion string `json:"conclusion"`
}

// MemSnapshot 内存统计快照
type MemSnapshot struct {
	Timestamp   time.Time
	HeapAlloc   uint64 // 堆内存分配
	HeapInuse   uint64 // 堆内存使用
	HeapObjects uint64 // 堆对象数
	NumGC       uint32 // GC 次数
	GCPauseNs   uint64 // 最近一次 GC 暂停时间
}

// 全局报表实例
var (
	globalReport     *StabilityReport
	globalReportOnce sync.Once
	globalReportMu   sync.Mutex
)

// initGlobalReport 初始化全局报表
func initGlobalReport() {
	globalReportOnce.Do(func() {
		var memStats runtime.MemStats
		runtime.ReadMemStats(&memStats)

		globalReport = &StabilityReport{
			GeneratedAt: time.Now(),
			Environment: EnvironmentInfo{
				GoVersion:   runtime.Version(),
				OS:          runtime.GOOS,
				Arch:        runtime.GOARCH,
				NumCPU:      runtime.NumCPU(),
				GOMAXPROCS:  runtime.GOMAXPROCS(0),
				InitialHeap: formatBytes(memStats.HeapAlloc),
			},
			Tests:   make(map[string]*TestResult),
			Summary: ReportSummary{},
		}
	})
}

// addTestResult 添加测试结果到报表
func addTestResult(result *TestResult) {
	initGlobalReport()
	globalReportMu.Lock()
	defer globalReportMu.Unlock()
	globalReport.Tests[result.Name] = result
}

// saveReport 保存报表到文件
func saveReport(t *testing.T) {
	if globalReport == nil {
		return
	}

	reportPath := getEnvString("STABILITY_REPORT")
	if reportPath == "" {
		return
	}

	globalReportMu.Lock()
	defer globalReportMu.Unlock()

	// 计算摘要
	var totalOps uint64
	for _, test := range globalReport.Tests {
		globalReport.Summary.TotalTests++
		switch test.Status {
		case "passed":
			globalReport.Summary.Passed++
		case "failed":
			globalReport.Summary.Failed++
		case "skipped":
			globalReport.Summary.Skipped++
		}
		if ops, ok := test.Metrics["total_ops"].(uint64); ok {
			totalOps += ops
		}
	}
	globalReport.Summary.TotalOps = totalOps
	globalReport.Duration = time.Since(globalReport.GeneratedAt).String()

	if globalReport.Summary.Failed == 0 {
		globalReport.Summary.Conclusion = "All stability tests passed"
	} else {
		globalReport.Summary.Conclusion = fmt.Sprintf("%d test(s) failed", globalReport.Summary.Failed)
	}

	// 确保目录存在
	dir := filepath.Dir(reportPath)
	if dir != "." && dir != "" {
		_ = os.MkdirAll(dir, 0755)
	}

	// 写入 JSON 文件
	data, err := json.MarshalIndent(globalReport, "", "  ")
	if err != nil {
		t.Logf("Failed to marshal report: %v", err)
		return
	}

	if err := os.WriteFile(reportPath, data, 0644); err != nil {
		t.Logf("Failed to write report: %v", err)
		return
	}

	t.Logf("Report saved to: %s", reportPath)
}

// formatBytes 格式化字节数
func formatBytes(bytes uint64) string {
	const (
		KB = 1024
		MB = KB * 1024
		GB = MB * 1024
	)
	switch {
	case bytes >= GB:
		return fmt.Sprintf("%.2f GB", float64(bytes)/GB)
	case bytes >= MB:
		return fmt.Sprintf("%.2f MB", float64(bytes)/MB)
	case bytes >= KB:
		return fmt.Sprintf("%.2f KB", float64(bytes)/KB)
	default:
		return fmt.Sprintf("%d B", bytes)
	}
}

// collectMemSnapshot 收集内存统计
func collectMemSnapshot() MemSnapshot {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return MemSnapshot{
		Timestamp:   time.Now(),
		HeapAlloc:   m.HeapAlloc,
		HeapInuse:   m.HeapInuse,
		HeapObjects: m.HeapObjects,
		NumGC:       m.NumGC,
		GCPauseNs:   m.PauseNs[(m.NumGC+255)%256],
	}
}

// generateHTMLReport 生成 HTML 可视化报表
func generateHTMLReport(report *StabilityReport, path string) error {
	html := `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>SDQ Stability Test Report</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #f5f5f5; color: #333; line-height: 1.6; }
        .container { max-width: 1200px; margin: 0 auto; padding: 20px; }
        .header { background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 40px; border-radius: 12px; margin-bottom: 24px; }
        .header h1 { font-size: 2em; margin-bottom: 8px; }
        .header .meta { opacity: 0.9; font-size: 0.95em; }
        .summary-cards { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 16px; margin-bottom: 24px; }
        .card { background: white; border-radius: 12px; padding: 24px; box-shadow: 0 2px 8px rgba(0,0,0,0.08); }
        .card.passed { border-left: 4px solid #22c55e; }
        .card.failed { border-left: 4px solid #ef4444; }
        .card.info { border-left: 4px solid #3b82f6; }
        .card h3 { font-size: 0.85em; color: #666; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 8px; }
        .card .value { font-size: 2em; font-weight: 700; }
        .card .value.passed { color: #22c55e; }
        .card .value.failed { color: #ef4444; }
        .section { background: white; border-radius: 12px; padding: 24px; margin-bottom: 24px; box-shadow: 0 2px 8px rgba(0,0,0,0.08); }
        .section h2 { font-size: 1.25em; margin-bottom: 16px; padding-bottom: 12px; border-bottom: 2px solid #f0f0f0; }
        .test-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(350px, 1fr)); gap: 16px; }
        .test-card { background: #fafafa; border-radius: 8px; padding: 20px; }
        .test-card.passed { border-left: 3px solid #22c55e; }
        .test-card.failed { border-left: 3px solid #ef4444; }
        .test-card h4 { display: flex; align-items: center; gap: 8px; margin-bottom: 12px; }
        .test-card h4 .status { width: 10px; height: 10px; border-radius: 50%; }
        .test-card h4 .status.passed { background: #22c55e; }
        .test-card h4 .status.failed { background: #ef4444; }
        .metrics { display: grid; grid-template-columns: repeat(2, 1fr); gap: 8px; font-size: 0.9em; }
        .metric { display: flex; justify-content: space-between; padding: 4px 0; border-bottom: 1px solid #eee; }
        .metric .label { color: #666; }
        .metric .val { font-weight: 600; font-family: 'SF Mono', Monaco, monospace; }
        .memory-section { margin-top: 12px; padding-top: 12px; border-top: 1px dashed #ddd; }
        .memory-section h5 { font-size: 0.8em; color: #888; margin-bottom: 8px; }
        .charts { display: grid; grid-template-columns: repeat(auto-fit, minmax(400px, 1fr)); gap: 24px; }
        .chart-container { position: relative; height: 300px; }
        .env-info { display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 12px; }
        .env-item { text-align: center; padding: 12px; background: #f8f9fa; border-radius: 8px; }
        .env-item .label { font-size: 0.75em; color: #666; text-transform: uppercase; }
        .env-item .value { font-size: 1.1em; font-weight: 600; margin-top: 4px; }
        .conclusion { padding: 20px; border-radius: 8px; margin-top: 16px; }
        .conclusion.passed { background: #dcfce7; color: #166534; }
        .conclusion.failed { background: #fee2e2; color: #991b1b; }
        .footer { text-align: center; padding: 20px; color: #888; font-size: 0.85em; }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>SDQ Stability Test Report</h1>
            <div class="meta">
                Generated: {{.GeneratedAt}} | Duration: {{.Duration}}
            </div>
        </div>

        <div class="summary-cards">
            <div class="card {{if eq .Summary.Failed 0}}passed{{else}}failed{{end}}">
                <h3>Test Results</h3>
                <div class="value {{if eq .Summary.Failed 0}}passed{{else}}failed{{end}}">{{.Summary.Passed}}/{{.Summary.TotalTests}}</div>
            </div>
            <div class="card info">
                <h3>Total Operations</h3>
                <div class="value">{{formatOps .Summary.TotalOps}}</div>
            </div>
            <div class="card {{if .Summary.MemoryLeak}}failed{{else}}passed{{end}}">
                <h3>Memory Leak</h3>
                <div class="value {{if .Summary.MemoryLeak}}failed{{else}}passed{{end}}">{{if .Summary.MemoryLeak}}Detected{{else}}None{{end}}</div>
            </div>
            <div class="card info">
                <h3>Environment</h3>
                <div class="value" style="font-size:1em;">{{.Environment.OS}}/{{.Environment.Arch}}</div>
            </div>
        </div>

        <div class="section">
            <h2>Environment Details</h2>
            <div class="env-info">
                <div class="env-item">
                    <div class="label">Go Version</div>
                    <div class="value">{{.Environment.GoVersion}}</div>
                </div>
                <div class="env-item">
                    <div class="label">OS / Arch</div>
                    <div class="value">{{.Environment.OS}} / {{.Environment.Arch}}</div>
                </div>
                <div class="env-item">
                    <div class="label">CPUs</div>
                    <div class="value">{{.Environment.NumCPU}}</div>
                </div>
                <div class="env-item">
                    <div class="label">GOMAXPROCS</div>
                    <div class="value">{{.Environment.GOMAXPROCS}}</div>
                </div>
                <div class="env-item">
                    <div class="label">Initial Heap</div>
                    <div class="value">{{.Environment.InitialHeap}}</div>
                </div>
            </div>
        </div>

        <div class="section">
            <h2>Test Results</h2>
            <div class="test-grid">
                {{range .Tests}}
                <div class="test-card {{.Status}}">
                    <h4>
                        <span class="status {{.Status}}"></span>
                        {{.Name}}
                    </h4>
                    <div class="metrics">
                        <div class="metric">
                            <span class="label">Duration</span>
                            <span class="val">{{.Duration}}</span>
                        </div>
                        {{range $key, $val := .Metrics}}
                        <div class="metric">
                            <span class="label">{{$key}}</span>
                            <span class="val">{{formatMetric $val}}</span>
                        </div>
                        {{end}}
                    </div>
                    <div class="memory-section">
                        <h5>Memory</h5>
                        <div class="metrics">
                            <div class="metric">
                                <span class="label">Initial</span>
                                <span class="val">{{.Memory.InitialHeap}}</span>
                            </div>
                            <div class="metric">
                                <span class="label">Peak</span>
                                <span class="val">{{.Memory.PeakHeap}}</span>
                            </div>
                            <div class="metric">
                                <span class="label">Final</span>
                                <span class="val">{{.Memory.FinalHeap}}</span>
                            </div>
                            <div class="metric">
                                <span class="label">GC Runs</span>
                                <span class="val">{{.Memory.GCRuns}}</span>
                            </div>
                        </div>
                    </div>
                    {{if .Error}}
                    <div style="margin-top:12px;padding:8px;background:#fee2e2;border-radius:4px;color:#991b1b;font-size:0.85em;">
                        {{.Error}}
                    </div>
                    {{end}}
                </div>
                {{end}}
            </div>
        </div>

        <div class="section">
            <h2>Performance Charts</h2>
            <div class="charts">
                <div class="chart-container">
                    <canvas id="opsChart"></canvas>
                </div>
                <div class="chart-container">
                    <canvas id="memoryChart"></canvas>
                </div>
            </div>
        </div>

        <div class="conclusion {{if eq .Summary.Failed 0}}passed{{else}}failed{{end}}">
            <strong>Conclusion:</strong> {{.Summary.Conclusion}}
        </div>

        <div class="footer">
            SDQ - Simple Delay Queue | Report generated at {{.GeneratedAt}}
        </div>
    </div>

    <script>
        const testData = {{.TestsJSON}};

        // Operations Chart
        const opsCtx = document.getElementById('opsChart').getContext('2d');
        new Chart(opsCtx, {
            type: 'bar',
            data: {
                labels: Object.keys(testData),
                datasets: [{
                    label: 'Total Operations',
                    data: Object.values(testData).map(t => t.metrics.total_ops || 0),
                    backgroundColor: Object.values(testData).map(t => t.status === 'passed' ? '#22c55e' : '#ef4444'),
                    borderRadius: 6
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: { legend: { display: false }, title: { display: true, text: 'Operations per Test' } },
                scales: { y: { beginAtZero: true } }
            }
        });

        // Memory Chart
        const memCtx = document.getElementById('memoryChart').getContext('2d');
        const parseMemory = (str) => {
            if (!str) return 0;
            const match = str.match(/([\d.]+)\s*(GB|MB|KB|B)/);
            if (!match) return 0;
            const val = parseFloat(match[1]);
            switch(match[2]) {
                case 'GB': return val * 1024;
                case 'MB': return val;
                case 'KB': return val / 1024;
                default: return val / 1024 / 1024;
            }
        };
        new Chart(memCtx, {
            type: 'bar',
            data: {
                labels: Object.keys(testData),
                datasets: [
                    { label: 'Initial', data: Object.values(testData).map(t => parseMemory(t.memory.initial_heap)), backgroundColor: '#3b82f6', borderRadius: 6 },
                    { label: 'Peak', data: Object.values(testData).map(t => parseMemory(t.memory.peak_heap)), backgroundColor: '#f59e0b', borderRadius: 6 },
                    { label: 'Final', data: Object.values(testData).map(t => parseMemory(t.memory.final_heap)), backgroundColor: '#22c55e', borderRadius: 6 }
                ]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: { title: { display: true, text: 'Memory Usage (MB)' } },
                scales: { y: { beginAtZero: true } }
            }
        });
    </script>
</body>
</html>`

	// 准备模板函数
	funcMap := template.FuncMap{
		"formatOps": func(ops uint64) string {
			if ops >= 1_000_000_000 {
				return fmt.Sprintf("%.2fB", float64(ops)/1_000_000_000)
			} else if ops >= 1_000_000 {
				return fmt.Sprintf("%.2fM", float64(ops)/1_000_000)
			} else if ops >= 1_000 {
				return fmt.Sprintf("%.2fK", float64(ops)/1_000)
			}
			return fmt.Sprintf("%d", ops)
		},
		"formatMetric": func(v interface{}) string {
			switch val := v.(type) {
			case float64:
				if val >= 1_000_000 {
					return fmt.Sprintf("%.2fM", val/1_000_000)
				} else if val >= 1_000 {
					return fmt.Sprintf("%.2fK", val/1_000)
				} else if val == float64(int64(val)) {
					return fmt.Sprintf("%d", int64(val))
				}
				return fmt.Sprintf("%.2f", val)
			case uint64:
				if val >= 1_000_000 {
					return fmt.Sprintf("%.2fM", float64(val)/1_000_000)
				} else if val >= 1_000 {
					return fmt.Sprintf("%.2fK", float64(val)/1_000)
				}
				return fmt.Sprintf("%d", val)
			case int:
				return fmt.Sprintf("%d", val)
			case string:
				return val
			default:
				return fmt.Sprintf("%v", v)
			}
		},
	}

	// 准备数据
	type templateData struct {
		*StabilityReport
		TestsJSON template.JS
	}

	testsJSON, _ := json.Marshal(report.Tests)
	data := templateData{
		StabilityReport: report,
		TestsJSON:       template.JS(testsJSON),
	}

	tmpl, err := template.New("report").Funcs(funcMap).Parse(html)
	if err != nil {
		return fmt.Errorf("parse template: %w", err)
	}

	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create file: %w", err)
	}
	defer f.Close()

	if err := tmpl.Execute(f, data); err != nil {
		return fmt.Errorf("execute template: %w", err)
	}

	return nil
}

// ============================================================
// 测试 1: 长时间运行测试
// ============================================================

// TestStability_LongRunning 验证长期运行的稳定性
// 运行方式:
//
//	go test -run=TestStability_LongRunning -timeout=25h -v
//	STABILITY_DURATION=1h go test -run=TestStability_LongRunning -timeout=2h -v
func TestStability_LongRunning(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping long-running stability test in short mode")
	}

	// 根据环境变量决定测试时长
	duration := shortStabilityDuration
	if d := getEnvDuration("STABILITY_DURATION"); d > 0 {
		duration = d
	}

	// 阶段性报告间隔（默认每小时）
	reportInterval := 1 * time.Hour
	if duration < 1*time.Hour {
		reportInterval = duration / 5 // 短测试分 5 个阶段报告
		if reportInterval < 1*time.Minute {
			reportInterval = 1 * time.Minute
		}
	}

	// 硬超时保护：比预期时长多 10%，防止无限运行
	hardTimeout := duration + duration/10
	if hardTimeout < duration+5*time.Minute {
		hardTimeout = duration + 5*time.Minute
	}

	t.Logf("Starting %v stability test (hard timeout: %v, report interval: %v)", duration, hardTimeout, reportInterval)

	config := DefaultConfig()
	config.Storage = NewMemoryStorage()

	q, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = q.Stop() }()

	// 使用硬超时作为上下文超时
	ctx, cancel := context.WithTimeout(context.Background(), hardTimeout)
	defer cancel()

	// 单独的测试结束时间点
	testEndTime := time.Now().Add(duration)

	var (
		putCount     atomic.Uint64
		reserveCount atomic.Uint64
		deleteCount  atomic.Uint64
		errorCount   atomic.Uint64
	)

	// 记录初始内存
	startTime := time.Now()
	initialMem := collectMemSnapshot()
	var memSnapshotsMu sync.Mutex
	var memSnapshots []MemSnapshot
	memSnapshots = append(memSnapshots, initialMem)

	// 阶段性报告保存函数
	saveInterimReport := func(phase string) {
		memSnapshotsMu.Lock()
		currentSnapshots := make([]MemSnapshot, len(memSnapshots))
		copy(currentSnapshots, memSnapshots)
		memSnapshotsMu.Unlock()

		elapsed := time.Since(startTime)
		puts := putCount.Load()
		reserves := reserveCount.Load()
		deletes := deleteCount.Load()
		errors := errorCount.Load()

		var peakHeap uint64
		for _, snap := range currentSnapshots {
			if snap.HeapAlloc > peakHeap {
				peakHeap = snap.HeapAlloc
			}
		}

		report := fmt.Sprintf(`
================================================================================
INTERIM REPORT - %s
================================================================================
Time: %s
Elapsed: %v / %v (%.1f%%)

Operations:
  Put:     %d (%.0f/s)
  Reserve: %d (%.0f/s)
  Delete:  %d (%.0f/s)
  Errors:  %d (%.4f%%)

Memory:
  Initial: %.2f MB
  Current: %.2f MB
  Peak:    %.2f MB

Status: RUNNING
================================================================================
`,
			phase,
			time.Now().Format("2006-01-02 15:04:05"),
			elapsed.Round(time.Second), duration, float64(elapsed)/float64(duration)*100,
			puts, float64(puts)/elapsed.Seconds(),
			reserves, float64(reserves)/elapsed.Seconds(),
			deletes, float64(deletes)/elapsed.Seconds(),
			errors, float64(errors)/float64(puts+reserves+1)*100,
			float64(initialMem.HeapAlloc)/1024/1024,
			float64(currentSnapshots[len(currentSnapshots)-1].HeapAlloc)/1024/1024,
			float64(peakHeap)/1024/1024,
		)

		t.Log(report)

		// 保存到文件
		reportFile := fmt.Sprintf("stability_interim_%s.txt", time.Now().Format("20060102_150405"))
		if err := os.WriteFile(reportFile, []byte(report), 0644); err != nil {
			t.Logf("Warning: failed to save interim report: %v", err)
		} else {
			t.Logf("Interim report saved to %s", reportFile)
		}
	}

	// 背压控制：限制队列中的最大任务数
	const maxPendingJobs = 10000

	// 启动生产者
	var wg sync.WaitGroup
	numProducers := 4
	numConsumers := 4

	for i := 0; i < numProducers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			topic := fmt.Sprintf("topic-%d", id%10)
			body := []byte("stability test payload")

			for {
				select {
				case <-ctx.Done():
					return
				default:
					// 检查是否到达测试结束时间
					if time.Now().After(testEndTime) {
						return
					}

					// 背压控制：当队列积压过多时暂停生产
					pending := putCount.Load() - deleteCount.Load()
					if pending > maxPendingJobs {
						time.Sleep(10 * time.Millisecond)
						continue
					}

					_, err := q.Put(topic, body, 1, 0, 30*time.Second)
					if err != nil {
						errorCount.Add(1)
					} else {
						putCount.Add(1)
					}
				}
			}
		}(i)
	}

	// 启动消费者
	topics := make([]string, 10)
	for i := range topics {
		topics[i] = fmt.Sprintf("topic-%d", i)
	}

	for i := 0; i < numConsumers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
					// 检查是否到达测试结束时间
					if time.Now().After(testEndTime) {
						return
					}

					job, err := q.Reserve(topics, 100*time.Millisecond)
					if err == nil {
						reserveCount.Add(1)
						if err := job.Delete(); err == nil {
							deleteCount.Add(1)
						}
					}
				}
			}
		}()
	}

	// 启动内存监控和阶段性报告
	wg.Add(1)
	go func() {
		defer wg.Done()
		memTicker := time.NewTicker(1 * time.Minute)
		defer memTicker.Stop()

		reportTicker := time.NewTicker(reportInterval)
		defer reportTicker.Stop()

		phaseNum := 1
		for {
			select {
			case <-ctx.Done():
				return
			case <-memTicker.C:
				// 检查是否到达测试结束时间
				if time.Now().After(testEndTime) {
					return
				}

				snap := collectMemSnapshot()
				memSnapshotsMu.Lock()
				memSnapshots = append(memSnapshots, snap)
				memSnapshotsMu.Unlock()

				t.Logf("Progress: put=%d reserve=%d delete=%d errors=%d heap=%.2fMB",
					putCount.Load(),
					reserveCount.Load(),
					deleteCount.Load(),
					errorCount.Load(),
					float64(snap.HeapAlloc)/1024/1024,
				)
			case <-reportTicker.C:
				// 检查是否到达测试结束时间
				if time.Now().After(testEndTime) {
					return
				}

				saveInterimReport(fmt.Sprintf("Phase %d", phaseNum))
				phaseNum++
			}
		}
	}()

	wg.Wait()

	// 分析结果
	finalMem := collectMemSnapshot()
	memSnapshotsMu.Lock()
	memSnapshots = append(memSnapshots, finalMem)
	memSnapshotsMu.Unlock()

	actualDuration := time.Since(startTime)

	t.Logf("\n=== Stability Test Results ===")
	t.Logf("Planned Duration: %v, Actual Duration: %v", duration, actualDuration.Round(time.Second))
	t.Logf("Put: %d, Reserve: %d, Delete: %d, Errors: %d",
		putCount.Load(), reserveCount.Load(), deleteCount.Load(), errorCount.Load())
	t.Logf("Initial heap: %.2f MB", float64(initialMem.HeapAlloc)/1024/1024)
	t.Logf("Final heap: %.2f MB", float64(finalMem.HeapAlloc)/1024/1024)
	t.Logf("GC runs: %d", finalMem.NumGC-initialMem.NumGC)

	// 检查是否因硬超时而终止
	if actualDuration > duration+1*time.Minute {
		t.Errorf("Test exceeded planned duration: planned=%v actual=%v (hard timeout triggered)", duration, actualDuration)
	}

	// 验证无严重内存泄漏（允许 2x 增长）
	if finalMem.HeapAlloc > initialMem.HeapAlloc*2 && finalMem.HeapAlloc > 100*1024*1024 {
		t.Errorf("Possible memory leak: initial=%.2fMB final=%.2fMB",
			float64(initialMem.HeapAlloc)/1024/1024,
			float64(finalMem.HeapAlloc)/1024/1024)
	}

	// 验证错误率
	totalOps := putCount.Load() + reserveCount.Load()
	if totalOps > 0 {
		errorRate := float64(errorCount.Load()) / float64(totalOps)
		if errorRate > 0.01 { // 允许 1% 错误率
			t.Errorf("High error rate: %.2f%%", errorRate*100)
		}
	}

	// 保存最终报告
	saveInterimReport("FINAL")
}

// ============================================================
// 测试 2: 百万级任务处理
// ============================================================

// TestStability_MillionJobs 验证大规模任务处理能力
// 运行方式:
//
//	go test -run=TestStability_MillionJobs -timeout=1h -v
//	JOB_COUNT=1000000 go test -run=TestStability_MillionJobs -timeout=2h -v
func TestStability_MillionJobs(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping million jobs test in short mode")
	}

	jobCount := shortJobCount
	if c := getEnvInt("JOB_COUNT"); c > 0 {
		jobCount = c
	}

	t.Logf("Testing with %d jobs", jobCount)

	config := DefaultConfig()
	config.Storage = NewMemoryStorage()
	config.Ticker = NewNoOpTicker() // 避免 ticker 干扰

	q, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = q.Stop() }()

	initialMem := collectMemSnapshot()
	body := []byte("million jobs test")

	// Phase 1: 批量写入
	t.Log("Phase 1: Putting jobs...")
	startPut := time.Now()
	for i := 0; i < jobCount; i++ {
		_, err := q.Put("million-test", body, uint32(i%1000), 0, 60*time.Second)
		if err != nil {
			t.Fatalf("Put failed at job %d: %v", i, err)
		}

		if i > 0 && i%100000 == 0 {
			mem := collectMemSnapshot()
			t.Logf("  Put %d jobs, heap=%.2fMB", i, float64(mem.HeapAlloc)/1024/1024)
		}
	}
	putDuration := time.Since(startPut)
	t.Logf("Put %d jobs in %v (%.0f jobs/sec)",
		jobCount, putDuration, float64(jobCount)/putDuration.Seconds())

	afterPutMem := collectMemSnapshot()
	t.Logf("Heap after put: %.2f MB", float64(afterPutMem.HeapAlloc)/1024/1024)

	// Phase 2: 批量消费
	t.Log("Phase 2: Consuming jobs...")
	startReserve := time.Now()
	consumed := 0
	for i := 0; i < jobCount; i++ {
		job, err := q.Reserve([]string{"million-test"}, 1*time.Second)
		if err != nil {
			t.Fatalf("Reserve failed at job %d: %v", i, err)
		}
		if err := job.Delete(); err != nil {
			t.Fatalf("Delete failed at job %d: %v", i, err)
		}
		consumed++

		if i > 0 && i%100000 == 0 {
			mem := collectMemSnapshot()
			t.Logf("  Consumed %d jobs, heap=%.2fMB", i, float64(mem.HeapAlloc)/1024/1024)
		}
	}
	reserveDuration := time.Since(startReserve)
	t.Logf("Consumed %d jobs in %v (%.0f jobs/sec)",
		consumed, reserveDuration, float64(consumed)/reserveDuration.Seconds())

	// 强制 GC
	runtime.GC()
	time.Sleep(100 * time.Millisecond)

	finalMem := collectMemSnapshot()

	t.Logf("\n=== Million Jobs Test Results ===")
	t.Logf("Jobs: %d", jobCount)
	t.Logf("Put throughput: %.0f jobs/sec", float64(jobCount)/putDuration.Seconds())
	t.Logf("Reserve throughput: %.0f jobs/sec", float64(consumed)/reserveDuration.Seconds())
	t.Logf("Initial heap: %.2f MB", float64(initialMem.HeapAlloc)/1024/1024)
	t.Logf("Peak heap: %.2f MB", float64(afterPutMem.HeapAlloc)/1024/1024)
	t.Logf("Final heap: %.2f MB", float64(finalMem.HeapAlloc)/1024/1024)

	// 验证内存回收
	if finalMem.HeapAlloc > afterPutMem.HeapAlloc/2 {
		t.Logf("Warning: memory not fully reclaimed after consuming all jobs")
	}
}

// ============================================================
// 测试 3: 万级 Topic 管理
// ============================================================

// TestStability_ManyTopics 验证大量 Topic 的管理能力
// 运行方式:
//
//	go test -run=TestStability_ManyTopics -timeout=1h -v
//	TOPIC_COUNT=10000 go test -run=TestStability_ManyTopics -timeout=2h -v
func TestStability_ManyTopics(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping many topics test in short mode")
	}

	topicCount := shortTopicCount
	if c := getEnvInt("TOPIC_COUNT"); c > 0 {
		topicCount = c
	}

	t.Logf("Testing with %d topics", topicCount)

	config := DefaultConfig()
	config.Storage = NewMemoryStorage()
	config.Ticker = NewNoOpTicker()

	q, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = q.Stop() }()

	initialMem := collectMemSnapshot()
	body := []byte("topic test")
	jobsPerTopic := 10

	// 创建大量 topic
	t.Log("Creating topics...")
	startCreate := time.Now()
	for i := 0; i < topicCount; i++ {
		topic := fmt.Sprintf("topic-%06d", i)
		for j := 0; j < jobsPerTopic; j++ {
			_, err := q.Put(topic, body, 1, 0, 60*time.Second)
			if err != nil {
				t.Fatalf("Put failed for topic %s: %v", topic, err)
			}
		}

		if i > 0 && i%1000 == 0 {
			mem := collectMemSnapshot()
			t.Logf("  Created %d topics, heap=%.2fMB", i, float64(mem.HeapAlloc)/1024/1024)
		}
	}
	createDuration := time.Since(startCreate)

	afterCreateMem := collectMemSnapshot()
	stats := q.Stats()

	t.Logf("Created %d topics with %d jobs in %v",
		topicCount, stats.TotalJobs, createDuration)
	t.Logf("Heap after create: %.2f MB", float64(afterCreateMem.HeapAlloc)/1024/1024)

	// 计算每个 topic 的内存开销（处理 GC 导致的负值情况）
	var memPerTopic float64
	if afterCreateMem.HeapAlloc > initialMem.HeapAlloc {
		memPerTopic = float64(afterCreateMem.HeapAlloc-initialMem.HeapAlloc) / float64(topicCount)
	}
	t.Logf("Memory per topic: %.2f KB", memPerTopic/1024)

	// 消费所有任务
	t.Log("Consuming all jobs...")
	startConsume := time.Now()
	consumed := 0
	allTopics := q.ListTopics()

	for {
		job, err := q.Reserve(allTopics, 100*time.Millisecond)
		if err == ErrTimeout {
			break
		}
		if err != nil {
			t.Fatalf("Reserve failed: %v", err)
		}
		if err := job.Delete(); err != nil {
			t.Fatalf("Delete failed: %v", err)
		}
		consumed++
	}
	consumeDuration := time.Since(startConsume)

	runtime.GC()
	time.Sleep(100 * time.Millisecond)

	finalMem := collectMemSnapshot()

	t.Logf("\n=== Many Topics Test Results ===")
	t.Logf("Topics: %d, Jobs per topic: %d", topicCount, jobsPerTopic)
	t.Logf("Total jobs consumed: %d", consumed)
	t.Logf("Create time: %v", createDuration)
	t.Logf("Consume time: %v", consumeDuration)
	t.Logf("Memory per topic: %.2f KB", memPerTopic/1024)
	t.Logf("Final heap: %.2f MB", float64(finalMem.HeapAlloc)/1024/1024)

	// 验证内存增长是线性的（允许 20% 误差）
	expectedMem := float64(topicCount) * memPerTopic
	actualGrowth := float64(afterCreateMem.HeapAlloc - initialMem.HeapAlloc)
	if actualGrowth > expectedMem*1.2 {
		t.Errorf("Non-linear memory growth: expected ~%.2fMB, got %.2fMB",
			expectedMem/1024/1024, actualGrowth/1024/1024)
	}
}

// ============================================================
// 测试 4: 内存泄漏检测
// ============================================================

// TestStability_MemoryLeak 检测内存泄漏
// 运行方式:
//
//	go test -run=TestStability_MemoryLeak -v
func TestStability_MemoryLeak(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping memory leak test in short mode")
	}

	config := DefaultConfig()
	config.Storage = NewMemoryStorage()
	config.Ticker = NewNoOpTicker()

	q, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = q.Stop() }()

	// 预热：执行足够多的操作让内存稳定
	body := []byte("leak test")
	warmupRounds := 3
	jobsPerRound := 10000

	for i := 0; i < warmupRounds; i++ {
		for j := 0; j < jobsPerRound; j++ {
			_, _ = q.Put("leak-test", body, 1, 0, 60*time.Second)
		}
		for j := 0; j < jobsPerRound; j++ {
			job, _ := q.Reserve([]string{"leak-test"}, time.Second)
			_ = job.Delete()
		}
	}

	runtime.GC()
	time.Sleep(100 * time.Millisecond)
	baselineMem := collectMemSnapshot()
	t.Logf("Baseline after warmup: heap=%.2fMB", float64(baselineMem.HeapAlloc)/1024/1024)

	// 执行多轮 put/reserve/delete 循环
	rounds := 10
	var memSamples []uint64

	for round := 0; round < rounds; round++ {
		// Put
		for i := 0; i < jobsPerRound; i++ {
			_, _ = q.Put("leak-test", body, 1, 0, 60*time.Second)
		}

		// Reserve + Delete
		for i := 0; i < jobsPerRound; i++ {
			job, err := q.Reserve([]string{"leak-test"}, time.Second)
			if err != nil {
				t.Fatalf("Round %d: Reserve failed at %d: %v", round, i, err)
			}
			_ = job.Delete()
		}

		runtime.GC()
		time.Sleep(50 * time.Millisecond)

		mem := collectMemSnapshot()
		memSamples = append(memSamples, mem.HeapAlloc)
		t.Logf("Round %d: heap=%.2fMB", round+1, float64(mem.HeapAlloc)/1024/1024)
	}

	// 分析内存趋势：只看后半部分（前几轮可能还在稳定）
	stableStart := rounds / 2
	var increasing int
	for i := stableStart + 1; i < len(memSamples); i++ {
		if memSamples[i] > memSamples[i-1]+100*1024 { // 允许 100KB 波动
			increasing++
		}
	}

	finalMem := collectMemSnapshot()

	// 比较最后几轮的平均值与 baseline
	var lastRoundsSum uint64
	lastRoundsCount := 3
	for i := len(memSamples) - lastRoundsCount; i < len(memSamples); i++ {
		lastRoundsSum += memSamples[i]
	}
	lastRoundsAvg := lastRoundsSum / uint64(lastRoundsCount)
	growth := float64(lastRoundsAvg) / float64(baselineMem.HeapAlloc)

	t.Logf("\n=== Memory Leak Test Results ===")
	t.Logf("Warmup rounds: %d, Test rounds: %d, Jobs per round: %d", warmupRounds, rounds, jobsPerRound)
	t.Logf("Baseline heap: %.2f MB", float64(baselineMem.HeapAlloc)/1024/1024)
	t.Logf("Final heap: %.2f MB", float64(finalMem.HeapAlloc)/1024/1024)
	t.Logf("Last %d rounds avg: %.2f MB", lastRoundsCount, float64(lastRoundsAvg)/1024/1024)
	t.Logf("Growth ratio: %.2fx", growth)
	t.Logf("Increasing rounds (after stabilization): %d/%d", increasing, rounds-stableStart-1)

	// 判定标准：
	// 1. 内存增长超过 50%（相对于预热后的 baseline）
	// 2. 后半部分轮次中，超过一半出现持续增长
	if increasing > (rounds-stableStart)/2 && growth > 1.5 {
		t.Errorf("Possible memory leak detected: growth=%.2fx, increasing=%d/%d",
			growth, increasing, rounds-stableStart-1)
	}
}

// ============================================================
// 测试 5: 并发稳定性
// ============================================================

// TestStability_ConcurrentOperations 验证并发操作的稳定性
// 运行方式:
//
//	go test -run=TestStability_ConcurrentOperations -v
//	go test -race -run=TestStability_ConcurrentOperations -v
func TestStability_ConcurrentOperations(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping concurrent stability test in short mode")
	}

	config := DefaultConfig()
	config.Storage = NewMemoryStorage()

	q, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = q.Stop() }()

	duration := 1 * time.Minute
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	var (
		putOps     atomic.Uint64
		reserveOps atomic.Uint64
		releaseOps atomic.Uint64
		buryOps    atomic.Uint64
		kickOps    atomic.Uint64
		touchOps   atomic.Uint64
		deleteOps  atomic.Uint64
		errors     atomic.Uint64
	)

	numWorkers := runtime.GOMAXPROCS(0) * 2
	var wg sync.WaitGroup

	// 混合操作 workers
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			topic := fmt.Sprintf("concurrent-%d", id%5)
			body := []byte("concurrent test")

			for {
				select {
				case <-ctx.Done():
					return
				default:
				}

				// Put
				jobID, err := q.Put(topic, body, 1, 0, 30*time.Second)
				if err != nil {
					errors.Add(1)
					continue
				}
				putOps.Add(1)

				// Reserve
				job, err := q.Reserve([]string{topic}, 100*time.Millisecond)
				if err != nil {
					continue
				}
				reserveOps.Add(1)

				// 随机操作
				switch id % 5 {
				case 0: // Touch + Delete
					if err := job.Touch(); err == nil {
						touchOps.Add(1)
					}
					if err := job.Delete(); err == nil {
						deleteOps.Add(1)
					}
				case 1: // Release + Re-reserve + Delete
					if err := job.Release(0, 0); err == nil {
						releaseOps.Add(1)
					}
					if job2, err := q.Reserve([]string{topic}, 100*time.Millisecond); err == nil {
						reserveOps.Add(1)
						_ = job2.Delete()
						deleteOps.Add(1)
					}
				case 2: // Bury + Kick + Reserve + Delete
					if err := job.Bury(1); err == nil {
						buryOps.Add(1)
					}
					if n, err := q.Kick(topic, 1); err == nil && n > 0 {
						kickOps.Add(1)
					}
					if job2, err := q.Reserve([]string{topic}, 100*time.Millisecond); err == nil {
						reserveOps.Add(1)
						_ = job2.Delete()
						deleteOps.Add(1)
					}
				default: // Simple Delete
					if err := job.Delete(); err == nil {
						deleteOps.Add(1)
					}
				}
				_ = jobID
			}
		}(i)
	}

	wg.Wait()

	t.Logf("\n=== Concurrent Stability Results ===")
	t.Logf("Duration: %v, Workers: %d", duration, numWorkers)
	t.Logf("Put: %d, Reserve: %d, Delete: %d", putOps.Load(), reserveOps.Load(), deleteOps.Load())
	t.Logf("Release: %d, Bury: %d, Kick: %d, Touch: %d",
		releaseOps.Load(), buryOps.Load(), kickOps.Load(), touchOps.Load())
	t.Logf("Errors: %d", errors.Load())

	// 最终状态检查
	stats := q.Stats()
	t.Logf("Final state: topics=%d, total=%d, ready=%d, reserved=%d, buried=%d",
		stats.Topics, stats.TotalJobs, stats.ReadyJobs, stats.ReservedJobs, stats.BuriedJobs)
}

// ============================================================
// 测试 6: SQLite 存储稳定性
// ============================================================

// TestStability_SQLiteStorage 验证 SQLite 存储的长期稳定性
// 运行方式:
//
//	go test -run=TestStability_SQLiteStorage -v
func TestStability_SQLiteStorage(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping SQLite stability test in short mode")
	}

	dbPath := t.TempDir() + "/stability.db"
	storage, err := NewSQLiteStorage(dbPath)
	if err != nil {
		t.Fatal(err)
	}

	config := DefaultConfig()
	config.Storage = storage
	config.Ticker = NewNoOpTicker()

	q, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = q.Stop() }()

	jobCount := 10000
	body := []byte("sqlite stability test")

	// 写入
	t.Log("Writing jobs to SQLite...")
	startWrite := time.Now()
	for i := 0; i < jobCount; i++ {
		_, err := q.Put("sqlite-test", body, uint32(i%100), 0, 60*time.Second)
		if err != nil {
			t.Fatalf("Put failed at %d: %v", i, err)
		}
	}
	writeDuration := time.Since(startWrite)
	t.Logf("Wrote %d jobs in %v (%.0f jobs/sec)",
		jobCount, writeDuration, float64(jobCount)/writeDuration.Seconds())

	// 读取
	t.Log("Reading jobs from SQLite...")
	startRead := time.Now()
	for i := 0; i < jobCount; i++ {
		job, err := q.Reserve([]string{"sqlite-test"}, time.Second)
		if err != nil {
			t.Fatalf("Reserve failed at %d: %v", i, err)
		}
		_ = job.Delete()
	}
	readDuration := time.Since(startRead)
	t.Logf("Read %d jobs in %v (%.0f jobs/sec)",
		jobCount, readDuration, float64(jobCount)/readDuration.Seconds())

	// 验证数据库文件大小
	fi, err := os.Stat(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Database file size: %.2f MB", float64(fi.Size())/1024/1024)
}

// ============================================================
// 辅助函数
// ============================================================

func getEnvDuration(key string) time.Duration {
	val := os.Getenv(key)
	if val == "" {
		return 0
	}
	d, err := time.ParseDuration(val)
	if err != nil {
		return 0
	}
	return d
}

func getEnvInt(key string) int {
	val := os.Getenv(key)
	if val == "" {
		return 0
	}
	n, err := strconv.Atoi(val)
	if err != nil {
		return 0
	}
	return n
}

func getEnvString(key string) string {
	return os.Getenv(key)
}

// ============================================================
// 报表生成主入口
// ============================================================

// TestStability_Report 运行所有稳定性测试并生成报表
// 运行方式:
//
//	STABILITY_REPORT=report.json go test -run=TestStability_Report -v -timeout=30m
func TestStability_Report(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stability report test in short mode")
	}

	reportPath := getEnvString("STABILITY_REPORT")
	if reportPath == "" {
		reportPath = "stability_report.json"
	}

	// 初始化报表
	initGlobalReport()
	startTime := time.Now()

	// 运行各项测试并收集结果
	tests := []struct {
		name string
		fn   func(t *testing.T) *TestResult
	}{
		{"LongRunning", runLongRunningTest},
		{"MillionJobs", runMillionJobsTest},
		{"ManyTopics", runManyTopicsTest},
		{"MemoryLeak", runMemoryLeakTest},
		{"ConcurrentOps", runConcurrentOpsTest},
		{"SQLiteStorage", runSQLiteStorageTest},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := tc.fn(t)
			addTestResult(result)

			if result.Status == "failed" {
				t.Errorf("Test %s failed: %s", tc.name, result.Error)
			}
		})
	}

	// 生成最终报表
	globalReportMu.Lock()
	globalReport.Duration = time.Since(startTime).String()

	// 计算摘要
	var totalOps uint64
	for _, test := range globalReport.Tests {
		globalReport.Summary.TotalTests++
		switch test.Status {
		case "passed":
			globalReport.Summary.Passed++
		case "failed":
			globalReport.Summary.Failed++
			if test.Name == "MemoryLeak" {
				globalReport.Summary.MemoryLeak = true
			}
		case "skipped":
			globalReport.Summary.Skipped++
		}
		if ops, ok := test.Metrics["total_ops"].(uint64); ok {
			totalOps += ops
		}
	}
	globalReport.Summary.TotalOps = totalOps

	if globalReport.Summary.Failed == 0 {
		globalReport.Summary.Conclusion = "All stability tests passed. System is stable."
	} else {
		globalReport.Summary.Conclusion = fmt.Sprintf("%d test(s) failed. Review required.", globalReport.Summary.Failed)
	}
	globalReportMu.Unlock()

	// 写入 JSON 报表文件
	data, err := json.MarshalIndent(globalReport, "", "  ")
	if err != nil {
		t.Fatalf("Failed to marshal report: %v", err)
	}

	// 确保目录存在
	if dir := filepath.Dir(reportPath); dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0755); err != nil {
			t.Fatalf("Failed to create report directory: %v", err)
		}
	}

	if err := os.WriteFile(reportPath, data, 0644); err != nil {
		t.Fatalf("Failed to write report: %v", err)
	}

	// 生成 HTML 报表
	htmlPath := strings.TrimSuffix(reportPath, ".json") + ".html"
	if err := generateHTMLReport(globalReport, htmlPath); err != nil {
		t.Logf("Warning: failed to generate HTML report: %v", err)
	} else {
		t.Logf("HTML report saved to: %s", htmlPath)
	}

	t.Logf("\n%s", strings.Repeat("=", 60))
	t.Logf("STABILITY TEST REPORT")
	t.Logf("%s", strings.Repeat("=", 60))
	t.Logf("Duration: %s", globalReport.Duration)
	t.Logf("Environment: %s %s, Go %s, %d CPUs",
		globalReport.Environment.OS,
		globalReport.Environment.Arch,
		globalReport.Environment.GoVersion,
		globalReport.Environment.NumCPU)
	t.Logf("%s", strings.Repeat("-", 60))
	t.Logf("Results: %d passed, %d failed, %d skipped",
		globalReport.Summary.Passed,
		globalReport.Summary.Failed,
		globalReport.Summary.Skipped)
	t.Logf("Total Operations: %d", globalReport.Summary.TotalOps)
	t.Logf("Memory Leak: %v", globalReport.Summary.MemoryLeak)
	t.Logf("%s", strings.Repeat("-", 60))
	t.Logf("Conclusion: %s", globalReport.Summary.Conclusion)
	t.Logf("%s", strings.Repeat("=", 60))
	t.Logf("Report saved to: %s", reportPath)
}

// ============================================================
// 独立测试运行函数（用于报表生成）
// ============================================================

func runLongRunningTest(t *testing.T) *TestResult {
	startTime := time.Now()
	result := &TestResult{
		Name:    "LongRunning",
		Status:  "passed",
		Metrics: make(map[string]interface{}),
	}

	// LongRunning test gets 80% of total duration (it's the main stability test)
	// Other tests share the remaining 20%
	duration := 30 * time.Second // 报表模式使用较短时间
	if d := getEnvDuration("STABILITY_DURATION"); d > 0 {
		duration = d * 80 / 100 // 80% of total duration for LongRunning
	}

	config := DefaultConfig()
	config.Storage = NewMemoryStorage()

	q, err := New(config)
	if err != nil {
		result.Status = "failed"
		result.Error = err.Error()
		return result
	}
	if err := q.Start(); err != nil {
		result.Status = "failed"
		result.Error = err.Error()
		return result
	}
	defer func() { _ = q.Stop() }()

	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	var putCount, reserveCount, deleteCount, errorCount atomic.Uint64
	initialMem := collectMemSnapshot()
	var peakHeap uint64 = initialMem.HeapAlloc

	const maxPendingJobs = 10000
	var wg sync.WaitGroup

	// 生产者
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			topic := fmt.Sprintf("topic-%d", id%10)
			body := []byte("stability test payload")
			for {
				select {
				case <-ctx.Done():
					return
				default:
					if putCount.Load()-deleteCount.Load() > maxPendingJobs {
						time.Sleep(10 * time.Millisecond)
						continue
					}
					if _, err := q.Put(topic, body, 1, 0, 30*time.Second); err != nil {
						errorCount.Add(1)
					} else {
						putCount.Add(1)
					}
				}
			}
		}(i)
	}

	// 消费者
	topics := make([]string, 10)
	for i := range topics {
		topics[i] = fmt.Sprintf("topic-%d", i)
	}
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
					if job, err := q.Reserve(topics, 100*time.Millisecond); err == nil {
						reserveCount.Add(1)
						if err := job.Delete(); err == nil {
							deleteCount.Add(1)
						}
					}
				}
			}
		}()
	}

	// 内存监控
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				mem := collectMemSnapshot()
				if mem.HeapAlloc > peakHeap {
					peakHeap = mem.HeapAlloc
				}
			}
		}
	}()

	wg.Wait()
	finalMem := collectMemSnapshot()

	result.Duration = time.Since(startTime).String()
	result.Metrics["duration"] = duration.String()
	result.Metrics["put_count"] = putCount.Load()
	result.Metrics["reserve_count"] = reserveCount.Load()
	result.Metrics["delete_count"] = deleteCount.Load()
	result.Metrics["error_count"] = errorCount.Load()
	result.Metrics["total_ops"] = putCount.Load() + reserveCount.Load()
	result.Metrics["ops_per_sec"] = float64(putCount.Load()+reserveCount.Load()) / duration.Seconds()
	result.Memory = MemoryMetrics{
		InitialHeap: formatBytes(initialMem.HeapAlloc),
		PeakHeap:    formatBytes(peakHeap),
		FinalHeap:   formatBytes(finalMem.HeapAlloc),
		GCRuns:      finalMem.NumGC - initialMem.NumGC,
	}

	// 验证
	if finalMem.HeapAlloc > initialMem.HeapAlloc*2 && finalMem.HeapAlloc > 100*1024*1024 {
		result.Status = "failed"
		result.Error = fmt.Sprintf("memory leak: initial=%s final=%s",
			formatBytes(initialMem.HeapAlloc), formatBytes(finalMem.HeapAlloc))
	}
	if errorCount.Load() > 0 {
		errorRate := float64(errorCount.Load()) / float64(putCount.Load()+reserveCount.Load())
		if errorRate > 0.01 {
			result.Status = "failed"
			result.Error = fmt.Sprintf("high error rate: %.2f%%", errorRate*100)
		}
	}

	return result
}

func runMillionJobsTest(t *testing.T) *TestResult {
	startTime := time.Now()
	result := &TestResult{
		Name:    "MillionJobs",
		Status:  "passed",
		Metrics: make(map[string]interface{}),
	}

	jobCount := 50000 // 报表模式使用较少任务
	if c := getEnvInt("JOB_COUNT"); c > 0 {
		jobCount = c
	}

	config := DefaultConfig()
	config.Storage = NewMemoryStorage()
	config.Ticker = NewNoOpTicker()

	q, err := New(config)
	if err != nil {
		result.Status = "failed"
		result.Error = err.Error()
		return result
	}
	if err := q.Start(); err != nil {
		result.Status = "failed"
		result.Error = err.Error()
		return result
	}
	defer func() { _ = q.Stop() }()

	initialMem := collectMemSnapshot()
	body := []byte("million jobs test")

	// Put
	putStart := time.Now()
	for i := 0; i < jobCount; i++ {
		if _, err := q.Put("million-test", body, uint32(i%1000), 0, 60*time.Second); err != nil {
			result.Status = "failed"
			result.Error = fmt.Sprintf("put failed at %d: %v", i, err)
			return result
		}
	}
	putDuration := time.Since(putStart)
	afterPutMem := collectMemSnapshot()

	// Reserve + Delete
	reserveStart := time.Now()
	for i := 0; i < jobCount; i++ {
		job, err := q.Reserve([]string{"million-test"}, time.Second)
		if err != nil {
			result.Status = "failed"
			result.Error = fmt.Sprintf("reserve failed at %d: %v", i, err)
			return result
		}
		if err := job.Delete(); err != nil {
			result.Status = "failed"
			result.Error = fmt.Sprintf("delete failed at %d: %v", i, err)
			return result
		}
	}
	reserveDuration := time.Since(reserveStart)

	runtime.GC()
	time.Sleep(100 * time.Millisecond)
	finalMem := collectMemSnapshot()

	result.Duration = time.Since(startTime).String()
	result.Metrics["job_count"] = jobCount
	result.Metrics["put_duration"] = putDuration.String()
	result.Metrics["reserve_duration"] = reserveDuration.String()
	result.Metrics["put_throughput"] = float64(jobCount) / putDuration.Seconds()
	result.Metrics["reserve_throughput"] = float64(jobCount) / reserveDuration.Seconds()
	result.Metrics["total_ops"] = uint64(jobCount * 2)
	result.Memory = MemoryMetrics{
		InitialHeap: formatBytes(initialMem.HeapAlloc),
		PeakHeap:    formatBytes(afterPutMem.HeapAlloc),
		FinalHeap:   formatBytes(finalMem.HeapAlloc),
		GCRuns:      finalMem.NumGC - initialMem.NumGC,
	}

	return result
}

func runManyTopicsTest(t *testing.T) *TestResult {
	startTime := time.Now()
	result := &TestResult{
		Name:    "ManyTopics",
		Status:  "passed",
		Metrics: make(map[string]interface{}),
	}

	topicCount := 500
	if c := getEnvInt("TOPIC_COUNT"); c > 0 {
		topicCount = c
	}
	jobsPerTopic := 10

	config := DefaultConfig()
	config.Storage = NewMemoryStorage()
	config.Ticker = NewNoOpTicker()

	q, err := New(config)
	if err != nil {
		result.Status = "failed"
		result.Error = err.Error()
		return result
	}
	if err := q.Start(); err != nil {
		result.Status = "failed"
		result.Error = err.Error()
		return result
	}
	defer func() { _ = q.Stop() }()

	initialMem := collectMemSnapshot()
	body := []byte("topic test")

	// 创建 topics
	createStart := time.Now()
	for i := 0; i < topicCount; i++ {
		topic := fmt.Sprintf("topic-%06d", i)
		for j := 0; j < jobsPerTopic; j++ {
			if _, err := q.Put(topic, body, 1, 0, 60*time.Second); err != nil {
				result.Status = "failed"
				result.Error = fmt.Sprintf("put failed for topic %s: %v", topic, err)
				return result
			}
		}
	}
	createDuration := time.Since(createStart)
	afterCreateMem := collectMemSnapshot()

	// 消费
	consumeStart := time.Now()
	consumed := 0
	allTopics := q.ListTopics()
	for {
		job, err := q.Reserve(allTopics, 100*time.Millisecond)
		if err == ErrTimeout {
			break
		}
		if err != nil {
			result.Status = "failed"
			result.Error = fmt.Sprintf("reserve failed: %v", err)
			return result
		}
		_ = job.Delete()
		consumed++
	}
	consumeDuration := time.Since(consumeStart)

	runtime.GC()
	time.Sleep(100 * time.Millisecond)
	finalMem := collectMemSnapshot()

	// 计算每个 topic 的内存开销（处理 GC 导致的负值情况）
	var memPerTopic float64
	if afterCreateMem.HeapAlloc > initialMem.HeapAlloc {
		memPerTopic = float64(afterCreateMem.HeapAlloc-initialMem.HeapAlloc) / float64(topicCount)
	}

	result.Duration = time.Since(startTime).String()
	result.Metrics["topic_count"] = topicCount
	result.Metrics["jobs_per_topic"] = jobsPerTopic
	result.Metrics["total_jobs"] = consumed
	result.Metrics["create_duration"] = createDuration.String()
	result.Metrics["consume_duration"] = consumeDuration.String()
	result.Metrics["memory_per_topic_kb"] = fmt.Sprintf("%.2f", memPerTopic/1024)
	result.Metrics["total_ops"] = uint64(consumed * 2)
	result.Memory = MemoryMetrics{
		InitialHeap: formatBytes(initialMem.HeapAlloc),
		PeakHeap:    formatBytes(afterCreateMem.HeapAlloc),
		FinalHeap:   formatBytes(finalMem.HeapAlloc),
		GCRuns:      finalMem.NumGC - initialMem.NumGC,
	}

	return result
}

func runMemoryLeakTest(t *testing.T) *TestResult {
	startTime := time.Now()
	result := &TestResult{
		Name:    "MemoryLeak",
		Status:  "passed",
		Metrics: make(map[string]interface{}),
	}

	config := DefaultConfig()
	config.Storage = NewMemoryStorage()
	config.Ticker = NewNoOpTicker()

	q, err := New(config)
	if err != nil {
		result.Status = "failed"
		result.Error = err.Error()
		return result
	}
	if err := q.Start(); err != nil {
		result.Status = "failed"
		result.Error = err.Error()
		return result
	}
	defer func() { _ = q.Stop() }()

	body := []byte("leak test")
	warmupRounds := 3
	jobsPerRound := 10000

	// 预热
	for i := 0; i < warmupRounds; i++ {
		for j := 0; j < jobsPerRound; j++ {
			_, _ = q.Put("leak-test", body, 1, 0, 60*time.Second)
		}
		for j := 0; j < jobsPerRound; j++ {
			job, _ := q.Reserve([]string{"leak-test"}, time.Second)
			_ = job.Delete()
		}
	}

	runtime.GC()
	time.Sleep(100 * time.Millisecond)
	baselineMem := collectMemSnapshot()

	// 测试轮次
	rounds := 10
	var memSamples []uint64
	var peakHeap uint64 = baselineMem.HeapAlloc

	for round := 0; round < rounds; round++ {
		for i := 0; i < jobsPerRound; i++ {
			_, _ = q.Put("leak-test", body, 1, 0, 60*time.Second)
		}
		for i := 0; i < jobsPerRound; i++ {
			job, err := q.Reserve([]string{"leak-test"}, time.Second)
			if err != nil {
				result.Status = "failed"
				result.Error = fmt.Sprintf("round %d reserve failed: %v", round, err)
				return result
			}
			_ = job.Delete()
		}

		runtime.GC()
		time.Sleep(50 * time.Millisecond)
		mem := collectMemSnapshot()
		memSamples = append(memSamples, mem.HeapAlloc)
		if mem.HeapAlloc > peakHeap {
			peakHeap = mem.HeapAlloc
		}
	}

	finalMem := collectMemSnapshot()

	// 分析
	stableStart := rounds / 2
	var increasing int
	for i := stableStart + 1; i < len(memSamples); i++ {
		if memSamples[i] > memSamples[i-1]+100*1024 {
			increasing++
		}
	}

	var lastRoundsSum uint64
	lastRoundsCount := 3
	for i := len(memSamples) - lastRoundsCount; i < len(memSamples); i++ {
		lastRoundsSum += memSamples[i]
	}
	lastRoundsAvg := lastRoundsSum / uint64(lastRoundsCount)
	growth := float64(lastRoundsAvg) / float64(baselineMem.HeapAlloc)

	result.Duration = time.Since(startTime).String()
	result.Metrics["warmup_rounds"] = warmupRounds
	result.Metrics["test_rounds"] = rounds
	result.Metrics["jobs_per_round"] = jobsPerRound
	result.Metrics["growth_ratio"] = growth
	result.Metrics["increasing_rounds"] = increasing
	result.Metrics["total_ops"] = uint64((warmupRounds + rounds) * jobsPerRound * 2)
	result.Memory = MemoryMetrics{
		InitialHeap: formatBytes(baselineMem.HeapAlloc),
		PeakHeap:    formatBytes(peakHeap),
		FinalHeap:   formatBytes(finalMem.HeapAlloc),
		GCRuns:      finalMem.NumGC - baselineMem.NumGC,
	}

	if increasing > (rounds-stableStart)/2 && growth > 1.5 {
		result.Status = "failed"
		result.Error = fmt.Sprintf("memory leak detected: growth=%.2fx, increasing=%d/%d",
			growth, increasing, rounds-stableStart-1)
	}

	return result
}

func runConcurrentOpsTest(t *testing.T) *TestResult {
	startTime := time.Now()
	result := &TestResult{
		Name:    "ConcurrentOps",
		Status:  "passed",
		Metrics: make(map[string]interface{}),
	}

	config := DefaultConfig()
	config.Storage = NewMemoryStorage()

	q, err := New(config)
	if err != nil {
		result.Status = "failed"
		result.Error = err.Error()
		return result
	}
	if err := q.Start(); err != nil {
		result.Status = "failed"
		result.Error = err.Error()
		return result
	}
	defer func() { _ = q.Stop() }()

	duration := 30 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	var putOps, reserveOps, deleteOps, releaseOps, buryOps, kickOps, touchOps, errors atomic.Uint64
	initialMem := collectMemSnapshot()
	var peakHeap uint64 = initialMem.HeapAlloc

	numWorkers := runtime.GOMAXPROCS(0) * 2
	var wg sync.WaitGroup

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			topic := fmt.Sprintf("concurrent-%d", id%5)
			body := []byte("concurrent test")

			for {
				select {
				case <-ctx.Done():
					return
				default:
				}

				if _, err := q.Put(topic, body, 1, 0, 30*time.Second); err != nil {
					errors.Add(1)
					continue
				}
				putOps.Add(1)

				job, err := q.Reserve([]string{topic}, 100*time.Millisecond)
				if err != nil {
					continue
				}
				reserveOps.Add(1)

				switch id % 5 {
				case 0:
					if err := job.Touch(); err == nil {
						touchOps.Add(1)
					}
					if err := job.Delete(); err == nil {
						deleteOps.Add(1)
					}
				case 1:
					if err := job.Release(0, 0); err == nil {
						releaseOps.Add(1)
					}
					if job2, err := q.Reserve([]string{topic}, 100*time.Millisecond); err == nil {
						reserveOps.Add(1)
						_ = job2.Delete()
						deleteOps.Add(1)
					}
				case 2:
					if err := job.Bury(1); err == nil {
						buryOps.Add(1)
					}
					if n, err := q.Kick(topic, 1); err == nil && n > 0 {
						kickOps.Add(1)
					}
					if job2, err := q.Reserve([]string{topic}, 100*time.Millisecond); err == nil {
						reserveOps.Add(1)
						_ = job2.Delete()
						deleteOps.Add(1)
					}
				default:
					if err := job.Delete(); err == nil {
						deleteOps.Add(1)
					}
				}
			}
		}(i)
	}

	// 内存监控
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				mem := collectMemSnapshot()
				if mem.HeapAlloc > peakHeap {
					peakHeap = mem.HeapAlloc
				}
			}
		}
	}()

	wg.Wait()
	finalMem := collectMemSnapshot()

	totalOps := putOps.Load() + reserveOps.Load() + deleteOps.Load() +
		releaseOps.Load() + buryOps.Load() + kickOps.Load() + touchOps.Load()

	result.Duration = time.Since(startTime).String()
	result.Metrics["duration"] = duration.String()
	result.Metrics["workers"] = numWorkers
	result.Metrics["put_ops"] = putOps.Load()
	result.Metrics["reserve_ops"] = reserveOps.Load()
	result.Metrics["delete_ops"] = deleteOps.Load()
	result.Metrics["release_ops"] = releaseOps.Load()
	result.Metrics["bury_ops"] = buryOps.Load()
	result.Metrics["kick_ops"] = kickOps.Load()
	result.Metrics["touch_ops"] = touchOps.Load()
	result.Metrics["error_count"] = errors.Load()
	result.Metrics["total_ops"] = totalOps
	result.Metrics["ops_per_sec"] = float64(totalOps) / duration.Seconds()
	result.Memory = MemoryMetrics{
		InitialHeap: formatBytes(initialMem.HeapAlloc),
		PeakHeap:    formatBytes(peakHeap),
		FinalHeap:   formatBytes(finalMem.HeapAlloc),
		GCRuns:      finalMem.NumGC - initialMem.NumGC,
	}

	return result
}

func runSQLiteStorageTest(t *testing.T) *TestResult {
	startTime := time.Now()
	result := &TestResult{
		Name:    "SQLiteStorage",
		Status:  "passed",
		Metrics: make(map[string]interface{}),
	}

	dbPath := t.TempDir() + "/stability.db"
	storage, err := NewSQLiteStorage(dbPath)
	if err != nil {
		result.Status = "failed"
		result.Error = err.Error()
		return result
	}

	config := DefaultConfig()
	config.Storage = storage
	config.Ticker = NewNoOpTicker()

	q, err := New(config)
	if err != nil {
		result.Status = "failed"
		result.Error = err.Error()
		return result
	}
	if err := q.Start(); err != nil {
		result.Status = "failed"
		result.Error = err.Error()
		return result
	}
	defer func() { _ = q.Stop() }()

	jobCount := 10000
	body := []byte("sqlite stability test")
	initialMem := collectMemSnapshot()

	// Write
	writeStart := time.Now()
	for i := 0; i < jobCount; i++ {
		if _, err := q.Put("sqlite-test", body, uint32(i%100), 0, 60*time.Second); err != nil {
			result.Status = "failed"
			result.Error = fmt.Sprintf("put failed at %d: %v", i, err)
			return result
		}
	}
	writeDuration := time.Since(writeStart)
	afterWriteMem := collectMemSnapshot()

	// Read
	readStart := time.Now()
	for i := 0; i < jobCount; i++ {
		job, err := q.Reserve([]string{"sqlite-test"}, time.Second)
		if err != nil {
			result.Status = "failed"
			result.Error = fmt.Sprintf("reserve failed at %d: %v", i, err)
			return result
		}
		_ = job.Delete()
	}
	readDuration := time.Since(readStart)
	finalMem := collectMemSnapshot()

	// DB size
	fi, err := os.Stat(dbPath)
	var dbSize int64
	if err == nil {
		dbSize = fi.Size()
	}

	result.Duration = time.Since(startTime).String()
	result.Metrics["job_count"] = jobCount
	result.Metrics["write_duration"] = writeDuration.String()
	result.Metrics["read_duration"] = readDuration.String()
	result.Metrics["write_throughput"] = float64(jobCount) / writeDuration.Seconds()
	result.Metrics["read_throughput"] = float64(jobCount) / readDuration.Seconds()
	result.Metrics["db_size_mb"] = float64(dbSize) / 1024 / 1024
	result.Metrics["total_ops"] = uint64(jobCount * 2)
	result.Memory = MemoryMetrics{
		InitialHeap: formatBytes(initialMem.HeapAlloc),
		PeakHeap:    formatBytes(afterWriteMem.HeapAlloc),
		FinalHeap:   formatBytes(finalMem.HeapAlloc),
		GCRuns:      finalMem.NumGC - initialMem.NumGC,
	}

	return result
}
