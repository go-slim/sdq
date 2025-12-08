package inspector

import (
	"context"
	"encoding/json"
	"fmt"
	"html/template"
	"net/http"
	"strconv"
	"strings"
	"time"

	"go-slim.dev/sdq"
)

// Handler 提供 HTTP 接口用于监控队列
type Handler struct {
	inspector *Inspector
	mux       *http.ServeMux
}

// NewHandler 创建 HTTP 处理器
func NewHandler(inspector *Inspector) *Handler {
	h := &Handler{
		inspector: inspector,
		mux:       http.NewServeMux(),
	}
	h.registerRoutes()
	return h
}

// ServeHTTP 实现 http.Handler 接口
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mux.ServeHTTP(w, r)
}

// Mux 返回内部的 ServeMux，方便挂载到其他路由
func (h *Handler) Mux() *http.ServeMux {
	return h.mux
}

func (h *Handler) registerRoutes() {
	// HTML 页面
	h.mux.HandleFunc("GET /", h.handleDashboard)
	h.mux.HandleFunc("GET /topics/{topic}", h.handleTopicDetail)
	h.mux.HandleFunc("GET /jobs/{id}", h.handleJobDetail)

	// JSON API
	h.mux.HandleFunc("GET /api/overview", h.handleAPIOverview)
	h.mux.HandleFunc("GET /api/topics", h.handleAPITopics)
	h.mux.HandleFunc("GET /api/topics/{topic}", h.handleAPITopic)
	h.mux.HandleFunc("GET /api/topics/{topic}/jobs", h.handleAPIJobs)
	h.mux.HandleFunc("GET /api/jobs/{id}", h.handleAPIJob)

	// 操作 API
	h.mux.HandleFunc("POST /api/topics/{topic}/kick", h.handleAPIKick)
	h.mux.HandleFunc("POST /api/topics/{topic}/delete-buried", h.handleAPIDeleteBuried)
	h.mux.HandleFunc("POST /api/jobs/{id}/kick", h.handleAPIKickJob)
	h.mux.HandleFunc("DELETE /api/jobs/{id}", h.handleAPIDeleteJob)
}

// HTML 页面处理器

func (h *Handler) handleDashboard(w http.ResponseWriter, r *http.Request) {
	overview := h.inspector.Overview()
	topics := h.inspector.ListTopics()

	data := map[string]any{
		"Overview": overview,
		"Topics":   topics,
	}

	h.renderHTML(w, dashboardTemplate, data)
}

func (h *Handler) handleTopicDetail(w http.ResponseWriter, r *http.Request) {
	topicName := r.PathValue("topic")
	if topicName == "" {
		http.Error(w, "topic name required", http.StatusBadRequest)
		return
	}

	topic, err := h.inspector.GetTopic(topicName)
	if err != nil {
		http.Error(w, "topic not found", http.StatusNotFound)
		return
	}

	// 解析查询参数
	stateStr := r.URL.Query().Get("state")
	page, _ := strconv.Atoi(r.URL.Query().Get("page"))
	if page < 1 {
		page = 1
	}
	pageSize := 20

	query := &JobQuery{
		Topic:    topicName,
		Page:     page,
		PageSize: pageSize,
		State:    parseState(stateStr),
	}

	result, err := h.inspector.ListJobs(r.Context(), query)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	data := map[string]any{
		"Topic":      topic,
		"Jobs":       result.Jobs,
		"Total":      result.Total,
		"Page":       result.Page,
		"PageSize":   result.PageSize,
		"TotalPages": result.TotalPages,
		"State":      stateStr,
	}

	h.renderHTML(w, topicDetailTemplate, data)
}

func (h *Handler) handleJobDetail(w http.ResponseWriter, r *http.Request) {
	idStr := r.PathValue("id")
	id, err := strconv.ParseUint(idStr, 10, 64)
	if err != nil {
		http.Error(w, "invalid job id", http.StatusBadRequest)
		return
	}

	job, err := h.inspector.GetJob(r.Context(), id, true)
	if err != nil {
		http.Error(w, "job not found", http.StatusNotFound)
		return
	}

	// 获取 body 内容
	body, _ := h.inspector.GetJobBody(r.Context(), id)
	bodyStr := string(body)
	if len(bodyStr) > 10000 {
		bodyStr = bodyStr[:10000] + "\n... (truncated)"
	}

	data := map[string]any{
		"Job":     job,
		"Body":    bodyStr,
		"BodyLen": len(body),
	}

	h.renderHTML(w, jobDetailTemplate, data)
}

// JSON API 处理器

func (h *Handler) handleAPIOverview(w http.ResponseWriter, r *http.Request) {
	overview := h.inspector.Overview()
	h.writeJSON(w, overview)
}

func (h *Handler) handleAPITopics(w http.ResponseWriter, r *http.Request) {
	topics := h.inspector.ListTopics()
	h.writeJSON(w, topics)
}

func (h *Handler) handleAPITopic(w http.ResponseWriter, r *http.Request) {
	topicName := r.PathValue("topic")
	if topicName == "" {
		h.writeError(w, "topic name required", http.StatusBadRequest)
		return
	}

	topic, err := h.inspector.GetTopic(topicName)
	if err != nil {
		h.writeError(w, "topic not found", http.StatusNotFound)
		return
	}

	h.writeJSON(w, topic)
}

func (h *Handler) handleAPIJobs(w http.ResponseWriter, r *http.Request) {
	topicName := r.PathValue("topic")
	if topicName == "" {
		h.writeError(w, "topic name required", http.StatusBadRequest)
		return
	}

	// 解析查询参数
	stateStr := r.URL.Query().Get("state")
	page, _ := strconv.Atoi(r.URL.Query().Get("page"))
	pageSize, _ := strconv.Atoi(r.URL.Query().Get("page_size"))

	if page < 1 {
		page = 1
	}
	if pageSize <= 0 || pageSize > 100 {
		pageSize = 20
	}

	query := &JobQuery{
		Topic:    topicName,
		Page:     page,
		PageSize: pageSize,
		State:    parseState(stateStr),
	}

	result, err := h.inspector.ListJobs(r.Context(), query)
	if err != nil {
		h.writeError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	h.writeJSON(w, result)
}

func (h *Handler) handleAPIJob(w http.ResponseWriter, r *http.Request) {
	idStr := r.PathValue("id")
	id, err := strconv.ParseUint(idStr, 10, 64)
	if err != nil {
		h.writeError(w, "invalid job id", http.StatusBadRequest)
		return
	}

	job, err := h.inspector.GetJob(r.Context(), id, true)
	if err != nil {
		h.writeError(w, "job not found", http.StatusNotFound)
		return
	}

	h.writeJSON(w, job)
}

// 操作 API

func (h *Handler) handleAPIKick(w http.ResponseWriter, r *http.Request) {
	topicName := r.PathValue("topic")
	if topicName == "" {
		h.writeError(w, "topic name required", http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	kicked, err := h.inspector.KickAllBuriedJobs(ctx, topicName)
	if err != nil {
		h.writeError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	h.writeJSON(w, map[string]any{
		"kicked": kicked,
	})
}

func (h *Handler) handleAPIDeleteBuried(w http.ResponseWriter, r *http.Request) {
	topicName := r.PathValue("topic")
	if topicName == "" {
		h.writeError(w, "topic name required", http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	deleted, err := h.inspector.DeleteAllBuriedJobs(ctx, topicName)
	if err != nil {
		h.writeError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	h.writeJSON(w, map[string]any{
		"deleted": deleted,
	})
}

func (h *Handler) handleAPIKickJob(w http.ResponseWriter, r *http.Request) {
	idStr := r.PathValue("id")
	id, err := strconv.ParseUint(idStr, 10, 64)
	if err != nil {
		h.writeError(w, "invalid job id", http.StatusBadRequest)
		return
	}

	if err := h.inspector.KickJob(id); err != nil {
		h.writeError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	h.writeJSON(w, map[string]any{
		"success": true,
	})
}

func (h *Handler) handleAPIDeleteJob(w http.ResponseWriter, r *http.Request) {
	idStr := r.PathValue("id")
	id, err := strconv.ParseUint(idStr, 10, 64)
	if err != nil {
		h.writeError(w, "invalid job id", http.StatusBadRequest)
		return
	}

	if err := h.inspector.DeleteJob(id); err != nil {
		h.writeError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	h.writeJSON(w, map[string]any{
		"success": true,
	})
}

// 辅助方法

func parseState(s string) *sdq.State {
	var state sdq.State
	switch s {
	case "ready":
		state = sdq.StateReady
	case "delayed":
		state = sdq.StateDelayed
	case "reserved":
		state = sdq.StateReserved
	case "buried":
		state = sdq.StateBuried
	case "enqueued":
		state = sdq.StateEnqueued
	default:
		return nil
	}
	return &state
}

func (h *Handler) writeJSON(w http.ResponseWriter, data any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (h *Handler) writeError(w http.ResponseWriter, message string, status int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": message})
}

func (h *Handler) renderHTML(w http.ResponseWriter, tmplStr string, data any) {
	tmpl, err := template.New("page").Funcs(templateFuncs).Parse(tmplStr)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := tmpl.Execute(w, data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

var templateFuncs = template.FuncMap{
	"formatDuration": func(d time.Duration) string {
		if d < time.Minute {
			return fmt.Sprintf("%.0fs", d.Seconds())
		}
		if d < time.Hour {
			return fmt.Sprintf("%.0fm", d.Minutes())
		}
		if d < 24*time.Hour {
			return fmt.Sprintf("%.1fh", d.Hours())
		}
		return fmt.Sprintf("%.1fd", d.Hours()/24)
	},
	"formatUptime": func(s string) string {
		return s
	},
	"formatFloat": func(f float64) string {
		return fmt.Sprintf("%.2f", f)
	},
	"formatTime": func(t time.Time) string {
		if t.IsZero() {
			return "-"
		}
		return t.Format("2006-01-02 15:04:05")
	},
	"sub": func(a, b int) int {
		return a - b
	},
	"add": func(a, b int) int {
		return a + b
	},
	"seq": func(start, end int) []int {
		var result []int
		for i := start; i <= end; i++ {
			result = append(result, i)
		}
		return result
	},
	"truncate": func(s string, n int) string {
		if len(s) <= n {
			return s
		}
		return s[:n] + "..."
	},
}

// HTML 模板

var dashboardTemplate = `<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>SDQ Inspector</title>
    <style>` + cssStyles + `</style>
</head>
<body>
    <div class="container">
        <header>
            <h1>SDQ Inspector</h1>
            <p class="subtitle">Queue Monitoring Dashboard</p>
            <div class="auto-refresh">
                <label>
                    <input type="checkbox" id="autoRefresh"> Auto Refresh
                </label>
                <select id="refreshInterval">
                    <option value="1000">1s</option>
                    <option value="2000">2s</option>
                    <option value="5000" selected>5s</option>
                    <option value="10000">10s</option>
                    <option value="30000">30s</option>
                </select>
                <span id="refreshStatus"></span>
            </div>
        </header>

        <section class="overview">
            <h2>Overview</h2>
            <div class="stats-grid">
                <div class="stat-card">
                    <div class="stat-value">{{.Overview.TotalJobs}}</div>
                    <div class="stat-label">Total Jobs</div>
                </div>
                <div class="stat-card">
                    <div class="stat-value">{{.Overview.ReadyJobs}}</div>
                    <div class="stat-label">Ready</div>
                </div>
                <div class="stat-card">
                    <div class="stat-value">{{.Overview.DelayedJobs}}</div>
                    <div class="stat-label">Delayed</div>
                </div>
                <div class="stat-card">
                    <div class="stat-value">{{.Overview.ReservedJobs}}</div>
                    <div class="stat-label">Reserved</div>
                </div>
                <div class="stat-card">
                    <div class="stat-value">{{.Overview.BuriedJobs}}</div>
                    <div class="stat-label">Buried</div>
                </div>
                <div class="stat-card">
                    <div class="stat-value">{{.Overview.TotalTopics}}</div>
                    <div class="stat-label">Topics</div>
                </div>
                <div class="stat-card">
                    <div class="stat-value">{{formatUptime .Overview.Uptime}}</div>
                    <div class="stat-label">Uptime</div>
                </div>
                <div class="stat-card">
                    {{if .Overview.Throughput}}
                    <div class="stat-value">{{formatFloat .Overview.Throughput.PutsPerSecond}}/s</div>
                    {{else}}
                    <div class="stat-value">-</div>
                    {{end}}
                    <div class="stat-label">Puts</div>
                </div>
            </div>
        </section>

        <section class="topics">
            <h2>Topics</h2>
            {{if .Topics}}
            <table class="data-table">
                <thead>
                    <tr>
                        <th>Name</th>
                        <th>Total</th>
                        <th>Ready</th>
                        <th>Delayed</th>
                        <th>Reserved</th>
                        <th>Buried</th>
                        <th>Actions</th>
                    </tr>
                </thead>
                <tbody>
                    {{range .Topics}}
                    <tr>
                        <td><a href="/topics/{{.Name}}">{{.Name}}</a></td>
                        <td>{{.TotalJobs}}</td>
                        <td>{{.ReadyJobs}}</td>
                        <td>{{.DelayedJobs}}</td>
                        <td>{{.ReservedJobs}}</td>
                        <td>{{.BuriedJobs}}</td>
                        <td>
                            {{if gt .BuriedJobs 0}}
                            <button class="btn btn-sm" onclick="kickAll('{{.Name}}')">Kick All</button>
                            {{end}}
                        </td>
                    </tr>
                    {{end}}
                </tbody>
            </table>
            {{else}}
            <p class="empty">No topics found</p>
            {{end}}
        </section>
    </div>

    <script>
    async function kickAll(topic) {
        if (!confirm('Kick all buried jobs in topic "' + topic + '"?')) return;
        try {
            const resp = await fetch('/api/topics/' + encodeURIComponent(topic) + '/kick', {method: 'POST'});
            const data = await resp.json();
            alert('Kicked ' + data.kicked + ' jobs');
            location.reload();
        } catch (e) {
            alert('Error: ' + e.message);
        }
    }

    // Auto refresh
    let refreshTimer = null;
    const autoRefreshCheckbox = document.getElementById('autoRefresh');
    const refreshIntervalSelect = document.getElementById('refreshInterval');
    const refreshStatus = document.getElementById('refreshStatus');

    function startAutoRefresh() {
        const interval = parseInt(refreshIntervalSelect.value);
        refreshTimer = setInterval(() => {
            refreshStatus.textContent = 'Refreshing...';
            location.reload();
        }, interval);
        refreshStatus.textContent = 'On';
        localStorage.setItem('sdq-auto-refresh', 'true');
        localStorage.setItem('sdq-refresh-interval', refreshIntervalSelect.value);
    }

    function stopAutoRefresh() {
        if (refreshTimer) {
            clearInterval(refreshTimer);
            refreshTimer = null;
        }
        refreshStatus.textContent = '';
        localStorage.setItem('sdq-auto-refresh', 'false');
    }

    autoRefreshCheckbox.addEventListener('change', function() {
        if (this.checked) {
            startAutoRefresh();
        } else {
            stopAutoRefresh();
        }
    });

    refreshIntervalSelect.addEventListener('change', function() {
        if (autoRefreshCheckbox.checked) {
            stopAutoRefresh();
            startAutoRefresh();
        }
        localStorage.setItem('sdq-refresh-interval', this.value);
    });

    // Restore state from localStorage
    if (localStorage.getItem('sdq-refresh-interval')) {
        refreshIntervalSelect.value = localStorage.getItem('sdq-refresh-interval');
    }
    if (localStorage.getItem('sdq-auto-refresh') === 'true') {
        autoRefreshCheckbox.checked = true;
        startAutoRefresh();
    }
    </script>
</body>
</html>`

var topicDetailTemplate = `<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>{{.Topic.Name}} - SDQ Inspector</title>
    <style>` + cssStyles + `</style>
</head>
<body>
    <div class="container">
        <header>
            <h1><a href="/">SDQ Inspector</a> / {{.Topic.Name}}</h1>
        </header>

        <section class="overview">
            <h2>Topic Stats</h2>
            <div class="stats-grid">
                <div class="stat-card">
                    <div class="stat-value">{{.Topic.TotalJobs}}</div>
                    <div class="stat-label">Total</div>
                </div>
                <div class="stat-card">
                    <div class="stat-value">{{.Topic.ReadyJobs}}</div>
                    <div class="stat-label">Ready</div>
                </div>
                <div class="stat-card">
                    <div class="stat-value">{{.Topic.DelayedJobs}}</div>
                    <div class="stat-label">Delayed</div>
                </div>
                <div class="stat-card">
                    <div class="stat-value">{{.Topic.ReservedJobs}}</div>
                    <div class="stat-label">Reserved</div>
                </div>
                <div class="stat-card">
                    <div class="stat-value">{{.Topic.BuriedJobs}}</div>
                    <div class="stat-label">Buried</div>
                </div>
            </div>
            {{if gt .Topic.BuriedJobs 0}}
            <div class="actions">
                <button class="btn" onclick="kickAll()">Kick All Buried</button>
                <button class="btn btn-danger" onclick="deleteAllBuried()">Delete All Buried</button>
            </div>
            {{end}}
        </section>

        <section class="jobs">
            <h2>Jobs ({{.Total}} total)</h2>

            <div class="filters">
                <a href="?state=" class="filter-btn {{if eq .State ""}}active{{end}}">All</a>
                <a href="?state=ready" class="filter-btn {{if eq .State "ready"}}active{{end}}">Ready</a>
                <a href="?state=delayed" class="filter-btn {{if eq .State "delayed"}}active{{end}}">Delayed</a>
                <a href="?state=reserved" class="filter-btn {{if eq .State "reserved"}}active{{end}}">Reserved</a>
                <a href="?state=buried" class="filter-btn {{if eq .State "buried"}}active{{end}}">Buried</a>
            </div>

            {{if .Jobs}}
            <table class="data-table">
                <thead>
                    <tr>
                        <th>ID</th>
                        <th>State</th>
                        <th>Priority</th>
                        <th>Reserves</th>
                        <th>Created</th>
                        <th>Actions</th>
                    </tr>
                </thead>
                <tbody>
                    {{range .Jobs}}
                    <tr>
                        <td><a href="/jobs/{{.ID}}">{{.ID}}</a></td>
                        <td><span class="state-badge state-{{.State}}">{{.State}}</span></td>
                        <td>{{.Priority}}</td>
                        <td>{{.Reserves}}</td>
                        <td>{{formatTime .CreatedAt}}</td>
                        <td>
                            {{if eq .State "buried"}}
                            <button class="btn btn-sm" onclick="kickJob({{.ID}})">Kick</button>
                            {{end}}
                            <button class="btn btn-sm btn-danger" onclick="deleteJob({{.ID}})">Delete</button>
                        </td>
                    </tr>
                    {{end}}
                </tbody>
            </table>

            {{if gt .TotalPages 1}}
            <div class="pagination">
                {{if gt .Page 1}}
                <a href="?state={{.State}}&page={{sub .Page 1}}" class="page-link">Prev</a>
                {{end}}

                {{$currentPage := .Page}}
                {{$state := .State}}
                {{range seq 1 .TotalPages}}
                {{if le . 10}}
                <a href="?state={{$state}}&page={{.}}" class="page-link {{if eq . $currentPage}}active{{end}}">{{.}}</a>
                {{end}}
                {{end}}

                {{if lt .Page .TotalPages}}
                <a href="?state={{.State}}&page={{add .Page 1}}" class="page-link">Next</a>
                {{end}}
            </div>
            {{end}}
            {{else}}
            <p class="empty">No jobs found</p>
            {{end}}
        </section>
    </div>

    <script>
    const topic = '{{.Topic.Name}}';

    async function kickAll() {
        if (!confirm('Kick all buried jobs?')) return;
        try {
            const resp = await fetch('/api/topics/' + encodeURIComponent(topic) + '/kick', {method: 'POST'});
            const data = await resp.json();
            alert('Kicked ' + data.kicked + ' jobs');
            location.reload();
        } catch (e) {
            alert('Error: ' + e.message);
        }
    }

    async function deleteAllBuried() {
        if (!confirm('Delete ALL buried jobs? This cannot be undone!')) return;
        try {
            const resp = await fetch('/api/topics/' + encodeURIComponent(topic) + '/delete-buried', {method: 'POST'});
            const data = await resp.json();
            alert('Deleted ' + data.deleted + ' jobs');
            location.reload();
        } catch (e) {
            alert('Error: ' + e.message);
        }
    }

    async function kickJob(id) {
        try {
            const resp = await fetch('/api/jobs/' + id + '/kick', {method: 'POST'});
            if (!resp.ok) throw new Error('Failed to kick job');
            location.reload();
        } catch (e) {
            alert('Error: ' + e.message);
        }
    }

    async function deleteJob(id) {
        if (!confirm('Delete job ' + id + '?')) return;
        try {
            const resp = await fetch('/api/jobs/' + id, {method: 'DELETE'});
            if (!resp.ok) throw new Error('Failed to delete job');
            location.reload();
        } catch (e) {
            alert('Error: ' + e.message);
        }
    }
    </script>
</body>
</html>`

var jobDetailTemplate = `<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Job {{.Job.ID}} - SDQ Inspector</title>
    <style>` + cssStyles + `</style>
</head>
<body>
    <div class="container">
        <header>
            <h1><a href="/">SDQ Inspector</a> / <a href="/topics/{{.Job.Topic}}">{{.Job.Topic}}</a> / Job {{.Job.ID}}</h1>
        </header>

        <section class="overview">
            <h2>Job Details</h2>
            <div class="job-info">
                <div class="info-row">
                    <span class="info-label">ID</span>
                    <span class="info-value">{{.Job.ID}}</span>
                </div>
                <div class="info-row">
                    <span class="info-label">Topic</span>
                    <span class="info-value"><a href="/topics/{{.Job.Topic}}">{{.Job.Topic}}</a></span>
                </div>
                <div class="info-row">
                    <span class="info-label">State</span>
                    <span class="info-value"><span class="state-badge state-{{.Job.State}}">{{.Job.State}}</span></span>
                </div>
                <div class="info-row">
                    <span class="info-label">Priority</span>
                    <span class="info-value">{{.Job.Priority}}</span>
                </div>
                <div class="info-row">
                    <span class="info-label">Delay</span>
                    <span class="info-value">{{.Job.Delay}}</span>
                </div>
                <div class="info-row">
                    <span class="info-label">TTR</span>
                    <span class="info-value">{{.Job.TTR}}</span>
                </div>
                <div class="info-row">
                    <span class="info-label">Age</span>
                    <span class="info-value">{{.Job.Age}}</span>
                </div>
                <div class="info-row">
                    <span class="info-label">Created At</span>
                    <span class="info-value">{{formatTime .Job.CreatedAt}}</span>
                </div>
                {{if not .Job.ReadyAt.IsZero}}
                <div class="info-row">
                    <span class="info-label">Ready At</span>
                    <span class="info-value">{{formatTime .Job.ReadyAt}}</span>
                </div>
                {{end}}
                {{if not .Job.ReservedAt.IsZero}}
                <div class="info-row">
                    <span class="info-label">Reserved At</span>
                    <span class="info-value">{{formatTime .Job.ReservedAt}}</span>
                </div>
                {{end}}
                {{if not .Job.BuriedAt.IsZero}}
                <div class="info-row">
                    <span class="info-label">Buried At</span>
                    <span class="info-value">{{formatTime .Job.BuriedAt}}</span>
                </div>
                {{end}}
                {{if .Job.TimeUntilReady}}
                <div class="info-row">
                    <span class="info-label">Time Until Ready</span>
                    <span class="info-value">{{.Job.TimeUntilReady}}</span>
                </div>
                {{end}}
                {{if .Job.TimeUntilTimeout}}
                <div class="info-row">
                    <span class="info-label">Time Until Timeout</span>
                    <span class="info-value">{{.Job.TimeUntilTimeout}}</span>
                </div>
                {{end}}
            </div>
        </section>

        <section>
            <h2>Statistics</h2>
            <div class="stats-grid">
                <div class="stat-card">
                    <div class="stat-value">{{.Job.Reserves}}</div>
                    <div class="stat-label">Reserves</div>
                </div>
                <div class="stat-card">
                    <div class="stat-value">{{.Job.Timeouts}}</div>
                    <div class="stat-label">Timeouts</div>
                </div>
                <div class="stat-card">
                    <div class="stat-value">{{.Job.Releases}}</div>
                    <div class="stat-label">Releases</div>
                </div>
                <div class="stat-card">
                    <div class="stat-value">{{.Job.Buries}}</div>
                    <div class="stat-label">Buries</div>
                </div>
                <div class="stat-card">
                    <div class="stat-value">{{.Job.Kicks}}</div>
                    <div class="stat-label">Kicks</div>
                </div>
                <div class="stat-card">
                    <div class="stat-value">{{.Job.Touches}}</div>
                    <div class="stat-label">Touches</div>
                </div>
            </div>
        </section>

        <section>
            <h2>Body ({{.BodyLen}} bytes)</h2>
            <pre class="job-body">{{.Body}}</pre>
        </section>

        <section>
            <h2>Actions</h2>
            <div class="actions">
                {{if eq .Job.State "buried"}}
                <button class="btn" onclick="kickJob()">Kick Job</button>
                {{end}}
                <button class="btn btn-danger" onclick="deleteJob()">Delete Job</button>
            </div>
        </section>
    </div>

    <script>
    const jobId = {{.Job.ID}};

    async function kickJob() {
        try {
            const resp = await fetch('/api/jobs/' + jobId + '/kick', {method: 'POST'});
            if (!resp.ok) throw new Error('Failed to kick job');
            alert('Job kicked successfully');
            location.reload();
        } catch (e) {
            alert('Error: ' + e.message);
        }
    }

    async function deleteJob() {
        if (!confirm('Delete job ' + jobId + '? This cannot be undone!')) return;
        try {
            const resp = await fetch('/api/jobs/' + jobId, {method: 'DELETE'});
            if (!resp.ok) throw new Error('Failed to delete job');
            alert('Job deleted successfully');
            window.location.href = '/';
        } catch (e) {
            alert('Error: ' + e.message);
        }
    }
    </script>
</body>
</html>`

var cssStyles = strings.ReplaceAll(`
* { box-sizing: border-box; margin: 0; padding: 0; }
body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; background: #f5f5f5; color: #333; line-height: 1.6; }
.container { max-width: 1200px; margin: 0 auto; padding: 20px; }
header { margin-bottom: 30px; }
header h1 { font-size: 24px; color: #1a1a1a; }
header h1 a { color: inherit; text-decoration: none; }
header h1 a:hover { text-decoration: underline; }
.subtitle { color: #666; font-size: 14px; }
section { background: #fff; border-radius: 8px; padding: 20px; margin-bottom: 20px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }
section h2 { font-size: 18px; margin-bottom: 15px; color: #1a1a1a; }
.stats-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(120px, 1fr)); gap: 15px; }
.stat-card { background: #f8f9fa; border-radius: 6px; padding: 15px; text-align: center; }
.stat-value { font-size: 24px; font-weight: 600; color: #1a1a1a; }
.stat-label { font-size: 12px; color: #666; text-transform: uppercase; }
.data-table { width: 100%; border-collapse: collapse; }
.data-table th, .data-table td { padding: 12px; text-align: left; border-bottom: 1px solid #eee; }
.data-table th { font-weight: 600; color: #666; font-size: 12px; text-transform: uppercase; }
.data-table tr:hover { background: #f8f9fa; }
.data-table a { color: #0066cc; text-decoration: none; }
.data-table a:hover { text-decoration: underline; }
.state-badge { display: inline-block; padding: 2px 8px; border-radius: 4px; font-size: 12px; font-weight: 500; }
.state-ready { background: #d4edda; color: #155724; }
.state-delayed { background: #fff3cd; color: #856404; }
.state-reserved { background: #cce5ff; color: #004085; }
.state-buried { background: #f8d7da; color: #721c24; }
.btn { display: inline-block; padding: 8px 16px; background: #0066cc; color: #fff; border: none; border-radius: 4px; cursor: pointer; font-size: 14px; }
.btn:hover { background: #0052a3; }
.btn-sm { padding: 4px 8px; font-size: 12px; }
.btn-danger { background: #dc3545; }
.btn-danger:hover { background: #c82333; }
.actions { margin-top: 15px; display: flex; gap: 10px; }
.filters { margin-bottom: 15px; display: flex; gap: 8px; flex-wrap: wrap; }
.filter-btn { display: inline-block; padding: 6px 12px; background: #e9ecef; color: #495057; border-radius: 4px; text-decoration: none; font-size: 13px; }
.filter-btn:hover { background: #dee2e6; }
.filter-btn.active { background: #0066cc; color: #fff; }
.pagination { margin-top: 20px; display: flex; gap: 5px; justify-content: center; }
.page-link { display: inline-block; padding: 6px 12px; background: #fff; border: 1px solid #dee2e6; color: #0066cc; border-radius: 4px; text-decoration: none; }
.page-link:hover { background: #e9ecef; }
.page-link.active { background: #0066cc; color: #fff; border-color: #0066cc; }
.empty { color: #666; font-style: italic; }
.job-info { display: grid; gap: 8px; }
.info-row { display: flex; padding: 8px 0; border-bottom: 1px solid #eee; }
.info-label { width: 150px; font-weight: 600; color: #666; font-size: 13px; }
.info-value { flex: 1; }
.job-body { background: #f8f9fa; padding: 15px; border-radius: 6px; overflow-x: auto; font-size: 13px; white-space: pre-wrap; word-break: break-all; max-height: 400px; overflow-y: auto; }
.auto-refresh { display: flex; align-items: center; gap: 8px; margin-top: 10px; font-size: 13px; }
.auto-refresh label { display: flex; align-items: center; gap: 4px; cursor: pointer; }
.auto-refresh select { padding: 4px 8px; border: 1px solid #ddd; border-radius: 4px; font-size: 13px; }
#refreshStatus { color: #28a745; font-weight: 500; }
`, "\n", "")
