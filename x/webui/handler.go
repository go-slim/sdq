package webui

import (
	"bytes"
	"encoding/json"
	"io/fs"
	"net/http"
	"strconv"
	"strings"

	"go-slim.dev/sdq"
)

// Handler 提供 HTTP API 接口和静态文件服务用于监控队列
//
// 使用示例：
//
//	query := webui.NewQuery(q)
//	handler := webui.NewHandler(query)
//
//	// 无前缀，直接使用（API + 静态文件）
//	http.Handle("/", handler)
//
//	// 添加路由前缀
//	http.Handle("/sdq/", http.StripPrefix("/sdq", handler))
//
// 可用的 API 路由：
//   - GET  /api/overview                      获取队列概览
//   - GET  /api/topics                        获取所有 Topic 列表
//   - GET  /api/topics/{topic}                获取单个 Topic 详情
//   - GET  /api/topics/{topic}/jobs           获取 Topic 的任务列表
//   - GET  /api/jobs/{id}                     获取单个任务详情
//   - POST /api/topics/{topic}/kick           踢出 Topic 所有埋葬任务
//   - POST /api/topics/{topic}/delete-buried  删除 Topic 所有埋葬任务
//   - POST /api/jobs/{id}/kick                踢出单个埋葬任务
//   - DELETE /api/jobs/{id}                   删除单个任务
//
// 静态文件路由：
//   - GET  /                                  前端 SPA 页面
type Handler struct {
	query      *Query
	mux        *http.ServeMux
	staticFS   fs.FS
	fileServer http.Handler
	basePath   string // 部署的基础路径，例如 "/sdq/" 或 "/"
}

// NewHandler 创建 HTTP 处理器
func NewHandler(query *Query) *Handler {
	return NewHandlerWithBasePath(query, "/")
}

// NewHandlerWithBasePath 创建带有自定义基础路径的 HTTP 处理器
// basePath 用于支持子路径部署，例如 "/sdq/"
func NewHandlerWithBasePath(query *Query, basePath string) *Handler {
	// 确保 basePath 以 / 结尾
	if basePath != "/" && !strings.HasSuffix(basePath, "/") {
		basePath += "/"
	}
	// 创建静态文件服务
	distFS, err := fs.Sub(DistFS, "frontend/dist")
	if err != nil {
		panic(err)
	}

	h := &Handler{
		query:      query,
		mux:        http.NewServeMux(),
		staticFS:   distFS,
		fileServer: http.FileServer(http.FS(distFS)),
		basePath:   basePath,
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

	// 静态文件服务（SPA 路由支持）
	h.mux.HandleFunc("/", h.handleStatic)
}

// ============================================================
// API 处理器
// ============================================================

// handleAPIOverview 获取队列概览
func (h *Handler) handleAPIOverview(w http.ResponseWriter, r *http.Request) {
	overview := h.query.Overview()
	h.writeJSON(w, overview)
}

// handleAPITopics 获取所有 Topic 列表
func (h *Handler) handleAPITopics(w http.ResponseWriter, r *http.Request) {
	topics := h.query.ListTopics()
	h.writeJSON(w, topics)
}

// handleAPITopic 获取单个 Topic 详情
func (h *Handler) handleAPITopic(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("topic")
	if topic == "" {
		h.writeError(w, http.StatusBadRequest, "topic name is required")
		return
	}

	info, err := h.query.GetTopic(topic)
	if err != nil {
		h.writeError(w, http.StatusNotFound, err.Error())
		return
	}

	h.writeJSON(w, info)
}

// handleAPIJobs 获取任务列表
func (h *Handler) handleAPIJobs(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("topic")
	query := r.URL.Query()

	// 解析查询参数
	page, _ := strconv.Atoi(query.Get("page"))
	pageSize, _ := strconv.Atoi(query.Get("page_size"))
	state := parseState(query.Get("state"))

	filter := &JobFilter{
		Topic:    topic,
		State:    state,
		Page:     page,
		PageSize: pageSize,
		OrderBy:  query.Get("order_by"),
		Order:    query.Get("order"),
	}

	result, err := h.query.ListJobs(r.Context(), filter)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	h.writeJSON(w, result)
}

// handleAPIJob 获取单个任务详情
func (h *Handler) handleAPIJob(w http.ResponseWriter, r *http.Request) {
	idStr := r.PathValue("id")
	id, err := strconv.ParseUint(idStr, 10, 64)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, "invalid job id")
		return
	}

	job, err := h.query.GetJob(r.Context(), id, true)
	if err != nil {
		h.writeError(w, http.StatusNotFound, err.Error())
		return
	}

	h.writeJSON(w, job)
}

// handleAPIKick 踢出 Topic 所有埋葬任务
func (h *Handler) handleAPIKick(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("topic")
	if topic == "" {
		h.writeError(w, http.StatusBadRequest, "topic name is required")
		return
	}

	kicked, err := h.query.KickAllBuriedJobs(r.Context(), topic)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	h.writeJSON(w, map[string]any{
		"kicked": kicked,
	})
}

// handleAPIDeleteBuried 删除 Topic 所有埋葬任务
func (h *Handler) handleAPIDeleteBuried(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("topic")
	if topic == "" {
		h.writeError(w, http.StatusBadRequest, "topic name is required")
		return
	}

	deleted, err := h.query.DeleteAllBuriedJobs(r.Context(), topic)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	h.writeJSON(w, map[string]any{
		"deleted": deleted,
	})
}

// handleAPIKickJob 踢出单个埋葬任务
func (h *Handler) handleAPIKickJob(w http.ResponseWriter, r *http.Request) {
	idStr := r.PathValue("id")
	id, err := strconv.ParseUint(idStr, 10, 64)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, "invalid job id")
		return
	}

	if err := h.query.KickJob(id); err != nil {
		h.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	h.writeJSON(w, map[string]any{
		"success": true,
	})
}

// handleAPIDeleteJob 删除单个任务
func (h *Handler) handleAPIDeleteJob(w http.ResponseWriter, r *http.Request) {
	idStr := r.PathValue("id")
	id, err := strconv.ParseUint(idStr, 10, 64)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, "invalid job id")
		return
	}

	if err := h.query.ForceDeleteJob(id); err != nil {
		h.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	h.writeJSON(w, map[string]any{
		"success": true,
	})
}

// ============================================================
// 静态文件处理器
// ============================================================

// handleStatic 处理静态文件请求（SPA 支持）
func (h *Handler) handleStatic(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path

	// 规范化路径
	cleanPath := strings.TrimPrefix(path, "/")

	// 如果是根路径或 index.html，返回注入了 base 标签的 HTML
	if cleanPath == "" || cleanPath == "index.html" {
		h.serveIndexHTML(w, r)
		return
	}

	// 检查文件是否存在
	if _, err := fs.Stat(h.staticFS, cleanPath); err == nil {
		// 文件存在，直接服务
		h.fileServer.ServeHTTP(w, r)
		return
	}

	// 文件不存在，检查是否是静态资源请求（JS/CSS/图片等）
	// 如果是静态资源请求但文件不存在，返回 404
	if strings.HasPrefix(path, "/assets/") ||
		strings.HasSuffix(path, ".js") ||
		strings.HasSuffix(path, ".css") ||
		strings.HasSuffix(path, ".png") ||
		strings.HasSuffix(path, ".jpg") ||
		strings.HasSuffix(path, ".svg") ||
		strings.HasSuffix(path, ".ico") {
		http.NotFound(w, r)
		return
	}

	// 文件不存在且不是静态资源，返回 index.html（SPA 路由）
	// 但 API 路由除外
	if strings.HasPrefix(path, "/api/") {
		http.NotFound(w, r)
		return
	}

	// 返回 index.html
	h.serveIndexHTML(w, r)
}

// serveIndexHTML 返回注入了 base 标签的 index.html
func (h *Handler) serveIndexHTML(w http.ResponseWriter, r *http.Request) {
	// 读取 index.html 内容
	indexData, err := fs.ReadFile(h.staticFS, "index.html")
	if err != nil {
		http.NotFound(w, r)
		return
	}

	// 在 <head> 标签后注入 <base> 标签以支持子路径部署
	baseTag := []byte(`<base href="` + h.basePath + `">`)
	modifiedHTML := bytes.Replace(indexData, []byte("<head>"), append([]byte("<head>\n    "), baseTag...), 1)

	// 设置正确的 Content-Type
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Content-Length", strconv.Itoa(len(modifiedHTML)))
	_, _ = w.Write(modifiedHTML)
}

// ============================================================
// 辅助函数
// ============================================================

// parseState 解析状态字符串
func parseState(s string) *sdq.State {
	if s == "" {
		return nil
	}

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
	default:
		return nil
	}

	return &state
}

// writeJSON 写入 JSON 响应
func (h *Handler) writeJSON(w http.ResponseWriter, data any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(data)
}

// writeError 写入错误响应
func (h *Handler) writeError(w http.ResponseWriter, code int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{
		"error": message,
	})
}
