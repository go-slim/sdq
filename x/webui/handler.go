package webui

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"html"
	"io/fs"
	"net/http"
	"strconv"
	"strings"
	"unicode/utf8"

	"go-slim.dev/sdq"
)

// Handler 提供 HTTP API 接口和静态文件服务用于监控队列
//
// 注意：Handler 不包含认证、授权或 CSRF 防护，且部分接口会修改队列或返回任务原始 Body。
// 生产环境必须通过外层中间件限制为可信管理员访问，不能直接暴露到公网。
//
// 使用示例：
//
//	query := webui.NewQuery(q)
//	handler := webui.NewHandler(query)
//
//	// 无前缀，直接使用（API + 静态文件）
//	http.Handle("/", handler)
//
//	// 添加路由前缀。浏览器侧 basePath 必须与实际挂载路径一致。
//	prefixed := webui.NewHandlerWithBasePath(query, "/sdq/")
//	http.Handle("/sdq/", http.StripPrefix("/sdq", prefixed))
//
// 可用的 API 路由：
//   - GET  /api/overview                      获取队列概览
//   - GET  /api/topics                        获取所有 Topic 列表
//   - GET  /api/topics/{topic}                获取单个 Topic 详情
//   - GET  /api/topics/{topic}/jobs           获取 Topic 的任务列表
//   - GET  /api/jobs/{id}                     获取单个任务详情
//   - GET  /api/jobs/{id}/body                获取单个任务的真实 Body
//   - GET  /api/metrics                       获取当前队列运行快照
//   - GET  /api/storage                       获取当前存储统计
//   - POST /api/topics/{topic}/kick           踢出 Topic 所有埋葬任务
//   - POST /api/topics/{topic}/delete-buried  删除 Topic 所有埋葬任务
//   - POST /api/jobs/{id}/kick                踢出单个埋葬任务
//   - DELETE /api/jobs/{id}                   强制删除任意状态的单个任务（不可逆）
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

// NewHandlerWithBasePath 创建带有自定义基础路径的 HTTP 处理器。
//
// basePath 是浏览器可见的挂载路径，例如 "/sdq/"，不是 StripPrefix 后 Handler 收到的路径。
// 空值按 "/" 处理；缺少开头或结尾的斜杠时会自动补齐。
func NewHandlerWithBasePath(query *Query, basePath string) *Handler {
	basePath = normalizeBasePath(basePath)
	// 前端使用原生 ES modules 和 importmap，不需要预先构建。
	frontendFS, err := fs.Sub(DistFS, "frontend")
	if err != nil {
		panic(err)
	}

	h := &Handler{
		query:      query,
		mux:        http.NewServeMux(),
		staticFS:   frontendFS,
		fileServer: http.FileServer(http.FS(frontendFS)),
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
	h.mux.HandleFunc("GET /api/jobs/{id}/body", h.handleAPIJobBody)
	h.mux.HandleFunc("GET /api/metrics", h.handleAPIMetrics)
	h.mux.HandleFunc("GET /api/storage", h.handleAPIStorage)

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
	state, validState := parseState(query.Get("state"))
	if !validState {
		h.writeError(w, http.StatusBadRequest, "invalid job state")
		return
	}

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

type jobBodyResponse struct {
	// Body 是原始任务 Body 的 UTF-8 文本，或者当 Encoding 为 base64 时的编码文本。
	Body string `json:"body"`
	// Encoding 为 utf-8 或 base64。UTF-8 内容不保证是 JSON。
	Encoding string `json:"encoding"`
	// Size 是编码前原始 Body 的字节数。
	Size int `json:"size"`
}

// handleAPIJobBody 获取任务实际保存的 Body。
//
// 非 UTF-8 Body 会整体编码为 base64，响应大小约增加三分之一。外层服务应结合 MaxJobSize
// 设置响应大小、权限和访问频率限制。
func (h *Handler) handleAPIJobBody(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseUint(r.PathValue("id"), 10, 64)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, "invalid job id")
		return
	}

	body, err := h.query.GetJobBody(r.Context(), id)
	if err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, sdq.ErrNotFound) {
			status = http.StatusNotFound
		}
		h.writeError(w, status, err.Error())
		return
	}

	response := jobBodyResponse{
		Body:     string(body),
		Encoding: "utf-8",
		Size:     len(body),
	}
	if !utf8.Valid(body) {
		response.Body = base64.StdEncoding.EncodeToString(body)
		response.Encoding = "base64"
	}
	h.writeJSON(w, response)
}

// handleAPIMetrics 获取同一时刻附近的队列概览和 Topic 状态。
func (h *Handler) handleAPIMetrics(w http.ResponseWriter, _ *http.Request) {
	h.writeJSON(w, h.query.TakeSnapshot())
}

// handleAPIStorage 获取存储实现实际报告的统计信息。
func (h *Handler) handleAPIStorage(w http.ResponseWriter, r *http.Request) {
	info, err := h.query.Storage(r.Context())
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	h.writeJSON(w, info)
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

// handleAPIDeleteJob 强制删除任意状态的单个任务。该操作不可逆。
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

// serveIndexHTML 返回配置了 base 标签的 index.html。
func (h *Handler) serveIndexHTML(w http.ResponseWriter, r *http.Request) {
	// 读取 index.html 内容
	indexData, err := fs.ReadFile(h.staticFS, "index.html")
	if err != nil {
		http.NotFound(w, r)
		return
	}

	// 前端自带根路径 base，部署到子路径时替换该默认值；兼容没有默认 base 的旧页面时，
	// 仍在 head 后注入。
	baseTag := []byte(`<base href="` + html.EscapeString(h.basePath) + `">`)
	modifiedHTML := bytes.Replace(indexData, []byte(`<base href="/">`), baseTag, 1)
	if bytes.Equal(modifiedHTML, indexData) {
		modifiedHTML = bytes.Replace(
			indexData,
			[]byte("<head>"),
			append([]byte("<head>\n    "), baseTag...),
			1,
		)
	}

	// 设置正确的 Content-Type
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Content-Length", strconv.Itoa(len(modifiedHTML)))
	_, _ = w.Write(modifiedHTML)
}

func normalizeBasePath(basePath string) string {
	basePath = strings.TrimSpace(basePath)
	if basePath == "" || basePath == "/" {
		return "/"
	}
	if !strings.HasPrefix(basePath, "/") {
		basePath = "/" + basePath
	}
	if !strings.HasSuffix(basePath, "/") {
		basePath += "/"
	}
	return basePath
}

// ============================================================
// 辅助函数
// ============================================================

// parseState 解析状态字符串。空值表示不限状态；第二个返回值报告非空值是否有效。
func parseState(s string) (*sdq.State, bool) {
	if s == "" {
		return nil, true
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
		return nil, false
	}

	return &state, true
}

// writeJSON 写入 JSON 响应
func (h *Handler) writeJSON(w http.ResponseWriter, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(w).Encode(data)
}

// writeError 写入错误响应
func (h *Handler) writeError(w http.ResponseWriter, code int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{
		"error": message,
	})
}
