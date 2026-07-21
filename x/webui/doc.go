// Package webui 提供 SDQ 的管理型 HTTP API 和嵌入式浏览器界面。
//
// Handler 同时包含只读查询和 Kick、Delete 等会修改运行中队列的操作，并可返回任务原始
// Body。包本身不实现身份认证、授权、CSRF 防护、TLS、审计或限流。生产环境必须把 Handler
// 挂载在应用已有的认证和管理权限中间件之后；使用 Cookie 认证时还必须由外层提供 CSRF
// 防护。不要将它直接暴露到不可信网络。
//
// 前端使用浏览器原生 ES modules 和 importmap，不需要构建步骤。默认 importmap 从公网 CDN
// 加载固定版本依赖；这些模块以页面自身权限运行。离线、供应链要求较高或受严格 CSP 约束
// 的部署应自行托管这些模块并修改 importmap。
package webui
