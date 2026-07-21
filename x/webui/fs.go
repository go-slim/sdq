package webui

import "embed"

// DistFS 嵌入无需构建即可由浏览器直接运行的前端静态资源。
//
//go:embed frontend/index.html frontend/*.css frontend/src
var DistFS embed.FS
