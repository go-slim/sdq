package webui

import "embed"

// DistFS 嵌入的前端静态资源（dist 目录）
//
//go:embed frontend/dist
var DistFS embed.FS
