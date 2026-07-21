# SDQ WebUI

SDQ WebUI 是由 `x/webui` 直接嵌入并提供服务的队列管理界面。

前端使用原生 ES modules 和 importmap 加载 Vue、Lucide，不需要 Node.js、包管理器或构建
步骤。`style.css` 是随源码维护的静态样式文件，浏览器会直接加载 `src` 中的组件模块。新增
模板类名时必须确认 `style.css` 已包含对应规则；这里没有 Tailwind 构建器自动扫描模板并
生成缺失样式。

## 运行

在仓库根目录运行：

```bash
go run ./examples/webui
```

然后访问 `http://localhost:8081/sdq/`。示例服务同时提供测试数据、JSON API 和前端静态
资源，修改 JavaScript 或 CSS 后重新启动 Go 服务即可验证。

## 目录

```text
frontend/
├── index.html        # importmap 与应用入口
├── style.css         # 浏览器直接加载的完整样式
└── src/
    ├── main.js       # Vue 应用入口
    ├── app.js        # 主应用装配
    ├── i18n.js       # 中英文词典、插值和 locale 格式化
    ├── router.js     # History API 路由
    ├── components/   # 展示组件
    ├── stores/       # 用户偏好状态
    ├── utils/        # API 与格式化工具
    └── views/        # 页面视图
```

## 资源加载

`index.html` 的 importmap 固定 Vue 和 Lucide 版本。Lucide 按图标模块加载，避免浏览器下载
完整图标集合。部署到子路径时，[`Handler`](../handler.go) 会替换页面中的 `<base>`，因此
模块、样式、页面路由和 API 都相对于当前挂载路径解析。

运行界面需要客户端可以访问 importmap 中声明的 CDN。若部署环境不能访问公网，应将对应
ESM 文件作为静态资源纳入 `frontend`，并把 importmap 改为相对路径。

CDN 模块会以页面自身的权限运行，并能访问同源管理 API。当前 importmap 固定了版本，但
importmap 模块没有 Subresource Integrity 校验；对供应链和可用性要求较高的部署应自行托管
并通过发布流程校验这些文件。

前端源码通过 Go `embed` 编入可执行文件。修改 JavaScript、CSS 或 importmap 后需要重新
启动或重新构建 Go 程序；浏览器刷新不会读取工作目录中的新文件。

## 部署与安全注意事项

WebUI 是队列管理界面，不是只读监控页。它提供 Kick、Delete 等写操作，并允许读取任务原始
Body；Body 可能包含业务数据、令牌或其他敏感信息。`x/webui` 不内置身份认证、授权、CSRF
防护、TLS、审计和限流，生产部署必须：

- 挂载在应用已有的管理员认证与授权中间件之后，不直接暴露到不可信网络；
- 使用 Cookie 认证时，由外层应用提供 CSRF 防护；
- 根据任务数据敏感度限制 Body 查询权限并记录管理操作审计；
- 在离线或严格 CSP 环境中自行托管 importmap 依赖，避免运行时依赖公网 CDN。

二进制 Body 会在 API 响应中编码为 base64，网络和浏览器内存占用约增加三分之一。应结合
队列的 `MaxJobSize` 和管理入口限流控制单次读取及并发读取成本。

界面依赖原生 ES modules、importmap、History API 和现代 JavaScript 语法，应使用仍在维护的
现代浏览器。

## 功能

- 队列概览和 Topic 列表
- Topic 状态筛选与任务分页
- Job 详情、Kick 和 Delete 操作
- Storage 与 Metrics 视图
- 主题、颜色、轮询间隔和侧栏偏好设置
- 中英文界面、系统语言回退以及本地化日期和数字
- 桌面与移动端响应式布局

## 数据来源

页面不包含 Mock 数据。队列、Topic、Job、任务 Body、运行指标和存储统计都来自 `x/webui`
提供的实时 API。Metrics 中的入队速率趋势仅保留页面打开期间采集到的真实快照；SDQ 目前
不持久化历史指标，因此页面不会补造打开前的趋势数据。Storage 只展示底层 Storage 接口
实际报告的字段，不推测文件路径、连接数、备份状态或读写性能。

Settings 中的轮询间隔控制 Metrics 页面，设置为 0 时关闭其自动轮询；Dashboard 的自动刷新
开关和间隔是该页面独立的临时状态。缩短间隔会按浏览器标签页数量线性增加 API 和队列统计
查询负载。

CI 对前端执行 `node --check`，只能发现单文件语法错误。它不会解析 importmap、访问 CDN，
也不会验证浏览器路由和组件运行时行为；依赖或路由变更仍需运行示例进行浏览器冒烟测试。
