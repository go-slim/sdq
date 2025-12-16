# SDQ WebUI

基于 Preact + Tailwind CSS 的 SDQ 队列监控 Web 界面。

## 🚀 功能特性

- **Dashboard 主页** - 显示队列概览、操作统计和主题列表
- **Topic 详情页** - 显示特定主题的任务列表，支持状态过滤和分页
- **Job 详情页** - 显示单个任务的完整信息和统计数据
- **实时刷新** - 可配置的自动数据更新
- **响应式设计** - 适配桌面和移动设备
- **Mock API** - 完整的模拟数据，无需后端即可开发

## 🛠️ 技术栈

- **Frontend**: Preact 10.27.2
- **Routing**: preact-iso 2.11.0 (原生 `<a>` 标签导航)
- **Styling**: Tailwind CSS 3.4.0 + PostCSS + Autoprefixer
- **Build**: Vite 7.3.0
- **Mocking**: Vite 内置中间件
- **CSS Processing**: PostCSS 配置处理

## 📦 安装和运行

```bash
# 安装依赖
npm install

# 启动开发服务器
npm run dev

# 构建生产版本
npm run build

# 预览构建结果
npm run preview
```

访问 http://localhost:5174 查看应用。

## 🗂️ 项目结构

```
src/
├── main.jsx          # 应用入口
├── app.jsx           # 主应用组件和路由配置
├── pages/
│   ├── dashboard.jsx     # Dashboard 主页
│   ├── topic-detail.jsx # Topic 详情页
│   ├── job-detail.jsx   # Job 详情页
│   └── _404.jsx         # 404 页面
└── ...
```

## 🔌 Mock API

内置完整的 Mock API，模拟 SDQ 的真实数据结构：

### 队列概览
- `GET /api/overview` - 返回队列整体统计

### 主题管理
- `GET /api/topics` - 返回所有主题
- `GET /api/topics/{topic}` - 返回指定主题统计
- `GET /api/topics/{topic}/jobs` - 返回主题下的任务列表

### 任务管理
- `GET /api/jobs/{id}` - 返回任务详情
- `POST /api/jobs/{id}/kick` - 踢出任务
- `DELETE /api/jobs/{id}` - 删除任务

## 🎨 设计特点

- **现代化 UI**: 使用 Tailwind CSS 构建
- **状态标识**: 不同颜色区分任务状态
- **交互友好**: 加载状态、错误处理、确认提示
- **响应式**: 完美适配各种屏幕尺寸

## 🔧 开发

### 开发命令
```bash
npm run dev          # 启动开发服务器
npm run build         # 构建生产版本
npm run preview       # 预览构建结果
```

### 项目配置
- **Vite 配置**: `vite.config.js`
- **Tailwind 配置**: `tailwind.config.js`
- **PostCSS 配置**: `postcss.config.js`

## 📊 界面预览

### Dashboard
- 队列整体统计（总任务数、各状态分布等）
- 操作统计（Puts、Reserves、Deletes 等）
- 主题列表表格

### Topic 详情
- 主题统计信息卡片
- 任务状态过滤按钮
- 分页任务列表
- 批量操作功能

### Job 详情
- 任务基本信息
- 统计数据
- JSON 格式任务内容
- 操作按钮（Kick、Delete）

## 🔄 切换到真实后端

要连接真实 Go 后端，修改 `vite.config.js`，注释掉 mock 中间件并启用代理：

```javascript
export default defineConfig({
  plugins: [
    // preact(), // 取消注释
    // 注释掉 mock 插件
  ],
  server: {
    proxy: {
      '/api': {
        target: 'http://localhost:8080',
        changeOrigin: true,
      },
    },
  }
});
```

## 📱 响应式支持

- **桌面端**: 完整功能和布局
- **平板端**: 适配布局调整  
- **移动端**: 移动优化布局

## 🚀 部署

```bash
# 构建
npm run build

# dist/ 目录包含构建结果
# 可直接部署到静态文件服务器
```

## 🤝 贡献

1. Fork 项目
2. 创建功能分支
3. 提交更改
4. 发起 Pull Request

## 📄 许可证

MIT License