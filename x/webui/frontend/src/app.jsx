import { LocationProvider, Router, Route, hydrate } from 'preact-iso';
import { Layout } from './components/layout.jsx';
import { Dashboard } from './pages/dashboard.jsx';
import { Topics } from './pages/topics.jsx';
import { Storage } from './pages/storage.jsx';
import { Metrics } from './pages/metrics.jsx';
import { TopicDetail } from './pages/topic-detail.jsx';
import { JobDetail } from './pages/job-detail.jsx';
import { Settings } from './pages/settings.jsx';
import { NotFound } from './pages/_404.jsx';
import './app.css';

// 获取 base path（从 <base> 标签，去掉尾部斜杠用于路由匹配）
function getBasePath() {
  const base = document.querySelector('base');
  if (!base) return '';
  const href = base.getAttribute('href') || '/';
  // 返回不带尾部斜杠的路径，用于路由前缀
  // 例如 "/sdq/" -> "/sdq", "/" -> ""
  return href === '/' ? '' : href.replace(/\/$/, '');
}

const BASE = getBasePath();

export function App() {
  return (
    <LocationProvider>
      <Layout>
        <Router>
          <Route path={`${BASE}/`} component={Dashboard} />
          <Route path={`${BASE}/topics`} component={Topics} />
          <Route path={`${BASE}/topics/:topic`} component={TopicDetail} />
          <Route path={`${BASE}/jobs/:id`} component={JobDetail} />
          <Route path={`${BASE}/storage`} component={Storage} />
          <Route path={`${BASE}/metrics`} component={Metrics} />
          <Route path={`${BASE}/settings`} component={Settings} />
          <Route default component={NotFound} />
        </Router>
      </Layout>
    </LocationProvider>
  );
}

hydrate(<App />);

export async function prerender(data) {
  return <App {...data} />;
}
