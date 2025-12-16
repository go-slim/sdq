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

export function App() {
  return (
    <LocationProvider>
      <Layout>
        <Router>
          <Route path="/" component={Dashboard} />
          <Route path="/topics" component={Topics} />
          <Route path="/topics/:topic" component={TopicDetail} />
          <Route path="/jobs/:id" component={JobDetail} />
          <Route path="/storage" component={Storage} />
          <Route path="/metrics" component={Metrics} />
          <Route path="/settings" component={Settings} />
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
