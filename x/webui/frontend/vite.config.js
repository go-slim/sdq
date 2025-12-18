import { defineConfig } from 'vite';
import preact from '@preact/preset-vite';
import tailwindcss from '@tailwindcss/vite'

// Mock API endpoints
const mockApiRoutes = (server) => {
  // Generate mock data
  const generateMockStats = () => ({
    TotalJobs: Math.floor(Math.random() * 2000) + 500,
    ReadyJobs: Math.floor(Math.random() * 100) + 20,
    DelayedJobs: Math.floor(Math.random() * 50) + 5,
    ReservedJobs: Math.floor(Math.random() * 30) + 5,
    BuriedJobs: Math.floor(Math.random() * 10),
    Topics: 5,
    TotalWaitingWorkers: Math.floor(Math.random() * 10) + 2,
    Uptime: 216720,
    Puts: Math.floor(Math.random() * 10000) + 5000,
    Reserves: Math.floor(Math.random() * 8000) + 4000,
    Deletes: Math.floor(Math.random() * 7000) + 3500,
    Releases: Math.floor(Math.random() * 500) + 100,
    Buries: Math.floor(Math.random() * 100) + 20,
    Kicks: Math.floor(Math.random() * 80) + 10,
    Timeouts: Math.floor(Math.random() * 200) + 50,
    Touches: Math.floor(Math.random() * 300) + 80,
    Throughput: {
      PutsPerSecond: (Math.random() * 5 + 1).toFixed(2),
      ReservesPerSecond: (Math.random() * 4 + 0.5).toFixed(2),
    }
  });

  const generateMockTopics = () => [
    {
      Name: 'email-processing',
      TotalJobs: Math.floor(Math.random() * 200) + 100,
      ReadyJobs: Math.floor(Math.random() * 30) + 10,
      ReservedJobs: Math.floor(Math.random() * 8) + 2,
      DelayedJobs: Math.floor(Math.random() * 5) + 1,
      BuriedJobs: Math.floor(Math.random() * 3)
    },
    {
      Name: 'image-upload',
      TotalJobs: Math.floor(Math.random() * 150) + 80,
      ReadyJobs: Math.floor(Math.random() * 25) + 8,
      ReservedJobs: Math.floor(Math.random() * 6) + 1,
      DelayedJobs: Math.floor(Math.random() * 4),
      BuriedJobs: Math.floor(Math.random() * 2)
    },
    {
      Name: 'data-sync',
      TotalJobs: Math.floor(Math.random() * 180) + 90,
      ReadyJobs: Math.floor(Math.random() * 35) + 12,
      ReservedJobs: Math.floor(Math.random() * 5) + 1,
      DelayedJobs: Math.floor(Math.random() * 3),
      BuriedJobs: 0
    }
  ];

  const generateMockJobs = (topicName, state, page, pageSize) => {
    const jobs = [];
    const startId = (page - 1) * pageSize + 1000;
    const states = ['ready', 'delayed', 'reserved', 'buried'];

    for (let i = 0; i < pageSize; i++) {
      const id = startId + i;
      const jobState = state || states[Math.floor(Math.random() * states.length)];

      jobs.push({
        ID: id,
        Topic: topicName,
        State: jobState,
        Priority: Math.floor(Math.random() * 2048) + 1,
        Reserves: Math.floor(Math.random() * 5),
        CreatedAt: new Date(Date.now() - Math.random() * 24 * 60 * 60 * 1000).toISOString()
      });
    }

    return jobs;
  };

  const generateMockJob = (id) => {
    const idNum = parseInt(id);
    const states = ['ready', 'delayed', 'reserved', 'buried'];
    const state = states[idNum % states.length];
    const now = Date.now();

    return {
      ID: idNum,
      Topic: 'email-processing',
      State: state,
      Priority: (idNum % 2048) + 1,
      Delay: state === 'delayed' ? ((idNum % 300) + 60) : 0,
      TTR: ((idNum % 120) + 30),
      Age: `${Math.floor((now - (now - Math.random() * 24 * 60 * 60 * 1000)) / 60000)}m`,
      Reserves: (idNum % 5),
      Timeouts: (idNum % 3),
      Releases: (idNum % 2),
      Buries: state === 'buried' ? ((idNum % 2) + 1) : 0,
      Kicks: state === 'buried' ? ((idNum % 3) + 1) : (idNum % 2),
      Touches: ((idNum % 4)),
      CreatedAt: new Date(now - Math.random() * 24 * 60 * 60 * 1000).toISOString(),
      ReadyAt: state === 'delayed' ? new Date(now + ((idNum % 300) + 60) * 1000).toISOString() : new Date(now - Math.random() * 24 * 60 * 60 * 1000).toISOString()
    };
  };

  // API: /api/overview
  server.middlewares.use('/api/overview', (req, res, next) => {
    if (req.method === 'GET') {
      res.setHeader('Content-Type', 'application/json');
      res.end(JSON.stringify(generateMockStats()));
      return;
    }
    next();
  });

  // API: /api/topics
  server.middlewares.use('/api/topics', (req, res, next) => {
    if (req.method === 'GET') {
      const url = req.url;
      const parts = url.split('/');
      const topicName = parts[2];

      if (topicName && url.includes('/jobs')) {
        // Handle topic jobs list
        const urlObj = new URL(url, 'http://localhost:3000');
        const state = urlObj.searchParams.get('state');
        const page = parseInt(urlObj.searchParams.get('page')) || 1;
        const pageSize = 20;

        const jobs = generateMockJobs(topicName, state, page, pageSize);
        const total = Math.floor(Math.random() * 100) + 50;

        res.setHeader('Content-Type', 'application/json');
        res.end(JSON.stringify({
          Jobs: jobs,
          Total: total,
          Page: page,
          PageSize: pageSize,
          TotalPages: Math.ceil(total / pageSize)
        }));
        return;
      } else if (topicName && !url.includes('/jobs')) {
        // Handle single topic
        const topics = generateMockTopics();
        const topic = topics.find(t => t.Name === topicName);

        if (topic) {
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify(topic));
          return;
        }
      } else {
        // Handle all topics list
        res.setHeader('Content-Type', 'application/json');
        res.end(JSON.stringify(generateMockTopics()));
        return;
      }
    }
    next();
  });

  // API: /api/jobs/{id}
  server.middlewares.use('/api/jobs/', (req, res, next) => {
    const urlParts = req.url.split('/');
    const jobId = urlParts[urlParts.length - 1];

    if (req.method === 'GET' && jobId && !isNaN(jobId)) {
      res.setHeader('Content-Type', 'application/json');
      res.end(JSON.stringify(generateMockJob(jobId)));
      return;
    }

    if (req.method === 'POST' && jobId && req.url.includes('/kick')) {
      res.setHeader('Content-Type', 'application/json');
      res.end(JSON.stringify({ success: true }));
      return;
    }

    if (req.method === 'DELETE' && jobId && !isNaN(jobId)) {
      res.setHeader('Content-Type', 'application/json');
      res.end(JSON.stringify({ success: true }));
      return;
    }

    next();
  });
};

export default defineConfig({
  base: './',  // 使用相对路径，支持任意子路径部署
  plugins: [
    tailwindcss(),
    preact(),
    {
      name: 'mock-api',
      configureServer: mockApiRoutes
    }
  ]
});
