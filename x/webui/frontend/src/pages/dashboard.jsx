import { useState, useEffect } from 'preact/hooks';

// Format uptime
const formatUptime = (seconds) => {
  if (!seconds) return 'Unknown';
  const days = Math.floor(seconds / 86400);
  const hours = Math.floor((seconds % 86400) / 3600);
  const minutes = Math.floor((seconds % 3600) / 60);
  return days > 0 ? `${days}d ${hours}h ${minutes}m` : `${hours}h ${minutes}m`;
};

// Stat Card
const StatCard = ({ label, value, color = "blue" }) => {
  const colorClasses = {
    blue: "bg-blue-50 dark:bg-[#1c2d41] text-blue-700 dark:text-[#58a6ff] border-gray-200 dark:border-[#30363d]",
    green: "bg-green-50 dark:bg-[#1b2e1f] text-green-700 dark:text-[#3fb950] border-gray-200 dark:border-[#30363d]",
    yellow: "bg-yellow-50 dark:bg-[#341a00] text-yellow-700 dark:text-[#d29922] border-gray-200 dark:border-[#30363d]",
    red: "bg-red-50 dark:bg-[#2e1a1f] text-red-700 dark:text-[#f85149] border-gray-200 dark:border-[#30363d]",
    gray: "bg-gray-50 dark:bg-[#21262d] text-gray-700 dark:text-[#c9d1d9] border-gray-200 dark:border-[#30363d]"
  };

  return (
    <div className={`${colorClasses[color]} p-6 rounded-lg border`}>
      <h3 className="text-lg font-semibold mb-2">{label}</h3>
      <p className="text-3xl font-bold">{value.toLocaleString()}</p>
    </div>
  );
};

// Topic Row
const TopicRow = ({ topic }) => (
  <tr className="border-b border-gray-200 dark:border-[#30363d] hover:bg-gray-50 dark:hover:bg-[#21262d]">
    <td className="px-6 py-4">
      <a href={`/topics/${topic.Name}`} className="font-medium text-blue-600 dark:text-[#58a6ff] hover:text-blue-800 dark:hover:text-[#79c0ff]">
        {topic.Name}
      </a>
    </td>
    <td className="px-6 py-4 text-center text-gray-900 dark:text-[#c9d1d9]">{topic.TotalJobs}</td>
    <td className="px-6 py-4 text-center text-gray-900 dark:text-[#c9d1d9]">{topic.ReadyJobs}</td>
    <td className="px-6 py-4 text-center text-gray-900 dark:text-[#c9d1d9]">{topic.ReservedJobs}</td>
    <td className="px-6 py-4 text-center text-gray-900 dark:text-[#c9d1d9]">{topic.DelayedJobs}</td>
    <td className="px-6 py-4 text-center text-gray-900 dark:text-[#c9d1d9]">{topic.BuriedJobs}</td>
    <td className="px-6 py-4 text-center">
      {topic.BuriedJobs > 0 && (
        <button className="bg-yellow-500 text-white px-3 py-1 rounded text-sm hover:bg-yellow-600">
          Kick All
        </button>
      )}
    </td>
  </tr>
);

export function Dashboard() {
  const [overview, setOverview] = useState(null);
  const [topics, setTopics] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [autoRefresh, setAutoRefresh] = useState(false);
  const [refreshInterval, setRefreshInterval] = useState(5000);

  const fetchData = async () => {
    try {
      const [overviewRes, topicsRes] = await Promise.all([
        fetch('/api/overview'),
        fetch('/api/topics')
      ]);

      if (!overviewRes.ok || !topicsRes.ok) {
        throw new Error('Failed to fetch data');
      }

      const overviewData = await overviewRes.json();
      const topicsData = await topicsRes.json();

      setOverview(overviewData);
      setTopics(topicsData);
      setError(null);
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchData();
  }, []);

  useEffect(() => {
    if (!autoRefresh) return;
    const interval = setInterval(fetchData, refreshInterval);
    return () => clearInterval(interval);
  }, [autoRefresh, refreshInterval]);

  if (loading) {
    return (
      <div className="flex items-center justify-center min-h-screen bg-gray-50 dark:bg-[#0d1117]">
        <div className="text-center">
          <div className="w-12 h-12 border-4 border-blue-200 border-t-blue-600 rounded-full animate-spin mx-auto mb-4"></div>
          <p className="text-gray-600 dark:text-[#8b949e]">Loading dashboard...</p>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="flex items-center justify-center min-h-screen bg-gray-50 dark:bg-[#0d1117]">
        <div className="text-center">
          <div className="text-red-600 text-4xl mb-4">⚠️</div>
          <h2 className="text-xl font-semibold mb-2 text-gray-900 dark:text-[#c9d1d9]">Error Loading Data</h2>
          <p className="mb-4 text-gray-600 dark:text-[#8b949e]">{error}</p>
          <button onClick={fetchData} className="bg-blue-600 text-white px-4 py-2 rounded hover:bg-blue-700">
            Retry
          </button>
        </div>
      </div>
    );
  }

  return (
    <>
      {/* Header */}
      <div className="max-w-7xl mx-auto mb-8">
        <div className="flex justify-between items-center h-16">
          <div>
            <h1 className="text-2xl font-bold text-gray-900 dark:text-[#c9d1d9]">SDQ Inspector</h1>
            <p className="text-sm text-gray-500 dark:text-[#8b949e]">Queue Monitoring Dashboard</p>
          </div>
          <div className="flex items-center space-x-4">
            <label className="flex items-center">
              <input
                type="checkbox"
                checked={autoRefresh}
                onChange={(e) => setAutoRefresh(e.target.checked)}
                className="mr-2"
              />
              <span className="text-sm text-gray-900 dark:text-[#c9d1d9]">Auto Refresh</span>
            </label>
            {autoRefresh && (
              <select
                value={refreshInterval}
                onChange={(e) => setRefreshInterval(Number(e.target.value))}
                className="border rounded px-2 py-1 text-sm bg-white dark:bg-[#0d1117] border-gray-300 dark:border-[#30363d] text-gray-900 dark:text-[#c9d1d9]"
              >
                <option value={1000}>1s</option>
                <option value={5000}>5s</option>
                <option value={10000}>10s</option>
                <option value={30000}>30s</option>
              </select>
            )}
            <button onClick={fetchData} className="px-4 py-2 rounded text-sm bg-gray-600 dark:bg-[#21262d] hover:bg-gray-700 dark:hover:bg-[#30363d] text-white dark:text-[#c9d1d9]">
              Refresh Now
            </button>
          </div>
        </div>
      </div>

      {/* Overview Stats */}
      <section className="max-w-7xl mx-auto mb-8">
        <h2 className="text-xl font-semibold mb-4 text-gray-900 dark:text-[#c9d1d9]">Queue Overview</h2>
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
          <StatCard label="Total Jobs" value={overview?.TotalJobs || 0} color="blue" />
          <StatCard label="Ready Jobs" value={overview?.ReadyJobs || 0} color="green" />
          <StatCard label="Reserved Jobs" value={overview?.ReservedJobs || 0} color="yellow" />
          <StatCard label="Delayed Jobs" value={overview?.DelayedJobs || 0} color="yellow" />
          <StatCard label="Buried Jobs" value={overview?.BuriedJobs || 0} color="red" />
          <StatCard label="Topics" value={overview?.Topics || 0} color="gray" />
          <StatCard label="Waiting Workers" value={overview?.TotalWaitingWorkers || 0} color="blue" />
          <StatCard label="Uptime" value={formatUptime(overview?.Uptime)} color="gray" />
        </div>
      </section>

      {/* Operation Stats */}
      <section className="max-w-7xl mx-auto mb-8">
        <h2 className="text-xl font-semibold mb-4 text-gray-900 dark:text-[#c9d1d9]">Operation Statistics</h2>
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
          <StatCard label="Puts" value={overview?.Puts || 0} color="green" />
          <StatCard label="Reserves" value={overview?.Reserves || 0} color="blue" />
          <StatCard label="Deletes" value={overview?.Deletes || 0} color="red" />
          <StatCard label="Releases" value={overview?.Releases || 0} color="yellow" />
          <StatCard label="Buries" value={overview?.Buries || 0} color="red" />
          <StatCard label="Kicks" value={overview?.Kicks || 0} color="blue" />
          <StatCard label="Timeouts" value={overview?.Timeouts || 0} color="yellow" />
          <StatCard label="Touches" value={overview?.Touches || 0} color="gray" />
        </div>
      </section>

      {/* Topics Table */}
      <section className="max-w-7xl mx-auto">
        <h2 className="text-xl font-semibold mb-4 text-gray-900 dark:text-[#c9d1d9]">Topics</h2>
        <div className="rounded-lg shadow overflow-hidden bg-white dark:bg-[#161b22]">
          <table className="min-w-full">
            <thead className="bg-gray-50 dark:bg-[#0d1117]">
              <tr>
                <th className="px-6 py-3 text-left text-xs font-medium uppercase tracking-wider text-gray-500 dark:text-[#8b949e]">
                  Topic Name
                </th>
                <th className="px-6 py-3 text-center text-xs font-medium uppercase tracking-wider text-gray-500 dark:text-[#8b949e]">
                  Total
                </th>
                <th className="px-6 py-3 text-center text-xs font-medium uppercase tracking-wider text-gray-500 dark:text-[#8b949e]">
                  Ready
                </th>
                <th className="px-6 py-3 text-center text-xs font-medium uppercase tracking-wider text-gray-500 dark:text-[#8b949e]">
                  Reserved
                </th>
                <th className="px-6 py-3 text-center text-xs font-medium uppercase tracking-wider text-gray-500 dark:text-[#8b949e]">
                  Delayed
                </th>
                <th className="px-6 py-3 text-center text-xs font-medium uppercase tracking-wider text-gray-500 dark:text-[#8b949e]">
                  Buried
                </th>
                <th className="px-6 py-3 text-center text-xs font-medium uppercase tracking-wider text-gray-500 dark:text-[#8b949e]">
                  Actions
                </th>
              </tr>
            </thead>
            <tbody className="bg-white dark:bg-[#161b22] divide-y divide-gray-200 dark:divide-[#30363d]">
              {topics.map(topic => (
                <TopicRow key={topic.Name} topic={topic} />
              ))}
            </tbody>
          </table>
        </div>
      </section>
    </>
  );
}
