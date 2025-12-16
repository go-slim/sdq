import { h } from 'preact';
import { useState, useEffect } from 'preact/hooks';

// Metric card
const MetricCard = ({ title, value, change, icon, color }) => {
  const colorClasses = {
    green: 'bg-green-50 dark:bg-[#1b2e1f] text-green-700 dark:text-[#3fb950] border-green-200 dark:border-[#30363d]',
    red: 'bg-red-50 dark:bg-[#2e1a1f] text-red-700 dark:text-[#f85149] border-red-200 dark:border-[#30363d]',
    blue: 'bg-blue-50 dark:bg-[#1c2d41] text-blue-700 dark:text-[#58a6ff] border-blue-200 dark:border-[#30363d]',
    yellow: 'bg-yellow-50 dark:bg-[#341a00] text-yellow-700 dark:text-[#d29922] border-yellow-200 dark:border-[#30363d]',
    gray: 'bg-gray-50 dark:bg-[#21262d] text-gray-700 dark:text-[#c9d1d9] border-gray-200 dark:border-[#30363d]',
    purple: 'bg-purple-50 dark:bg-[#271c3a] text-purple-700 dark:text-[#b392f0] border-purple-200 dark:border-[#30363d]',
  };

  const changeColor = change > 0
    ? 'text-green-600 dark:text-[#3fb950]'
    : change < 0
      ? 'text-red-600 dark:text-[#f85149]'
      : 'text-gray-600 dark:text-[#8b949e]';
  const changePrefix = change > 0 ? '+' : '';

  return (
    <div className={`bg-white dark:bg-[#161b22] p-6 rounded-lg shadow-sm border ${colorClasses[color]}`}>
      <div className="flex items-center justify-between mb-4">
        <div className="flex items-center">
          <div className="w-10 h-10 rounded-lg flex items-center justify-center mr-3 bg-white dark:bg-[#0d1117]">
            {icon}
          </div>
          <div>
            <div className="text-sm text-gray-500 dark:text-[#8b949e]">{title}</div>
            <div className="text-2xl font-bold text-gray-900 dark:text-[#c9d1d9]">{value}</div>
          </div>
        </div>
        {change !== undefined && (
          <div className={`text-sm font-medium ${changeColor}`}>
            {changePrefix}{change}
          </div>
        )}
      </div>
      <div className="w-full rounded-full h-2 bg-gray-200 dark:bg-[#21262d]">
        <div
          className={`bg-${color}-500 h-2 rounded-full`}
          style={{ width: `${Math.min(Math.abs(change || 50), 100)}%` }}
        ></div>
      </div>
    </div>
  );
};

// Chart container
const ChartContainer = ({ title, type, data }) => (
  <div className="p-6 rounded-lg shadow-sm border bg-white dark:bg-[#161b22] border-gray-200 dark:border-[#30363d]">
    <h3 className="text-lg font-semibold mb-4 text-gray-900 dark:text-[#c9d1d9]">{title}</h3>
    <div className="h-64 rounded-lg flex items-center justify-center bg-gray-50 dark:bg-[#0d1117]">
      <div className="text-gray-500 dark:text-[#8b949e]">
        {type === 'line' ? '📈 Line Chart' : '📊 Bar Chart'}
      </div>
      <div className="ml-4 text-sm text-gray-400 dark:text-[#6e7681]">
        {data.length} data points found
      </div>
    </div>
  </div>
);

export function Metrics() {
  const [metrics, setMetrics] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [timeRange, setTimeRange] = useState('1h');

  const fetchMetrics = async () => {
    try {
      // Mock metrics data
      const data = {
        throughput: {
          current: 156.7,
          change: 12.3,
          icon: '📈'
        },
        latency: {
          current: '45ms',
          change: -8.2,
          icon: '⚡'
        },
        errorRate: {
          current: '0.12%',
          change: -0.05,
          icon: '🎯'
        },
        activeConnections: {
          current: 24,
          change: 6,
          icon: '🔗'
        }
      };

      setMetrics(data);
      setError(null);
    } catch (e) {
      setError(e.message);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchMetrics();

    // Set up auto-refresh
    const interval = setInterval(fetchMetrics, 30000); // Refresh every 30 seconds
    return () => clearInterval(interval);
  }, []);

  if (loading) {
    return (
      <div className="flex items-center justify-center min-h-96 bg-gray-50 dark:bg-[#0d1117]">
        <div className="text-center">
          <div className="w-12 h-12 border-4 border-blue-200 border-t-blue-600 rounded-full animate-spin mx-auto mb-4"></div>
          <p className="text-gray-600 dark:text-[#8b949e]">Loading metrics...</p>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="flex items-center justify-center min-h-96 bg-gray-50 dark:bg-[#0d1117]">
        <div className="text-center">
          <div className="text-red-600 text-4xl mb-4">⚠️</div>
          <h2 className="text-xl font-semibold mb-2 text-gray-900 dark:text-[#c9d1d9]">Error Loading Metrics</h2>
          <p className="mb-4 text-gray-600 dark:text-[#8b949e]">{error}</p>
          <button onClick={fetchMetrics} className="bg-blue-600 text-white px-4 py-2 rounded hover:bg-blue-700">
            Retry
          </button>
        </div>
      </div>
    );
  }

  return (
    <>
      <div className="px-4 sm:px-6 lg:px-8">
        <div className="flex justify-between items-center h-16">
          <div className="flex items-center space-x-4">
            <span className="text-gray-500 dark:text-[#8b949e] hover:text-gray-700 dark:hover:text-[#c9d1d9]">Dashboard</span>
            <span className="text-gray-400 dark:text-[#6e7681]">/</span>
            <span className="font-semibold text-gray-900 dark:text-[#c9d1d9]">Metrics</span>
          </div>
          <div className="flex items-center space-x-4">
            <select
              value={timeRange}
              onChange={(e) => setTimeRange(e.target.value)}
              className="border rounded px-3 py-2 text-sm bg-white dark:bg-[#0d1117] border-gray-300 dark:border-[#30363d] text-gray-900 dark:text-[#c9d1d9]"
            >
              <option value="1h">Last Hour</option>
              <option value="24h">Last 24 Hours</option>
              <option value="7d">Last 7 Days</option>
              <option value="30d">Last 30 Days</option>
            </select>
            <button onClick={fetchMetrics} className="px-4 py-2 rounded text-sm bg-gray-600 dark:bg-[#21262d] hover:bg-gray-700 dark:hover:bg-[#30363d] text-white dark:text-[#c9d1d9]">
              Refresh
            </button>
          </div>
        </div>
      </div>

      <div className="mb-8">
        <h1 className="text-3xl font-bold mb-2 text-gray-900 dark:text-[#c9d1d9]">Performance Metrics</h1>
        <p className="text-gray-600 dark:text-[#8b949e]">Monitor your queue performance and usage statistics</p>
      </div>

      {metrics && (
        <>
          {/* Key Metrics */}
          <section className="mb-8">
            <h2 className="text-xl font-semibold mb-4 text-gray-900 dark:text-[#c9d1d9]">Key Performance Indicators</h2>
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
              <MetricCard
                title="Jobs/Second"
                value={metrics.throughput.current}
                change={metrics.throughput.change}
                icon={metrics.throughput.icon}
                color="green"
              />
              <MetricCard
                title="Average Latency"
                value={metrics.latency.current}
                change={metrics.latency.change}
                icon={metrics.latency.icon}
                color="blue"
              />
              <MetricCard
                title="Error Rate"
                value={metrics.errorRate.current}
                change={metrics.errorRate.change}
                icon={metrics.errorRate.icon}
                color="yellow"
              />
              <MetricCard
                title="Active Connections"
                value={metrics.activeConnections.current}
                change={metrics.activeConnections.change}
                icon={metrics.activeConnections.icon}
                color="purple"
              />
            </div>
          </section>

          {/* Charts */}
          <section className="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-8">
            <ChartContainer
              title="Throughput Trend"
              type="line"
              data={[[{ time: '00:00', value: 120 }, { time: '01:00', value: 135 }, { time: '02:00', value: 156 }]]}
            />
            <ChartContainer
              title="Queue Depth"
              type="bar"
              data={[{ name: 'ready', value: 245 }, { name: 'delayed', value: 45 }, { name: 'reserved', value: 23 }]}
            />
          </section>

          {/* Detailed Tables */}
          <section className="rounded-lg shadow-sm p-6 bg-white dark:bg-[#161b22] border-gray-200 dark:border-[#30363d]">
            <h2 className="text-xl font-semibold mb-4 text-gray-900 dark:text-[#c9d1d9]">Detailed Statistics</h2>

            <div className="grid grid-cols-1 md:grid-cols-2 gap-8">
              <div>
                <h3 className="text-lg font-medium mb-4 text-gray-900 dark:text-[#c9d1d9]">Job Statistics</h3>
                <table className="w-full">
                  <thead>
                    <tr className="border-b border-gray-200 dark:border-[#30363d]">
                      <th className="text-left py-2 text-sm font-medium text-gray-600 dark:text-[#8b949e]">Type</th>
                      <th className="text-right py-2 text-sm font-medium text-gray-600 dark:text-[#8b949e]">Count</th>
                      <th className="text-right py-2 text-sm font-medium text-gray-600 dark:text-[#8b949e]">Percentage</th>
                    </tr>
                  </thead>
                  <tbody>
                    <tr className="border-b border-gray-200 dark:border-[#30363d]">
                      <td className="py-2 text-gray-900 dark:text-[#c9d1d9]">Completed</td>
                      <td className="text-right py-2 text-gray-900 dark:text-[#c9d1d9]">15,420</td>
                      <td className="text-right py-2 text-green-600 dark:text-[#3fb950]">89.2%</td>
                    </tr>
                    <tr className="border-b border-gray-200 dark:border-[#30363d]">
                      <td className="py-2 text-gray-900 dark:text-[#c9d1d9]">Failed</td>
                      <td className="text-right py-2 text-gray-900 dark:text-[#c9d1d9]">234</td>
                      <td className="text-right py-2 text-red-600 dark:text-[#f85149]">1.4%</td>
                    </tr>
                    <tr className="border-b border-gray-200 dark:border-[#30363d]">
                      <td className="py-2 text-gray-900 dark:text-[#c9d1d9]">Timed Out</td>
                      <td className="text-right py-2 text-gray-900 dark:text-[#c9d1d9]">567</td>
                      <td className="text-right py-2 text-yellow-600 dark:text-[#d29922]">3.3%</td>
                    </tr>
                    <tr className="border-b border-gray-200 dark:border-[#30363d]">
                      <td className="py-2 text-gray-900 dark:text-[#c9d1d9]">Buried</td>
                      <td className="text-right py-2 text-gray-900 dark:text-[#c9d1d9]">876</td>
                      <td className="text-right py-2 text-orange-600 dark:text-[#fb8500]">5.1%</td>
                    </tr>
                  </tbody>
                </table>
              </div>

              <div>
                <h3 className="text-lg font-medium mb-4 text-gray-900 dark:text-[#c9d1d9]">Topic Performance</h3>
                <table className="w-full">
                  <thead>
                    <tr className="border-b border-gray-200 dark:border-[#30363d]">
                      <th className="text-left py-2 text-sm font-medium text-gray-600 dark:text-[#8b949e]">Topic</th>
                      <th className="text-right py-2 text-sm font-medium text-gray-600 dark:text-[#8b949e]">Jobs</th>
                      <th className="text-right py-2 text-sm font-medium text-gray-600 dark:text-[#8b949e]">Avg Time</th>
                      <th className="text-right py-2 text-sm font-medium text-gray-600 dark:text-[#8b949e]">Success Rate</th>
                    </tr>
                  </thead>
                  <tbody>
                    <tr className="border-b border-gray-200 dark:border-[#30363d]">
                      <td className="py-2 text-gray-900 dark:text-[#c9d1d9]">email-processing</td>
                      <td className="text-right py-2 text-gray-900 dark:text-[#c9d1d9]">8,456</td>
                      <td className="text-right py-2 text-gray-900 dark:text-[#c9d1d9]">2.3s</td>
                      <td className="text-right py-2 text-green-600 dark:text-[#3fb950]">98.7%</td>
                    </tr>
                    <tr className="border-b border-gray-200 dark:border-[#30363d]">
                      <td className="py-2 text-gray-900 dark:text-[#c9d1d9]">image-upload</td>
                      <td className="text-right py-2 text-gray-900 dark:text-[#c9d1d9]">3,892</td>
                      <td className="text-right py-2 text-gray-900 dark:text-[#c9d1d9]">5.1s</td>
                      <td className="text-right py-2 text-green-600 dark:text-[#3fb950]">96.3%</td>
                    </tr>
                    <tr className="border-b border-gray-200 dark:border-[#30363d]">
                      <td className="py-2 text-gray-900 dark:text-[#c9d1d9]">data-sync</td>
                      <td className="text-right py-2 text-gray-900 dark:text-[#c9d1d9]">2,234</td>
                      <td className="text-right py-2 text-gray-900 dark:text-[#c9d1d9]">1.8s</td>
                      <td className="text-right py-2 text-green-600 dark:text-[#3fb950]">97.5%</td>
                    </tr>
                  </tbody>
                </table>
              </div>
            </div>
          </section>
        </>
      )}
    </>
  );
}
