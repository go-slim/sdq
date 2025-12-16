import { h } from 'preact';
import { useState, useEffect } from 'preact/hooks';

// Storage card
const StorageCard = ({ title, description, stats, icon }) => (
  <div className="p-6 rounded-lg shadow-sm border transition-shadow bg-white dark:bg-[#161b22] border-gray-200 dark:border-[#30363d] hover:shadow-md dark:hover:bg-[#1c2128]">
    <div className="flex items-center mb-4">
      <div className="w-10 h-10 rounded-lg flex items-center justify-center mr-3 bg-blue-100 dark:bg-[#21262d]">
        {icon}
      </div>
      <div>
        <h3 className="text-lg font-semibold text-gray-900 dark:text-[#c9d1d9]">{title}</h3>
        <p className="text-sm text-gray-500 dark:text-[#8b949e]">{description}</p>
      </div>
    </div>

    <div className="grid grid-cols-2 gap-4">
      {stats.map((stat, index) => (
        <div key={index} className="text-center">
          <div className="text-2xl font-bold text-gray-900 dark:text-[#c9d1d9]">{stat.value}</div>
          <div className="text-sm text-gray-500 dark:text-[#8b949e]">{stat.label}</div>
        </div>
      ))}
    </div>
  </div>
);

export function Storage() {
  const [storageStats, setStorageStats] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  const fetchStorageStats = async () => {
    try {
      // Mock storage stats - in real app this would fetch from API
      const stats = {
        totalSize: '125.6 GB',
        totalJobs: 15420,
        activeConnections: 8,
        uptime: '15d 8h 32m'
      };

      setStorageStats(stats);
      setError(null);
    } catch (e) {
      setError(e.message);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchStorageStats();
  }, []);

  if (loading) {
    return (
      <div className="flex items-center justify-center min-h-96 bg-gray-50 dark:bg-[#0d1117]">
        <div className="text-center">
          <div className="w-12 h-12 border-4 border-blue-200 border-t-blue-600 rounded-full animate-spin mx-auto mb-4"></div>
          <p className="text-gray-600 dark:text-[#8b949e]">Loading storage information...</p>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="flex items-center justify-center min-h-96 bg-gray-50 dark:bg-[#0d1117]">
        <div className="text-center">
          <div className="text-red-600 text-4xl mb-4">⚠️</div>
          <h2 className="text-xl font-semibold mb-2 text-gray-900 dark:text-[#c9d1d9]">Error Loading Storage</h2>
          <p className="mb-4 text-gray-600 dark:text-[#8b949e]">{error}</p>
          <button onClick={fetchStorageStats} className="bg-blue-600 text-white px-4 py-2 rounded hover:bg-blue-700">
            Retry
          </button>
        </div>
      </div>
    );
  }

  return (
    <>
      <div className="max-w-7xl mx-auto">
        <div className="flex justify-between items-center h-16">
          <div className="flex items-center space-x-4">
            <span className="text-gray-500 dark:text-[#8b949e] hover:text-gray-700 dark:hover:text-[#c9d1d9]">Dashboard</span>
            <span className="text-gray-400 dark:text-[#6e7681]">/</span>
            <span className="font-semibold text-gray-900 dark:text-[#c9d1d9]">Storage</span>
          </div>
          <button onClick={fetchStorageStats} className="px-4 py-2 rounded text-sm bg-gray-600 dark:bg-[#21262d] hover:bg-gray-700 dark:hover:bg-[#30363d] text-white dark:text-[#c9d1d9]">
            Refresh
          </button>
        </div>
      </div>

      <div className="max-w-7xl mx-auto mb-8">
        <h1 className="text-3xl font-bold mb-2 text-gray-900 dark:text-[#c9d1d9]">Storage Information</h1>
        <p className="text-gray-600 dark:text-[#8b949e]">Monitor your storage usage and performance metrics</p>
      </div>

      {storageStats && (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6 max-w-7xl mx-auto mb-8">
          <StorageCard
            title="Primary Storage"
            description="Main database storage"
            icon="💾"
            stats={[
              { label: "Total Size", value: storageStats.totalSize },
              { label: "Jobs", value: storageStats.totalJobs.toLocaleString() }
            ]}
          />
          <StorageCard
            title="Performance"
            description="Storage performance metrics"
            icon="⚡"
            stats={[
              { label: "Connections", value: storageStats.activeConnections },
              { label: "Uptime", value: storageStats.uptime }
            ]}
          />
          <StorageCard
            title="Configuration"
            description="Storage backend settings"
            icon="⚙️"
            stats={[
              { label: "Type", value: "SQLite" },
              { label: "Path", value: "/data/sdq.db" }
            ]}
          />
        </div>
      )}

      <section className="max-w-7xl mx-auto rounded-lg shadow-sm border p-6 bg-white dark:bg-[#161b22] border-gray-200 dark:border-[#30363d]">
        <h2 className="text-xl font-semibold mb-4 text-gray-900 dark:text-[#c9d1d9]">Storage Details</h2>

        <div className="space-y-6">
          <div>
            <h3 className="text-lg font-medium mb-2 text-gray-900 dark:text-[#c9d1d9]">Database Information</h3>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <div>
                <div className="text-sm text-gray-500 dark:text-[#8b949e]">Database Type</div>
                <div className="font-medium text-gray-900 dark:text-[#c9d1d9]">SQLite 3.x</div>
              </div>
              <div>
                <div className="text-sm text-gray-500 dark:text-[#8b949e]">File Location</div>
                <div className="font-medium text-gray-900 dark:text-[#c9d1d9]">/var/lib/sdq/jobs.db</div>
              </div>
              <div>
                <div className="text-sm text-gray-500 dark:text-[#8b949e]">Journal Mode</div>
                <div className="font-medium text-gray-900 dark:text-[#c9d1d9]">WAL</div>
              </div>
              <div>
                <div className="text-sm text-gray-500 dark:text-[#8b949e]">Page Size</div>
                <div className="font-medium text-gray-900 dark:text-[#c9d1d9]">4096 bytes</div>
              </div>
            </div>
          </div>

          <div>
            <h3 className="text-lg font-medium mb-2 text-gray-900 dark:text-[#c9d1d9]">Performance Metrics</h3>
            <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
              <div>
                <div className="text-sm text-gray-500 dark:text-[#8b949e]">Avg Write Speed</div>
                <div className="font-medium text-gray-900 dark:text-[#c9d1d9]">1,250 ops/sec</div>
              </div>
              <div>
                <div className="text-sm text-gray-500 dark:text-[#8b949e]">Avg Read Speed</div>
                <div className="font-medium text-gray-900 dark:text-[#c9d1d9]">3,800 ops/sec</div>
              </div>
              <div>
                <div className="text-sm text-gray-500 dark:text-[#8b949e]">Cache Hit Rate</div>
                <div className="font-medium text-gray-900 dark:text-[#c9d1d9]">94.2%</div>
              </div>
            </div>
          </div>

          <div>
            <h3 className="text-lg font-medium mb-2 text-gray-900 dark:text-[#c9d1d9]">Backup Information</h3>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <div>
                <div className="text-sm text-gray-500 dark:text-[#8b949e]">Last Backup</div>
                <div className="font-medium text-gray-900 dark:text-[#c9d1d9]">2 hours ago</div>
              </div>
              <div>
                <div className="text-sm text-gray-500 dark:text-[#8b949e]">Backup Size</div>
                <div className="font-medium text-gray-900 dark:text-[#c9d1d9]">42.1 GB</div>
              </div>
              <div>
                <div className="text-sm text-gray-500 dark:text-[#8b949e]">Retention</div>
                <div className="font-medium text-gray-900 dark:text-[#c9d1d9]">30 days</div>
              </div>
              <div>
                <div className="text-sm text-gray-500 dark:text-[#8b949e]">Status</div>
                <div className="font-medium text-green-600 dark:text-[#3fb950]">Healthy</div>
              </div>
            </div>
          </div>
        </div>
      </section>
    </>
  );
}
