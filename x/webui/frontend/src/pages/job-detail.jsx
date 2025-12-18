import { useState, useEffect } from 'preact/hooks';

// State badge
const StateBadge = ({ state }) => {
  const stateClasses = {
    ready: "bg-green-100 text-green-800 dark:bg-[#1b2e1f] dark:text-[#3fb950]",
    delayed: "bg-yellow-100 text-yellow-800 dark:bg-[#341a00] dark:text-[#d29922]",
    reserved: "bg-blue-100 text-blue-800 dark:bg-[#1c2d41] dark:text-[#58a6ff]",
    buried: "bg-red-100 text-red-800 dark:bg-[#2e1a1f] dark:text-[#f85149]",
    enqueued: "bg-gray-100 text-gray-800 dark:bg-[#21262d] dark:text-[#c9d1d9]"
  };

  return (
    <span className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${stateClasses[state] || stateClasses.enqueued}`}>
      {state}
    </span>
  );
};

// Format date
const formatDate = (dateString) => {
  if (!dateString) return '-';
  return new Date(dateString).toLocaleString();
};

export function JobDetail({ id }) {
  const [job, setJob] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  const fetchJobData = async () => {
    try {
      const response = await fetch(`api/jobs/${id}`);

      if (!response.ok) {
        if (response.status === 404) {
          throw new Error(`Job #${id} not found`);
        }
        throw new Error(`HTTP error! status: ${response.status}`);
      }

      const data = await response.json();
      setJob(data);
      setError(null);
    } catch (e) {
      setError(e.message);
      setJob(null);
    } finally {
      setLoading(false);
    }
  };

  const handleAction = async (action) => {
    if (action === 'kick') {
      if (!confirm(`Kick job #${id}?`)) return;

      try {
        const response = await fetch(`api/jobs/${id}/kick`, {
          method: 'POST'
        });

        if (!response.ok) {
          throw new Error('Failed to kick job');
        }

        alert('Job kicked successfully');
        await fetchJobData();
      } catch (e) {
        alert(`Error: ${e.message}`);
      }
    } else if (action === 'delete') {
      if (!confirm(`Delete job #${id}? This cannot be undone!`)) return;

      try {
        const response = await fetch(`api/jobs/${id}`, {
          method: 'DELETE'
        });

        if (!response.ok) {
          throw new Error('Failed to delete job');
        }

        alert('Job deleted successfully');
        window.location.href = '/';
      } catch (e) {
        alert(`Error: ${e.message}`);
      }
    }
  };

  useEffect(() => {
    fetchJobData();
  }, [id]);

  if (loading) {
    return (
      <div className="flex items-center justify-center min-h-screen bg-gray-50 dark:bg-[#0d1117]">
        <div className="text-center">
          <div className="w-12 h-12 border-4 border-blue-200 border-t-blue-600 rounded-full animate-spin mx-auto mb-4"></div>
          <p className="text-gray-600 dark:text-[#8b949e]">Loading job details...</p>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="flex items-center justify-center min-h-screen bg-gray-50 dark:bg-[#0d1117]">
        <div className="text-center">
          <div className="text-red-600 text-4xl mb-4">⚠️</div>
          <h2 className="text-xl font-semibold mb-2 text-gray-900 dark:text-[#c9d1d9]">Error Loading Job</h2>
          <p className="mb-4 text-gray-600 dark:text-[#8b949e]">{error}</p>
          <button onClick={fetchJobData} className="bg-blue-600 text-white px-4 py-2 rounded hover:bg-blue-700 mr-2">
            Retry
          </button>
          <a href="" className="px-4 py-2 rounded bg-gray-600 hover:bg-gray-700 text-white dark:bg-[#21262d] dark:hover:bg-[#30363d] dark:text-[#c9d1d9]">
            Back to Dashboard
          </a>
        </div>
      </div>
    );
  }

  if (!job) {
    return (
      <div className="flex items-center justify-center min-h-screen bg-gray-50 dark:bg-[#0d1117]">
        <div className="text-center">
          <div className="text-4xl mb-4 text-gray-600 dark:text-[#8b949e]">📭</div>
          <h2 className="text-xl font-semibold text-gray-900 dark:text-[#c9d1d9]">Job Not Found</h2>
          <p className="text-gray-600 dark:text-[#8b949e]">Job #{id} does not exist.</p>
        </div>
      </div>
    );
  }

  const mockBody = JSON.stringify({
    email: 'user@example.com',
    template: 'welcome',
    userId: 12345,
    metadata: {
      source: 'web-app',
      timestamp: new Date().toISOString(),
      version: '1.0'
    }
  }, null, 2);

  return (
    <>
      {/* Header */}
      <div className="max-w-7xl mx-auto mb-8">
        <div className="flex justify-between items-center h-16">
          <div className="flex items-center space-x-4">
            <a href="" className="text-gray-500 hover:text-gray-700 dark:text-[#8b949e] dark:hover:text-[#c9d1d9]">Dashboard</a>
            <span className="text-gray-400 dark:text-[#6e7681]">/</span>
            <a href={`topics/${job.topic}`} className="text-gray-500 hover:text-gray-700 dark:text-[#8b949e] dark:hover:text-[#c9d1d9]">{job.topic}</a>
            <span className="text-gray-400 dark:text-[#6e7681]">/</span>
            <span className="font-semibold text-gray-900 dark:text-[#c9d1d9]">Job #{job.id}</span>
          </div>
          <button onClick={fetchJobData} className="px-4 py-2 rounded text-sm bg-gray-600 hover:bg-gray-700 text-white dark:bg-[#21262d] dark:hover:bg-[#30363d] dark:text-[#c9d1d9]">
            Refresh
          </button>
        </div>
      </div>

      {/* Job Overview */}
      <section className="max-w-7xl mx-auto mb-8">
        <div className="rounded-lg shadow p-6 bg-white dark:bg-[#161b22]">
          <div className="flex justify-between items-start mb-6">
            <div className="flex items-center space-x-4">
              <h2 className="text-2xl font-bold text-gray-900 dark:text-[#c9d1d9]">Job #{job.id}</h2>
              <StateBadge state={job.state} />
            </div>
            <div className="flex space-x-2">
              {job.state === 'buried' && (
                <button onClick={() => handleAction('kick')} className="bg-yellow-500 text-white px-4 py-2 rounded hover:bg-yellow-600">
                  Kick Job
                </button>
              )}
              <button onClick={() => handleAction('delete')} className="bg-red-600 text-white px-4 py-2 rounded hover:bg-red-700">
                Delete Job
              </button>
            </div>
          </div>

          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div className="flex justify-between py-2 border-b border-gray-200 dark:border-[#30363d]">
              <span className="font-medium text-gray-700 dark:text-[#8b949e]">ID</span>
              <span className="text-gray-900 dark:text-[#c9d1d9]">#{job.id}</span>
            </div>
            <div className="flex justify-between py-2 border-b border-gray-200 dark:border-[#30363d]">
              <span className="font-medium text-gray-700 dark:text-[#8b949e]">Topic</span>
              <a href={`topics/${job.topic}`} className="text-blue-600 hover:text-blue-800 dark:text-[#58a6ff] dark:hover:text-[#79c0ff]">{job.topic}</a>
            </div>
            <div className="flex justify-between py-2 border-b border-gray-200 dark:border-[#30363d]">
              <span className="font-medium text-gray-700 dark:text-[#8b949e]">State</span>
              <StateBadge state={job.state} />
            </div>
            <div className="flex justify-between py-2 border-b border-gray-200 dark:border-[#30363d]">
              <span className="font-medium text-gray-700 dark:text-[#8b949e]">Priority</span>
              <span className="text-gray-900 dark:text-[#c9d1d9]">{job.priority}</span>
            </div>
            <div className="flex justify-between py-2 border-b border-gray-200 dark:border-[#30363d]">
              <span className="font-medium text-gray-700 dark:text-[#8b949e]">Delay</span>
              <span className="text-gray-900 dark:text-[#c9d1d9]">{job.delay > 0 ? `${job.delay}s` : 'None'}</span>
            </div>
            <div className="flex justify-between py-2 border-b border-gray-200 dark:border-[#30363d]">
              <span className="font-medium text-gray-700 dark:text-[#8b949e]">TTR</span>
              <span className="text-gray-900 dark:text-[#c9d1d9]">{job.ttr}s</span>
            </div>
            <div className="flex justify-between py-2 border-b border-gray-200 dark:border-[#30363d]">
              <span className="font-medium text-gray-700 dark:text-[#8b949e]">Created At</span>
              <span className="text-gray-900 dark:text-[#c9d1d9]">{formatDate(job.created_at)}</span>
            </div>
            <div className="flex justify-between py-2 border-b border-gray-200 dark:border-[#30363d]">
              <span className="font-medium text-gray-700 dark:text-[#8b949e]">Age</span>
              <span className="text-gray-900 dark:text-[#c9d1d9]">{job.age || 'Unknown'}</span>
            </div>
          </div>
        </div>
      </section>

      {/* Statistics */}
      <section className="max-w-7xl mx-auto mb-8">
        <div className="rounded-lg shadow p-6 bg-white dark:bg-[#161b22]">
          <h2 className="text-xl font-semibold mb-4 text-gray-900 dark:text-[#c9d1d9]">Statistics</h2>
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
            <div className="text-center p-4 rounded bg-gray-50 dark:bg-[#0d1117]">
              <div className="text-2xl font-bold text-gray-900 dark:text-[#c9d1d9]">{job.reserves}</div>
              <div className="text-sm mt-1 text-gray-500 dark:text-[#8b949e]">Reserves</div>
            </div>
            <div className="text-center p-4 rounded bg-gray-50 dark:bg-[#0d1117]">
              <div className="text-2xl font-bold text-gray-900 dark:text-[#c9d1d9]">{job.timeouts}</div>
              <div className="text-sm mt-1 text-gray-500 dark:text-[#8b949e]">Timeouts</div>
            </div>
            <div className="text-center p-4 rounded bg-gray-50 dark:bg-[#0d1117]">
              <div className="text-2xl font-bold text-gray-900 dark:text-[#c9d1d9]">{job.releases}</div>
              <div className="text-sm mt-1 text-gray-500 dark:text-[#8b949e]">Releases</div>
            </div>
            <div className="text-center p-4 rounded bg-gray-50 dark:bg-[#0d1117]">
              <div className="text-2xl font-bold text-gray-900 dark:text-[#c9d1d9]">{job.buries}</div>
              <div className="text-sm mt-1 text-gray-500 dark:text-[#8b949e]">Buries</div>
            </div>
            <div className="text-center p-4 rounded bg-gray-50 dark:bg-[#0d1117]">
              <div className="text-2xl font-bold text-gray-900 dark:text-[#c9d1d9]">{job.kicks}</div>
              <div className="text-sm mt-1 text-gray-500 dark:text-[#8b949e]">Kicks</div>
            </div>
            <div className="text-center p-4 rounded bg-gray-50 dark:bg-[#0d1117]">
              <div className="text-2xl font-bold text-gray-900 dark:text-[#c9d1d9]">{job.touches}</div>
              <div className="text-sm mt-1 text-gray-500 dark:text-[#8b949e]">Touches</div>
            </div>
          </div>
        </div>
      </section>

      {/* Job Body */}
      <section className="max-w-7xl mx-auto">
        <div className="rounded-lg shadow p-6 bg-white dark:bg-[#161b22]">
          <div className="flex justify-between items-center mb-4">
            <h2 className="text-xl font-semibold text-gray-900 dark:text-[#c9d1d9]">Job Body</h2>
            <span className="text-sm text-gray-500 dark:text-[#8b949e]">({mockBody.length} bytes)</span>
          </div>
          <pre className="p-4 rounded overflow-x-auto text-sm bg-gray-50 text-gray-900 dark:bg-[#0d1117] dark:text-[#c9d1d9]">
            {mockBody}
          </pre>
        </div>
      </section>
    </>
  );
}
