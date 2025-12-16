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

// Job row
const JobRow = ({ job }) => (
  <tr className="border-b border-gray-200 hover:bg-gray-50 dark:border-[#30363d] dark:hover:bg-[#21262d]">
    <td className="px-6 py-4">
      <a href={`/jobs/${job.ID}`} className="font-medium text-blue-600 hover:text-blue-800 dark:text-[#58a6ff] dark:hover:text-[#79c0ff]">
        #{job.ID}
      </a>
    </td>
    <td className="px-6 py-4">
      <StateBadge state={job.State} />
    </td>
    <td className="px-6 py-4 text-center text-gray-900 dark:text-[#c9d1d9]">{job.Priority}</td>
    <td className="px-6 py-4 text-center text-gray-900 dark:text-[#c9d1d9]">{job.Reserves}</td>
    <td className="px-6 py-4 text-sm text-gray-500 dark:text-[#8b949e]">
      {new Date(job.CreatedAt).toLocaleString()}
    </td>
    <td className="px-6 py-4 text-center">
      {job.State === 'buried' && (
        <button className="bg-yellow-500 text-white px-3 py-1 rounded text-sm hover:bg-yellow-600 mr-2">
          Kick
        </button>
      )}
      <button className="bg-red-600 text-white px-3 py-1 rounded text-sm hover:bg-red-700">
        Delete
      </button>
    </td>
  </tr>
);

export function TopicDetail({ topic }) {
  const [topicData, setTopicData] = useState(null);
  const [jobsData, setJobsData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [currentState, setCurrentState] = useState('');
  const [currentPage, setCurrentPage] = useState(1);

  const fetchTopicData = async () => {
    try {
      const response = await fetch(`/api/topics/${topic}`);
      if (!response.ok) throw new Error('Topic not found');
      const data = await response.json();
      setTopicData(data);
    } catch (e) {
      setError(e.message);
    }
  };

  const fetchJobsData = async (state = currentState, page = currentPage) => {
    try {
      const params = new URLSearchParams({
        page: page.toString(),
        page_size: '20'
      });

      if (state) {
        params.set('state', state);
      }

      const response = await fetch(`/api/topics/${topic}/jobs?${params}`);
      if (!response.ok) throw new Error('Failed to fetch jobs');
      const data = await response.json();
      setJobsData(data);
    } catch (e) {
      setError(e.message);
    }
  };

  const fetchData = async () => {
    setLoading(true);
    await Promise.all([
      fetchTopicData(),
      fetchJobsData()
    ]);
    setLoading(false);
  };

  useEffect(() => {
    fetchData();
  }, []);

  useEffect(() => {
    fetchJobsData(currentState, currentPage);
  }, [currentState, currentPage]);

  if (loading) {
    return (
      <div className="flex items-center justify-center min-h-screen bg-gray-50 dark:bg-[#0d1117]">
        <div className="text-center">
          <div className="w-12 h-12 border-4 border-blue-200 border-t-blue-600 rounded-full animate-spin mx-auto mb-4"></div>
          <p className="text-gray-600 dark:text-[#8b949e]">Loading topic details...</p>
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

  if (!topicData) {
    return (
      <div className="flex items-center justify-center min-h-screen bg-gray-50 dark:bg-[#0d1117]">
        <div className="text-center">
          <div className="text-4xl mb-4 text-gray-600 dark:text-[#8b949e]">📭</div>
          <h2 className="text-xl font-semibold text-gray-900 dark:text-[#c9d1d9]">Topic Not Found</h2>
          <p className="text-gray-600 dark:text-[#8b949e]">Topic "{topic}" does not exist.</p>
        </div>
      </div>
    );
  }

  return (
    <>
      {/* Header */}
      <div className="max-w-7xl mx-auto mb-8">
        <div className="flex justify-between items-center h-16">
          <div className="flex items-center space-x-4">
            <a href="/" className="text-gray-500 hover:text-gray-700 dark:text-[#8b949e] dark:hover:text-[#c9d1d9]">Dashboard</a>
            <span className="text-gray-400 dark:text-[#6e7681]">/</span>
            <span className="font-semibold text-gray-900 dark:text-[#c9d1d9]">{topic}</span>
          </div>
          <button onClick={fetchData} className="px-4 py-2 rounded text-sm bg-gray-600 hover:bg-gray-700 text-white dark:bg-[#21262d] dark:hover:bg-[#30363d] dark:text-[#c9d1d9]">
            Refresh
          </button>
        </div>
      </div>

      {/* Main Content */}
      {/* Topic Stats */}
      <section className="max-w-7xl mx-auto mb-8">
        <h2 className="text-xl font-semibold mb-4 text-gray-900 dark:text-[#c9d1d9]">Topic Statistics</h2>
        <div className="grid grid-cols-1 md:grid-cols-5 gap-6">
          <div className="p-6 rounded-lg border text-center bg-white border-gray-200 dark:bg-[#161b22] dark:border-[#30363d]">
            <div className="text-2xl font-bold text-gray-900 dark:text-[#c9d1d9]">{topicData.TotalJobs}</div>
            <div className="text-sm mt-1 text-gray-500 dark:text-[#8b949e]">Total Jobs</div>
          </div>
          <div className="p-6 rounded-lg border text-center bg-white border-gray-200 dark:bg-[#161b22] dark:border-[#30363d]">
            <div className="text-2xl font-bold text-green-600 dark:text-[#3fb950]">{topicData.ReadyJobs}</div>
            <div className="text-sm mt-1 text-gray-500 dark:text-[#8b949e]">Ready</div>
          </div>
          <div className="p-6 rounded-lg border text-center bg-white border-gray-200 dark:bg-[#161b22] dark:border-[#30363d]">
            <div className="text-2xl font-bold text-blue-600 dark:text-[#58a6ff]">{topicData.ReservedJobs}</div>
            <div className="text-sm mt-1 text-gray-500 dark:text-[#8b949e]">Reserved</div>
          </div>
          <div className="p-6 rounded-lg border text-center bg-white border-gray-200 dark:bg-[#161b22] dark:border-[#30363d]">
            <div className="text-2xl font-bold text-yellow-600 dark:text-[#d29922]">{topicData.DelayedJobs}</div>
            <div className="text-sm mt-1 text-gray-500 dark:text-[#8b949e]">Delayed</div>
          </div>
          <div className="p-6 rounded-lg border text-center bg-white border-gray-200 dark:bg-[#161b22] dark:border-[#30363d]">
            <div className="text-2xl font-bold text-red-600 dark:text-[#f85149]">{topicData.BuriedJobs}</div>
            <div className="text-sm mt-1 text-gray-500 dark:text-[#8b949e]">Buried</div>
          </div>
        </div>
      </section>

      {/* Jobs Section */}
      <section className="max-w-7xl mx-auto">
        <div className="flex flex-col sm:flex-row justify-between items-start sm:items-center mb-4 gap-4">
          <h2 className="text-xl font-semibold text-gray-900 dark:text-[#c9d1d9]">
            Jobs ({jobsData?.Total || 0} total)
          </h2>
          <div className="flex flex-wrap gap-2">
            {['', 'ready', 'delayed', 'reserved', 'buried'].map(state => (
              <button
                key={state || 'all'}
                onClick={() => {
                  setCurrentState(state);
                  setCurrentPage(1);
                }}
                className={`px-4 py-2 rounded text-sm font-medium ${currentState === state
                  ? 'bg-blue-600 text-white'
                  : 'bg-gray-200 text-gray-700 hover:bg-gray-300 dark:bg-[#21262d] dark:text-[#c9d1d9] dark:hover:bg-[#30363d]'
                  }`}
              >
                {state || 'All'}
              </button>
            ))}
          </div>
        </div>

        {jobsData?.Jobs && jobsData.Jobs.length > 0 ? (
          <>
            <div className="rounded-lg shadow overflow-hidden bg-white dark:bg-[#161b22]">
              <table className="min-w-full">
                <thead className="bg-gray-50 dark:bg-[#0d1117]">
                  <tr>
                    <th className="px-6 py-3 text-left text-xs font-medium uppercase text-gray-500 dark:text-[#8b949e]">ID</th>
                    <th className="px-6 py-3 text-left text-xs font-medium uppercase text-gray-500 dark:text-[#8b949e]">State</th>
                    <th className="px-6 py-3 text-center text-xs font-medium uppercase text-gray-500 dark:text-[#8b949e]">Priority</th>
                    <th className="px-6 py-3 text-center text-xs font-medium uppercase text-gray-500 dark:text-[#8b949e]">Reserves</th>
                    <th className="px-6 py-3 text-left text-xs font-medium uppercase text-gray-500 dark:text-[#8b949e]">Created At</th>
                    <th className="px-6 py-3 text-center text-xs font-medium uppercase text-gray-500 dark:text-[#8b949e]">Actions</th>
                  </tr>
                </thead>
                <tbody className="bg-white divide-y divide-gray-200 dark:bg-[#161b22] dark:divide-[#30363d]">
                  {jobsData.Jobs.map(job => (
                    <JobRow key={job.ID} job={job} />
                  ))}
                </tbody>
              </table>
            </div>

            {/* Pagination */}
            {jobsData.TotalPages > 1 && (
              <div className="flex justify-center items-center mt-6 space-x-2">
                <button
                  onClick={() => setCurrentPage(1)}
                  disabled={currentPage === 1}
                  className={`px-3 py-2 text-sm rounded ${currentPage === 1
                    ? 'bg-gray-100 text-gray-400 cursor-not-allowed dark:bg-[#21262d] dark:text-[#6e7681]'
                    : 'bg-white border border-gray-300 text-gray-700 hover:bg-gray-50 dark:bg-[#21262d] dark:border dark:border-[#30363d] dark:text-[#c9d1d9] dark:hover:bg-[#30363d]'
                    }`}
                >
                  «
                </button>

                <button
                  onClick={() => setCurrentPage(Math.max(1, currentPage - 1))}
                  disabled={currentPage === 1}
                  className={`px-3 py-2 text-sm rounded ${currentPage === 1
                    ? 'bg-gray-100 text-gray-400 cursor-not-allowed dark:bg-[#21262d] dark:text-[#6e7681]'
                    : 'bg-white border border-gray-300 text-gray-700 hover:bg-gray-50 dark:bg-[#21262d] dark:border dark:border-[#30363d] dark:text-[#c9d1d9] dark:hover:bg-[#30363d]'
                    }`}
                >
                  ‹
                </button>

                {(() => {
                  const totalPages = jobsData.TotalPages || 1;
                  const pages = [];
                  const maxVisible = 5;

                  let startPage = Math.max(1, currentPage - Math.floor(maxVisible / 2));
                  let endPage = Math.min(totalPages, startPage + maxVisible - 1);

                  if (endPage - startPage < maxVisible - 1) {
                    startPage = Math.max(1, endPage - maxVisible + 1);
                  }

                  for (let i = startPage; i <= endPage; i++) {
                    pages.push(i);
                  }

                  return pages.map(page => (
                    <button
                      key={page}
                      onClick={() => setCurrentPage(page)}
                      className={`px-3 py-2 text-sm rounded ${page === currentPage
                        ? 'bg-blue-600 text-white'
                        : 'bg-white border border-gray-300 text-gray-700 hover:bg-gray-50 dark:bg-[#21262d] dark:border dark:border-[#30363d] dark:text-[#c9d1d9] dark:hover:bg-[#30363d]'
                        }`}
                    >
                      {page}
                    </button>
                  ));
                })()}

                <button
                  onClick={() => setCurrentPage(Math.min(jobsData.TotalPages, currentPage + 1))}
                  disabled={currentPage === jobsData.TotalPages}
                  className={`px-3 py-2 text-sm rounded ${currentPage === jobsData.TotalPages
                    ? 'bg-gray-100 text-gray-400 cursor-not-allowed dark:bg-[#21262d] dark:text-[#6e7681]'
                    : 'bg-white border border-gray-300 text-gray-700 hover:bg-gray-50 dark:bg-[#21262d] dark:border dark:border-[#30363d] dark:text-[#c9d1d9] dark:hover:bg-[#30363d]'
                    }`}
                >
                  ›
                </button>

                <button
                  onClick={() => setCurrentPage(jobsData.TotalPages)}
                  disabled={currentPage === jobsData.TotalPages}
                  className={`px-3 py-2 text-sm rounded ${currentPage === jobsData.TotalPages
                    ? 'bg-gray-100 text-gray-400 cursor-not-allowed dark:bg-[#21262d] dark:text-[#6e7681]'
                    : 'bg-white border border-gray-300 text-gray-700 hover:bg-gray-50 dark:bg-[#21262d] dark:border dark:border-[#30363d] dark:text-[#c9d1d9] dark:hover:bg-[#30363d]'
                    }`}
                >
                  »
                </button>
              </div>
            )}

            <div className="flex justify-center items-center mt-4 text-sm text-gray-600 dark:text-[#8b949e]">
              Showing {((currentPage - 1) * (jobsData.PageSize || 20)) + 1} to{' '}
              {Math.min(currentPage * (jobsData.PageSize || 20), jobsData.Total)} of{' '}
              {jobsData.Total} jobs
            </div>
          </>
        ) : (
          <div className="rounded-lg p-12 text-center bg-white text-gray-500 dark:bg-[#161b22] dark:text-[#8b949e]">
            <p className="text-lg font-medium">No jobs found</p>
            <p className="mt-1">No jobs match to current filters.</p>
            {currentState && (
              <button
                onClick={() => {
                  setCurrentState('');
                  setCurrentPage(1);
                }}
                className="mt-4 bg-blue-600 text-white px-4 py-2 rounded hover:bg-blue-700"
              >
                Show All Jobs
              </button>
            )}
          </div>
        )}
      </section>
    </>
  );
}
