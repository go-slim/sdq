import { h } from 'preact';
import { useState, useEffect } from 'preact/hooks';
import { useLocation } from 'preact-iso';

// State badge
const StateBadge = ({ state }) => {
  const stateClasses = {
    ready: "bg-green-100 dark:bg-[#1b2e1f] text-green-800 dark:text-[#3fb950]",
    delayed: "bg-yellow-100 dark:bg-[#341a00] text-yellow-800 dark:text-[#d29922]",
    reserved: "bg-blue-100 dark:bg-[#1c2d41] text-blue-800 dark:text-[#58a6ff]",
    buried: "bg-red-100 dark:bg-[#2e1a1f] text-red-800 dark:text-[#f85149]",
    enqueued: "bg-gray-100 dark:bg-[#21262d] text-gray-800 dark:text-[#c9d1d9]"
  };

  return (
    <span className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${stateClasses[state] || stateClasses.enqueued}`}>
      {state}
    </span>
  );
};

// Topic card
const TopicCard = ({ topic }) => (
  <a href={`/topics/${topic.Name}`} className="block p-6 rounded-lg shadow-sm border transition-shadow bg-white dark:bg-[#161b22] border-gray-200 dark:border-[#30363d] hover:shadow-md dark:hover:bg-[#1c2128]">
    <div className="flex justify-between items-start mb-4">
      <h3 className="text-lg font-semibold text-gray-900 dark:text-[#c9d1d9]">{topic.Name}</h3>
      <StateBadge state="ready" />
    </div>
    <div className="grid grid-cols-2 gap-4 mb-4">
      <div className="text-center">
        <div className="text-2xl font-bold text-gray-900 dark:text-[#c9d1d9]">{topic.TotalJobs}</div>
        <div className="text-sm text-gray-500 dark:text-[#8b949e]">Total</div>
      </div>
      <div className="text-center">
        <div className="text-2xl font-bold text-green-600 dark:text-[#3fb950]">{topic.ReadyJobs}</div>
        <div className="text-sm text-gray-500 dark:text-[#8b949e]">Ready</div>
      </div>
      <div className="text-center">
        <div className="text-2xl font-bold text-blue-600 dark:text-[#58a6ff]">{topic.ReservedJobs}</div>
        <div className="text-sm text-gray-500 dark:text-[#8b949e]">Reserved</div>
      </div>
      <div className="text-center">
        <div className="text-2xl font-bold text-yellow-600 dark:text-[#d29922]">{topic.DelayedJobs}</div>
        <div className="text-sm text-gray-500 dark:text-[#8b949e]">Delayed</div>
      </div>
    </div>
    {topic.BuriedJobs > 0 && (
      <div className="flex justify-between items-center">
        <div className="text-center">
          <div className="text-2xl font-bold text-red-600 dark:text-[#f85149]">{topic.BuriedJobs}</div>
          <div className="text-sm text-gray-500 dark:text-[#8b949e]">Buried</div>
        </div>
        <button className="bg-yellow-500 text-white px-3 py-1 rounded text-sm hover:bg-yellow-600">
          Kick All
        </button>
      </div>
    )}
  </a>
);

export function Topics() {
  const [topics, setTopics] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  const fetchTopics = async () => {
    try {
      const response = await fetch('/api/topics');
      if (!response.ok) throw new Error('Failed to fetch topics');
      const data = await response.json();
      setTopics(data);
      setError(null);
    } catch (e) {
      setError(e.message);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchTopics();
  }, []);

  if (loading) {
    return (
      <div className="flex items-center justify-center min-h-96 bg-gray-50 dark:bg-[#0d1117]">
        <div className="text-center">
          <div className="w-12 h-12 border-4 border-blue-200 border-t-blue-600 rounded-full animate-spin mx-auto mb-4"></div>
          <p className="text-gray-600 dark:text-[#8b949e]">Loading topics...</p>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="flex items-center justify-center min-h-96 bg-gray-50 dark:bg-[#0d1117]">
        <div className="text-center">
          <div className="text-red-600 text-4xl mb-4">⚠️</div>
          <h2 className="text-xl font-semibold mb-2 text-gray-900 dark:text-[#c9d1d9]">Error Loading Topics</h2>
          <p className="mb-4 text-gray-600 dark:text-[#8b949e]">{error}</p>
          <button onClick={fetchTopics} className="bg-blue-600 text-white px-4 py-2 rounded hover:bg-blue-700">
            Retry
          </button>
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-gray-50 dark:bg-[#0d1117]">
      <header className="shadow-sm border-b bg-white dark:bg-[#161b22] border-gray-200 dark:border-[#30363d]">
        <div className="px-4 sm:px-6 lg:px-8">
          <div className="flex justify-between items-center h-16">
            <div className="flex items-center space-x-4">
              <span className="text-gray-500 dark:text-[#8b949e] hover:text-gray-700 dark:hover:text-[#c9d1d9]">Dashboard</span>
              <span className="text-gray-400 dark:text-[#6e7681]">/</span>
              <span className="font-semibold text-gray-900 dark:text-[#c9d1d9]">Topics</span>
            </div>
            <button onClick={fetchTopics} className="px-4 py-2 rounded text-sm bg-gray-600 dark:bg-[#21262d] hover:bg-gray-700 dark:hover:bg-[#30363d] text-white dark:text-[#c9d1d9]">
              Refresh
            </button>
          </div>
        </div>
      </header>

      <main className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        <div className="mb-8">
          <h1 className="text-3xl font-bold mb-2 text-gray-900 dark:text-[#c9d1d9]">Topics</h1>
          <p className="text-gray-600 dark:text-[#8b949e]">Manage and monitor your queue topics</p>
        </div>

        {topics && topics.length > 0 ? (
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
            {topics.map(topic => (
              <TopicCard key={topic.Name} topic={topic} />
            ))}
          </div>
        ) : (
          <div className="rounded-lg p-12 text-center bg-white dark:bg-[#161b22] text-gray-500 dark:text-[#8b949e]">
            <div className="text-lg font-medium">No topics found</div>
            <p className="mt-1">Create some jobs to get started with topics.</p>
          </div>
        )}
      </main>
    </div>
  );
}
