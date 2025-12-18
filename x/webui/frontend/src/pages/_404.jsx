export function NotFound() {
  return (
    <div className="flex items-center justify-center min-h-screen bg-gray-50 dark:bg-[#0d1117]">
      <div className="text-center">
        <div className="text-6xl font-bold mb-4 text-gray-300 dark:text-[#30363d]">404</div>
        <h1 className="text-2xl font-semibold mb-2 text-gray-900 dark:text-[#c9d1d9]">Page Not Found</h1>
        <p className="mb-6 text-gray-600 dark:text-[#8b949e]">The page you're looking for doesn't exist.</p>
        <a href="" className="bg-blue-600 text-white px-6 py-2 rounded-lg hover:bg-blue-700">
          Go to Dashboard
        </a>
      </div>
    </div>
  );
}
