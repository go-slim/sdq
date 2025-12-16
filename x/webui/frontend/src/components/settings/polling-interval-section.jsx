import { usePollingInterval } from '../../hooks/use-polling-interval';

// Settings section wrapper
const SettingsSection = ({ title, children, icon }) => (
  <div className="p-6 rounded-lg shadow-sm border mb-6 bg-white dark:bg-[#161b22] border-gray-200 dark:border-[#30363d]">
    <div className="flex items-center mb-4">
      <div className="w-8 h-8 rounded-lg flex items-center justify-center mr-3 bg-blue-100 dark:bg-[#21262d]">
        {icon}
      </div>
      <h2 className="text-xl font-semibold text-gray-900 dark:text-[#c9d1d9]">{title}</h2>
    </div>
    {children}
  </div>
);

// Slider component - 限制在1s-30s之间
const Slider = ({ label, value, onChange, min, max, formatter, pollingIntervals }) => (
  <div className="py-4">
    <div className="flex justify-between items-center mb-3">
      <label className="text-sm font-medium text-gray-700 dark:text-[#c9d1d9]">{label}</label>
      <span className="text-sm font-medium text-blue-600 dark:text-[#58a6ff]">
        {formatter ? formatter(value) : `${value}ms`}
      </span>
    </div>
    <input
      type="range"
      min={min}
      max={max}
      step={1000}
      value={value}
      onChange={(e) => onChange(parseInt(e.target.value))}
      className="w-full h-2 rounded-lg appearance-none cursor-pointer bg-gray-200 dark:bg-[#21262d] accent-blue-600 dark:accent-[#58a6ff]"
    />
    <div className="flex justify-between text-xs mt-1 text-gray-500 dark:text-[#8b949e]">
      {pollingIntervals.map(interval => (
        <span key={interval.value}>{interval.label}</span>
      ))}
    </div>
  </div>
);

export function PollingIntervalSection() {
  const { pollingInterval, setPollingInterval } = usePollingInterval();

  // 轮询间隔选项 (ms) - 只支持1s-30s
  const pollingIntervals = [
    { value: 1000, label: '1s' },
    { value: 2000, label: '2s' },
    { value: 3000, label: '3s' },
    { value: 5000, label: '5s' },
    { value: 10000, label: '10s' },
    { value: 15000, label: '15s' },
    { value: 20000, label: '20s' },
    { value: 30000, label: '30s' }
  ];

  return (
    <SettingsSection title="Polling Interval" icon="⚡">
      <div className="py-4">
        <Slider
          label="Auto Refresh Interval"
          value={pollingInterval}
          onChange={setPollingInterval}
          min={1000}
          max={30000}
          pollingIntervals={pollingIntervals}
          formatter={(value) => {
            if (value === 0) return 'Disabled';
            return `${value / 1000}s`;
          }}
        />
        <div className="mt-4 p-3 rounded-md bg-gray-50 dark:bg-[#0d1117]">
          <p className="text-sm text-gray-600 dark:text-[#8b949e]">
            {pollingInterval === 0
              ? 'Auto refresh is disabled. Dashboard will not update automatically.'
              : `Dashboard will refresh every ${pollingInterval / 1000} seconds.`
            }
          </p>
        </div>
      </div>
    </SettingsSection>
  );
}
