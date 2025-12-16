import { useTheme } from "../../hooks";
import { ThemePreviewWrapper } from "./theme-preview-wrapper";

const themeOptions = [
  { value: 'system', label: 'Sync with system', description: 'The theme will match your system active settings' },
  { value: 'light', label: 'Light theme', description: 'Use light theme all the time' },
  { value: 'dark', label: 'Dark theme', description: 'Use dark theme all the time' }
];

const ThemeSelect = () => {
  const { setTheme, theme } = useTheme();

  return (
    <div className="mb-8 ">
      <label className="block font-semibold mb-3">Theme mode</label>
      <select
        value={theme}
        onChange={(e) => setTheme(e.target.value)}
        className="border border-gray-300 dark:border-gray-700 rounded-md px-4 py-2 pr-10 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500 appearance-none cursor-pointer w-80"
        style={{
          backgroundImage: `url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' fill='none' viewBox='0 0 20 20'%3E%3Cpath stroke='%236b7280' stroke-linecap='round' stroke-linejoin='round' stroke-width='1.5' d='m6 8 4 4 4-4'/%3E%3C/svg%3E")`,
          backgroundPosition: 'right 0.5rem center',
          backgroundRepeat: 'no-repeat',
          backgroundSize: '1.5em 1.5em',
        }}
      >
        {themeOptions.map(option => (
          <option key={option.value} value={option.value}>
            {option.label}
          </option>
        ))}
      </select>
      <p className="text-[#8b949e] text-sm mt-2">
        {themeOptions.find(opt => opt.value === theme)?.description}
      </p>
    </div>
  )
}

export function ThemePreferencesSection() {
  return (
    <div className="p-8 rounded-lg border border-[#30363d] mb-6">
      <h2 className="text-2xl font-semibold  mb-2">Theme preferences</h2>
      <p className="text-sm mb-6 leading-relaxed max-w-4xl">
        Choose how GitHub looks to you. Select a single theme, or sync with your system and automatically switch between day and night themes. Selections are applied immediately and saved automatically.
      </p>

      <ThemeSelect />

      <div className="grid grid-cols-2 gap-8">
        <ThemePreviewWrapper dark={false} />
        <ThemePreviewWrapper dark={true} />
      </div>
    </div>
  );
}
