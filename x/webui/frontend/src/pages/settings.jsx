import { useState } from 'preact/hooks';
import { useTheme } from '../hooks/use-theme';
import { useLanguage } from '../hooks/use-language';
import { usePollingInterval } from '../hooks/use-polling-interval';
import { useSidebar } from '../hooks/use-sidebar';

// Import section components
import { ThemePreferencesSection } from '../components/settings/theme-preferences-section';
import { LanguageSection } from '../components/settings/language-section';
import { PollingIntervalSection } from '../components/settings/polling-interval-section';
import { SidebarSection } from '../components/settings/sidebar-section';

export function Settings() {
  const { theme, setTheme } = useTheme();
  const { language, setLanguage } = useLanguage();
  const { pollingInterval, setPollingInterval } = usePollingInterval();
  const { isCollapsed: sidebarCollapsed, setCollapsed: setSidebarCollapsed } = useSidebar();

  const [saved, setSaved] = useState(false);

  const saveSettings = async () => {
    try {
      // 模拟保存 - 实际应用中会保存到后端
      await new Promise(resolve => setTimeout(resolve, 500));
      setSaved(true);
      setTimeout(() => setSaved(false), 2000);
    } catch (e) {
      console.error('Failed to save settings:', e);
    }
  };

  const resetSettings = () => {
    setTheme('system');
    setLanguage(null);
    setPollingInterval(60000);
    setSidebarCollapsed(false);
  };

  return (
    <>
      <div className="flex justify-between items-center h-16 mb-8">
        <div className="flex items-center space-x-4">
          <span className="text-gray-500 dark:text-[#8b949e] hover:text-gray-700 dark:hover:text-[#c9d1d9]">Dashboard</span>
          <span className="text-gray-400 dark:text-[#6e7681]">/</span>
          <span className="font-semibold text-gray-900 dark:text-[#c9d1d9]">Settings</span>
        </div>
        <button
          onClick={saveSettings}
          disabled={saved}
          className={`px-4 py-2 rounded text-sm font-medium transition-colors ${saved
            ? 'bg-green-600 hover:bg-green-700 text-white'
            : 'bg-blue-600 hover:bg-blue-700 text-white'
            }`}
        >
          {saved ? 'Saved!' : 'Save Changes'}
        </button>
      </div>

      <div className="mb-8">
        <h1 className="text-3xl font-bold mb-2 text-gray-900 dark:text-[#c9d1d9]">Settings</h1>
        <p className="text-gray-600 dark:text-[#8b949e]">Configure your SDQ WebUI preferences</p>
      </div>

      {/* 操作按钮 */}
      <div className="flex space-x-4 mb-6">
        <button
          onClick={saveSettings}
          className="px-4 py-2 bg-blue-600 text-white rounded hover:bg-blue-700 transition-colors"
        >
          Save All Settings
        </button>
        <button
          onClick={resetSettings}
          className="px-4 py-2 rounded transition-colors bg-gray-600 dark:bg-[#21262d] hover:bg-gray-700 dark:hover:bg-[#30363d] text-white dark:text-[#c9d1d9]"
        >
          Reset to Defaults
        </button>
      </div>

      {/* 主题设置 - GitHub Style */}
      <ThemePreferencesSection />

      {/* 语言设置 */}
      <LanguageSection />

      {/* 轮询间隔设置 */}
      <PollingIntervalSection />

      {/* 侧边栏控制 */}
      <SidebarSection />
    </>
  );
}
