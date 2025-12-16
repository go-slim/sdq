import { useLanguage } from '../../hooks/use-language';

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

export function LanguageSection() {
  const { language, setLanguage } = useLanguage();

  // 语言选项
  const languageOptions = [
    { value: 'en', label: 'English' },
    { value: 'zh', label: '中文' }
  ];

  return (
    <SettingsSection title="Language" icon="🌐">
      <div className="py-4">
        <label className="block text-sm font-medium mb-3 text-gray-700 dark:text-[#c9d1d9]">
          Select Language
        </label>
        <div className="grid grid-cols-2 gap-3 max-w-xs">
          {languageOptions.map(option => (
            <button
              key={option.value}
              onClick={() => setLanguage(option.value)}
              className={`p-4 border-2 rounded-lg text-center transition-all hover:scale-105 ${language === option.value
                ? 'border-blue-500 dark:border-[#1f6feb] bg-blue-50 dark:bg-[#1c2d41] text-blue-700 dark:text-[#58a6ff] shadow-sm'
                : 'border-gray-200 dark:border-[#30363d] hover:border-gray-300 dark:hover:border-[#484f58] text-gray-600 dark:text-[#8b949e]'
                }`}
            >
              <div className="text-sm font-medium">{option.label}</div>
            </button>
          ))}
        </div>
      </div>
    </SettingsSection>
  );
}
