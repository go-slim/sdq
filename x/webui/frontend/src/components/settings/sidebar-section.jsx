import { useSidebar } from '../../hooks/use-sidebar';

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

// Toggle switch
const Toggle = ({ label, description, enabled, onToggle }) => (
  <div className="flex items-center justify-between py-3 border-b border-gray-100 dark:border-[#21262d]">
    <div>
      <div className="font-medium text-gray-900 dark:text-[#c9d1d9]">{label}</div>
      {description && <div className="text-sm mt-1 text-gray-500 dark:text-[#8b949e]">{description}</div>}
    </div>
    <button
      onClick={onToggle}
      className={`relative inline-flex h-6 w-11 items-center rounded-full transition-colors focus:outline-none ${enabled ? 'bg-blue-600' : 'bg-gray-200 dark:bg-[#21262d]'
        }`}
    >
      <span className={`inline-block h-4 w-4 rounded-full bg-white transition-transform ${enabled ? 'translate-x-6' : 'translate-x-1'
        }`} />
    </button>
  </div>
);

export function SidebarSection() {
  const { isCollapsed, setCollapsed } = useSidebar();

  return (
    <SettingsSection title="Sidebar" icon="📱">
      <div className="py-4">
        <Toggle
          label="Collapsed Sidebar by Default"
          description="Keep sidebar collapsed when loading the application"
          enabled={isCollapsed}
          onToggle={() => setCollapsed(!isCollapsed)}
        />

        <div className="mt-4 p-3 rounded-md bg-gray-50 dark:bg-[#0d1117]">
          <p className="text-sm text-gray-600 dark:text-[#8b949e]">
            {isCollapsed
              ? 'Sidebar will be collapsed by default. Click the menu icon to expand.'
              : 'Sidebar will be expanded by default to show navigation options.'
            }
          </p>
        </div>

        <div className="mt-4 p-3 border rounded-md border-gray-200 dark:border-[#30363d]">
          <h4 className="text-sm font-medium mb-2 text-gray-700 dark:text-[#c9d1d9]">Preview</h4>
          <div className="flex items-center space-x-2">
            <div className={`w-8 h-8 rounded transition-all ${isCollapsed ? 'opacity-50' : 'opacity-100'} bg-blue-100 dark:bg-[#21262d]`}>
              <div className="w-full h-full flex items-center justify-center text-xs">
                {isCollapsed ? '☰' : '☰'}
              </div>
            </div>
            <div className={`flex-1 h-8 rounded transition-all ${isCollapsed ? 'opacity-30' : 'opacity-100'} bg-gray-100 dark:bg-[#21262d]`} />
          </div>
        </div>
      </div>
    </SettingsSection>
  );
}
