import { clsx } from 'clsx';
import { useMemo } from 'preact/hooks';
import { useLocation } from 'preact-iso';
import {
  ListTodoIcon,
  LineChartIcon,
  SettingsIcon,
  DatabaseIcon,
  MonitorIcon,
  MoonIcon,
  SunIcon,
  MenuIcon,
} from 'lucide-preact';
import { useSidebar, useTheme } from '../hooks';

const NavLink = ({ Icon, label, href, subhref, className }) => {
  const { isCollapsed } = useSidebar();
  const { isDark } = useTheme();
  const { path } = useLocation();

  const isActive = useMemo(() => {
    if (!href || !path) return false;
    if (path === href) return true;
    const s = href === '/' && subhref ? subhref : href;
    if (s.length >= path.length) return false;
    if (path[s.length] !== '/') return false;
    return path.startsWith(s);
  }, [path, href, subhref]);

  return (
    <a href={href} className={clsx(
      "flex py-3 rounded-r-full capitalize transition-colors cursor-pointer",
      className,
      isActive
        ? isDark ? 'bg-[#1c2d41] text-[#58a6ff]' : 'bg-blue-100 text-blue-700'
        : isDark ? 'text-[#c9d1d9] hover:bg-[#21262d]' : 'text-gray-700 hover:bg-gray-100',
    )}>
      <span className="shrink-0 w-16 inline-block">
        <Icon className="mx-auto" />
      </span>
      <span className={clsx(
        'transition-opacity flex-1 pointer-events-none',
        isCollapsed ? 'opacity-0' : 'opacity-100',
      )}>{label}</span>
    </a>
  );
};

function ThemeSwitchButton({ className }) {
  const { theme, isDark, toggleTheme } = useTheme();

  const Icon = theme === 'system'
    ? MonitorIcon
    : theme === 'dark'
      ? MoonIcon
      : SunIcon;

  return (
    <button
      type="button"
      onClick={toggleTheme}
      className={clsx(
        "content-center cursor-pointer transition-colors rounded-lg",
        isDark ? 'text-[#c9d1d9] hover:bg-[#21262d]' : 'text-gray-700 hover:bg-gray-100',
        className
      )}
    >
      <Icon className="mx-auto" />
    </button>
  );
}

function SidebarToggleButton({ className }) {
  const { isCollapsed, toggleCollapsed } = useSidebar();
  const { isDark } = useTheme();

  return (
    <button
      type="button"
      onClick={toggleCollapsed}
      className={clsx(
        "content-center cursor-pointer transition-colors rounded-lg",
        isDark ? 'text-[#c9d1d9] hover:bg-[#21262d]' : 'text-gray-700 hover:bg-gray-100',
        className
      )}
    >
      <MenuIcon className="mx-auto" />
    </button>
  );
}

export function Layout({ children }) {
  const { isCollapsed } = useSidebar();
  const { isDark } = useTheme();

  return (
    <>
      <header className="sticky top-0 z-20 h-0">
        <div className={clsx(
          "flex items-center shadow-sm h-16 gap-2 border-b",
          isDark ? 'bg-[#161b22] border-[#30363d]' : 'bg-white border-gray-200'
        )}>
          <div className="shrink-0 size-16 flex items-center justify-center">
            <SidebarToggleButton className="size-12" />
          </div>
          <div className="flex-1">
            <h1 className={clsx(
              "text-2xl font-bold",
              isDark ? 'text-[#c9d1d9]' : 'text-gray-900'
            )}>SDQ Monitoring</h1>
          </div>
          <div className="shrink-0 size-16 flex items-center justify-center">
            <ThemeSwitchButton className="size-12" />
          </div>
        </div>
      </header>
      <div className={clsx(
        "flex flex-1 min-h-screen",
        isDark ? 'bg-[#0d1117]' : 'bg-gray-50'
      )}>
        <aside className={clsx(
          "flex flex-col justify-between h-screen sticky top-0 z-10 pt-20 pb-4 transition-all",
          isCollapsed ? "w-16" : "w-64"
        )}>
          <nav>
            <NavLink Icon={ListTodoIcon} label="Topics" href="/" subhref="/topics" />
            <NavLink Icon={DatabaseIcon} label="Storage" href="/storage" />
            <NavLink Icon={LineChartIcon} label="Metrics" href="/metrics" />
          </nav>
          <div>
            <NavLink Icon={SettingsIcon} label="Settings" href="/settings" />
          </div>
        </aside>
        <main className="flex-1 pt-24 pb-8 px-4 sm:px-6 lg:px-8">
          {children}
        </main>
      </div>
    </>
  );
}
