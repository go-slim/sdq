import { clsx } from 'clsx';
import { createContext } from 'preact';
import { useState, useContext } from 'preact/hooks';
import { MoonIcon, SunIcon } from "lucide-preact";
import { useTheme, useColor, supportedColors } from '../../hooks';
import { ToggleSwitch } from '../toggle-switch';
import { ThemePreview } from './theme-preview';

const HoveringColorContext = createContext({
  hoveringColor: null,
  setHoveringColor: () => { },
});

export function ColorButton({ value }) {
  const { color, setColor } = useColor();
  const { setHoveringColor } = useContext(HoveringColorContext);

  return (
    <button
      type="button"
      onClick={() => setColor(value)}
      onMouseEnter={() => setHoveringColor(value)}
      onMouseLeave={() => setHoveringColor(null)}
      style={{ '--color-current': `var(--color-${value})` }}
      className={clsx(
        "ring-0 p-0 size-8 rounded-full cursor-pointer transition-all ease-in-out",
        "hover:ring-2 hover:p-1",
        "after:content-[''] after:block after:rounded-full after:bg-[var(--color-current)] after:size-full",
        color === value
          ? 'ring-2 p-1 after:opacity-100 ring-[var(--color-current)]'
          : 'ring-gray-200 hover:ring-gray-400'
      )}
    >
    </button>
  );
}

export function ThemePreviewWrapper({ dark }) {
  const { isDark, setTheme, theme } = useTheme();
  const { color } = useColor();
  const [hoveringColor, setHoveringColor] = useState(null);
  const isActive = dark === isDark;

  return (
    <HoveringColorContext.Provider value={{ hoveringColor, setHoveringColor }}>
      <div className={clsx(
        "border rounded-md overflow-hidden mb-12",
        isActive
          ? 'border-blue-300 dark:border-blue-900'
          : 'border-gray-300 dark:border-gray-700'
      )}>
        <div className={clsx(
          "px-6 py-4 border-b",
          isActive
            ? 'bg-blue-100 border-blue-300 dark:bg-blue-900/20 dark:border-blue-900'
            : 'border-gray-300 dark:border-gray-700 dark:bg-gray-800'
        )}>
          <div className="flex items-center gap-2 h-6.5">
            {dark ? <MoonIcon size={18} /> : <SunIcon size={18} />}
            <span className="font-semibold">{dark ? 'Dark' : 'Light'} theme</span>
            <span className="flex-1"></span>
            {isActive && (
              <span className="text-xs text-[#1f6feb] font-semibold px-2 py-1 rounded-full border border-[#1f6feb]">
                Active
              </span>
            )}
          </div>
        </div>
        <div className="px-6 my-4">
          <p className="text-[#8b949e] text-sm">
            This theme will be active when your system is set to "{dark ? 'dark' : 'light'} mode"
          </p>
        </div>
        <div className={clsx('px-6 pb-6', dark ? 'dark' : 'light')}>
          <ThemePreview dark={dark} color={hoveringColor ?? color} />
        </div>
        <div className="flex items-center justify-between gap-2 mx-6 mb-4">
          <div className="flex gap-2">
            {supportedColors.map(color => (
              <ColorButton key={color} value={color} />
            ))}
          </div>
          {theme !== 'system' && (
            <div>
              <ToggleSwitch
                value={theme}
                trueValue={dark ? 'dark' : 'light'}
                falseValue={dark ? 'light' : 'dark'}
                onChange={setTheme} />
            </div>
          )}
        </div>
      </div>
    </HoveringColorContext.Provider>
  );
}
