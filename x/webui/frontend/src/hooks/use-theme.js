import { useEffect, useMemo, useState } from 'preact/hooks';
import { createSubscriber } from '../util';

const storageKey = 'sdq/theme';
const values = ['light', 'dark', 'system'];
const { subscribe, notify } = createSubscriber();

let globalTheme = localStorage.getItem(storageKey) ?? 'system';

const MQL = () => matchMedia('(prefers-color-scheme: dark)')

export function useTheme() {
  const [system, setSystem] = useState(() => MQL().matches ? 'dark' : 'light');
  const [theme, setThemeState] = useState(() => globalTheme);

  const setTheme = (value) => {
    if (!values.includes(value)) {
      throw new Error(`expected one of ${values.join(', ')}, got ${value}`);
    }
    globalTheme = value;
    setThemeState(value)
    notify();
    if (value === 'system') {
      localStorage.removeItem(storageKey);
    } else {
      localStorage.setItem(storageKey, value);
    }
  };

  const toggleTheme = () => {
    const next = values[(values.indexOf(theme) + 1) % 3];
    setTheme(next);
  };

  const isDark = useMemo(() => {
    if (theme === 'system') {
      return system === 'dark';
    } else {
      return theme === 'dark';
    }
  }, [theme, system]);

  // 监听系统主题
  useEffect(() => {
    const mql = matchMedia('(prefers-color-scheme: dark)');
    const handler = () => setSystem(mql.matches ? 'dark' : 'light');
    handler(); // 初始对齐
    mql.addEventListener('change', handler);
    return () => mql.removeEventListener('change', handler);
  }, [theme]);

  // 监听本地存储主题
  useEffect(() => {
    const listener = (ev) => {
      if (ev.key === storageKey) {
        if (values.includes(ev.newValue)) {
          setThemeState(ev.newValue);
        }
      }
    };
    window.addEventListener('storage', listener);
    return () => {
      window.removeEventListener('storage', listener);
    }
  }, [])

  // 落地到 DOM 上面
  useEffect(() => {
    const auto = theme === 'system';
    const name = auto ? system : theme;
    const cls = document.documentElement.classList;
    cls.remove('light', 'dark');
    cls.add(name);
  }, [system, theme]);

  // 广播主题
  useEffect(() => {
    return subscribe(() => {
      setThemeState(globalTheme);
    });
  }, []);

  return {
    theme,
    setTheme,
    toggleTheme,
    system,
    isDark,
  };
}
