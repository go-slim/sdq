import { useState, useEffect, useMemo } from "preact/hooks";
import { createSubscriber, nextTick } from "../util";

const storageKey = 'sdq/language';
const fallbackLanguage = 'en';
const supportedLanguages = ['en', 'zh'];
const { subscribe, notify } = createSubscriber();

let globalLanguage = localStorage.getItem(storageKey);

export function useLanguage() {
  const [systemLanguage, setSystemState] = useState(() => navigator.language);
  const [userLanguage, setUserLanguageState] = useState(() => globalLanguage);

  const language = useMemo(() => {
    if (userLanguage) return userLanguage;
    if (!systemLanguage) return fallbackLanguage;
    if (supportedLanguages.includes(systemLanguage)) return systemLanguage;
    return fallbackLanguage;
  }, [userLanguage, systemLanguage]);

  const setLanguage = (value) => {
    setUserLanguageState((oldValue) => {
      if (oldValue === value) {
        return oldValue;
      }

      const newValue = typeof value === 'function'
        ? value(oldValue)
        : value;

      if (newValue && !supportedLanguages.includes(newValue)) {
        throw new Error(`expected one of ${supportedLanguages.join(', ')}, got ${newValue}`);
      }

      nextTick(() => {
        globalLanguage = newValue;
        notify();
        if (newValue == null) {
          localStorage.removeItem(storageKey);
        } else {
          localStorage.setItem(storageKey, `${newValue}`);
        }
      });

      return newValue;
    })
  }

  // 监听系统主语言
  useEffect(() => {
    const handler = () => setSystemState(navigator.language);
    handler(); // 初始对齐
    window.addEventListener('languagechange', handler);
    return () => window.removeEventListener('languagechange', handler);
  }, [userLanguage]);


  // 监听本地存储主题
  useEffect(() => {
    const listener = (ev) => {
      if (ev.key === storageKey) {
        setUserLanguageState(ev.newValue);
      }
    };
    window.addEventListener('storage', listener);
    return () => {
      window.removeEventListener('storage', listener);
    }
  }, []);

  // 广播语言
  useEffect(() => {
    return subscribe(() => {
      setUserLanguageState(globalLanguage);
    });
  }, []);

  return {
    systemLanguage,
    userLanguage,
    language,
    setLanguage
  };
}
