import { useEffect, useState } from "preact/hooks";
import { nextTick, createSubscriber } from "../util";

const storageKey = 'sdq/pollingInterval';
const defaultInterval = 60000;
const { subscribe, notify } = createSubscriber();

const parseInterval = (value) => {
  if (value == null) return defaultInterval;
  const interval = parseInt(value, 10);
  return isNaN(interval) ? defaultInterval : interval;
};

let globalInterval = (() => {
  const value = localStorage.getItem(storageKey);
  return parseInterval(value);
})();

export function usePollingInterval() {
  const [pollingInterval, setPollingIntervalState] = useState(() => globalInterval);

  const setPollingInterval = (value) => {
    setPollingIntervalState((oldValue) => {
      if (oldValue === value) {
        return oldValue;
      }

      const newValue = typeof value === 'function'
        ? value(oldValue)
        : parseInterval(value);

      nextTick(() => {
        globalInterval = newValue;
        notify(newValue);
        if (value != null) {
          localStorage.setItem(storageKey, newValue);
        } else {
          localStorage.removeItem(storageKey);
        }
      })

      return newValue;
    })
  };

  // 监听本地存储
  useEffect(() => {
    const listener = (ev) => {
      if (ev.key === storageKey) {
        setPollingIntervalState(parseInterval(ev.newValue));
      }
    };
    window.addEventListener('storage', listener);
    return () => {
      window.removeEventListener('storage', listener);
    }
  }, []);

  // 广播轮询间隔
  useEffect(() => {
    return subscribe(() => {
      setPollingIntervalState(globalInterval);
    });
  }, []);

  return {
    pollingInterval,
    setPollingInterval,
  };
}


export {
  defaultInterval,
}
