import { useState, useEffect } from "preact/hooks";
import { createSubscriber, nextTick } from "../util";

const storageKey = 'sdq/sidebar';
const { subscribe, notify } = createSubscriber();

let globalCollapsed = (localStorage.getItem(storageKey) ?? 'false') === 'true';

export function useSidebar() {
  const [isCollapsed, setCollapsedState] = useState(() => globalCollapsed);

  const setCollapsed = (value) => {
    setCollapsedState((oldValue) => {
      if (oldValue === value) {
        return oldValue;
      }

      const newValue = typeof value === 'function'
        ? value(oldValue)
        : value;

      nextTick(() => {
        globalCollapsed = newValue;
        notify();
        localStorage.setItem(storageKey, `${newValue}`);
      });

      return newValue;
    })
  }

  const toggleCollapsed = () => {
    setCollapsed(v => !v);
  }

  useEffect(() => {
    return subscribe(() => {
      setCollapsedState(globalCollapsed);
    });
  }, []);

  return { isCollapsed, setCollapsed, toggleCollapsed };
}
