import { useEffect, useState } from "preact/hooks";
import { useStorageChange } from './use-storage-change';
import { createSubscriber } from '../util';

export const supportedColors = ['indigo', 'red', 'amber', 'lime', 'emerald', 'fuchsia'];
export const defaultColor = 'indigo';

const storageKey = 'sdq/color';
const { subscribe, notify } = createSubscriber();

let globalColor = (() => {
  const storedColor = localStorage.getItem(storageKey);
  if (!storedColor) return defaultColor;
  if (!supportedColors.includes(storedColor)) return defaultColor;
  return storedColor;
})();

export function useColor() {
  const [color, setColorState] = useState(() => globalColor);

  const setColor = (color) => {
    color ??= defaultColor;
    if (!supportedColors.includes(color)) {
      console.error(`expected one of ${supportedColors.join(', ')}, got ${color}`);
    } else {
      globalColor = color;
      setColorState(color);
      notify();
      localStorage.setItem(storageKey, color);
    }
  };

  useStorageChange(storageKey, (value) => {
    value ??= defaultColor;
    if (supportedColors.includes(value)) {
      globalColor = value;
      setColorState(value);
    }
  });

  useEffect(() => {
    return subscribe(() => {
      setColorState(globalColor);
    });
  }, []);

  return {
    color,
    setColor
  }
}
