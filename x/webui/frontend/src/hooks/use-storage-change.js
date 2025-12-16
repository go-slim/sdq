import { useEffect } from 'preact/hooks';

/**
 * @callback ValueChange
 * @param {string|null} value
 *
 * @param {string} key
 * @param {ValueChange} onChange
 */
export function useStorageChange(key, onChange) {
  useEffect(() => {
    if (typeof onChange !== 'function') {
      return;
    }
    /** @param {StorageEvent} ev */
    const listener = (ev) => {
      if (ev.key === key) {
        onChange(ev.newValue);
      }
    };
    window.addEventListener('storage', listener);
    return () => {
      window.removeEventListener('storage', listener);
    }
  }, [key]);
}
