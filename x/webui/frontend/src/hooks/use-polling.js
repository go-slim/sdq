import { useCallback, useEffect, useState } from "preact/hooks";
import { defaultInterval, usePollingInterval } from "./use-polling-interval";

export function usePolling(opts) {
  const { callback, interval, immediate = true } = opts;
  const { pollingInterval } = usePollingInterval();
  const [isPolling, setIsPolling] = useState(false);
  const firstExecuted = useRef(true);
  const tickTimeRef = useRef(null);
  const tickIdRef = useRef(null);

  const calcDelay = useCallback(() => {
    let delay = interval ?? pollingInterval;
    if (delay < 0) {
      console.error("Polling interval must be a positive number");
      delay = defaultInterval;
    }
    if (tickTimeRef.current === null) {
      return delay;
    }
    const diff = Date.now() - tickTimeRef.current;
    if (diff > 0 && diff < delay) {
      return delay - diff;
    }
    return delay;
  }, [interval, pollingInterval, tickTimeRef.current]);

  const startPolling = useCallback(async () => {
    if (!callback) return;
    setIsPolling(true);
    await execute();
    tickTimeRef.current = Date.now();
    tickIdRef.current = setTimeout(startPolling, calcDelay());
  }, [callback, calcDelay, execute]);

  const stopPolling = useCallback(() => {
    setIsPolling(false);
    clearTimeout(tickIdRef.current);
  }, [tickIdRef.current]);

  useEffect(() => {
    if (!callback || !isPolling) {
      return;
    }

    const start = async () => {
      await callback();
      tickTimeRef.current = Date.now();
      tickIdRef.current = setTimeout(start, calcDelay());
    };

    start();

    return () => {
      clearTimeout(tickIdRef.current);
    };
  }, [callback, calcDelay, isPolling]);

  useEffect(() => {
    if (immediate && firstExecuted.current) {
      firstExecuted.current = false;
      startPolling();
    }
  }, [firstExecuted.current, immediate])

  return {
    startPolling,
    stopPolling,
  };
}
