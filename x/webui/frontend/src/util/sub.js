/**
 * 创建一个订阅器
 *
 * @typedef Subscriber
 * @property {(listener: () => void) => void} subscribe - 添加订阅器
 * @property {() => void} notify - 通知所有订阅者
 *
 * @returns {Subscriber}
 */
export function createSubscriber() {
  /** @type {Set<VoidFunction>} */
  const listeners = new Set();

  // 添加订阅器
  const subscribe = (listener) => {
    listeners.add(listener);
    return () => listeners.delete(listener);
  };

  // 通知所有订阅者
  const notify = () => {
    listeners.forEach((listener) => listener());
  };

  return {
    subscribe,
    notify,
  };
}
