let canUseMicrotask = typeof enqueueMicrotask === 'function';
let flushPromise = null;

export function nextTick(callback) {
  if (canUseMicrotask) {
    enqueueMicrotask(callback);
  } else if (flushPromise) {
    // 同一 tick 里的调用直接复用已有的 flushPromise
    flushPromise.then(callback);
  } else {
    // 第一次调用时创建微任务，完成后清空缓存
    flushPromise = Promise.resolve().then(() => {
      flushPromise = null;
      callback();
    });
  }
}
