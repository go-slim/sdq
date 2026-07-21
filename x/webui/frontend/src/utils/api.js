export async function api(path, options = {}) {
  const requestPath = String(path).replace(/^\/+/, "");
  const response = await fetch(new URL(requestPath, document.baseURI), {
    ...options,
    headers: {
      Accept: "application/json",
      ...options.headers,
    },
  });
  if (!response.ok) {
    let message = `HTTP ${response.status}`;
    try {
      const body = await response.json();
      message = body.error || message;
    } catch {
      // Keep the status-based fallback when the response is not JSON.
    }
    throw new Error(message);
  }
  if (response.status === 204) return null;
  return response.json();
}
