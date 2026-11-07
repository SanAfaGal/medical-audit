/*
 * Shared client helpers: one place that knows how the API reports errors.
 *
 *   apiErrorMessage(detail, fallback)  -> human readable text for any FastAPI `detail`
 *   api.get/post/put/patch/delete(url, body?, opts?)  -> {ok, status, data, error}
 *
 * `api.*` never throws on HTTP errors or network failures, so callers just check `ok`.
 */
(function (root) {
  'use strict';

  // FastAPI sends `detail` as a string, or a list of {loc, msg} for validation errors (422).
  function apiErrorMessage(detail, fallback) {
    fallback = fallback || 'Error inesperado.';
    if (detail === null || detail === undefined || detail === '') return fallback;
    if (typeof detail === 'string') return detail;
    if (Array.isArray(detail)) {
      const parts = detail.map(function (e) {
        if (typeof e === 'string') return e;
        if (!e || typeof e !== 'object') return String(e);
        const field = Array.isArray(e.loc) ? e.loc.filter(function (p) { return p !== 'body'; }).join('.') : '';
        const msg = e.msg || e.message || JSON.stringify(e);
        return field ? field + ': ' + msg : msg;
      }).filter(Boolean);
      return parts.length ? parts.join(' · ') : fallback;
    }
    if (typeof detail === 'object') return detail.msg || detail.message || detail.detail || fallback;
    return String(detail);
  }

  async function request(method, url, body, opts) {
    opts = opts || {};
    const init = { method: method, headers: Object.assign({}, opts.headers) };
    if (opts.signal) init.signal = opts.signal;
    if (typeof FormData !== 'undefined' && body instanceof FormData) {
      init.body = body;                       // the browser sets the multipart boundary itself
    } else if (body !== undefined && body !== null) {
      init.headers['Content-Type'] = 'application/json';
      init.body = JSON.stringify(body);
    }

    let res;
    try {
      res = await fetch(url, init);
    } catch (e) {
      return { ok: false, status: 0, data: null, error: 'No se pudo conectar con el servidor.' };
    }

    let data = null;
    const type = res.headers.get('content-type') || '';
    if (res.status !== 204) {
      try { data = type.indexOf('json') !== -1 ? await res.json() : await res.text(); } catch (e) { data = null; }
    }
    if (res.ok) return { ok: true, status: res.status, data: data, error: null };
    const detail = data && typeof data === 'object' ? data.detail : data;
    return { ok: false, status: res.status, data: data, error: apiErrorMessage(detail, 'Error ' + res.status + '.') };
  }

  const api = {
    request: request,
    get: function (url, opts) { return request('GET', url, undefined, opts); },
    post: function (url, body, opts) { return request('POST', url, body, opts); },
    put: function (url, body, opts) { return request('PUT', url, body, opts); },
    patch: function (url, body, opts) { return request('PATCH', url, body, opts); },
    delete: function (url, opts) { return request('DELETE', url, undefined, opts); },
  };

  root.apiErrorMessage = apiErrorMessage;
  root.api = api;
  if (typeof module !== 'undefined' && module.exports) module.exports = { apiErrorMessage: apiErrorMessage, api: api };
})(typeof window !== 'undefined' ? window : globalThis);
