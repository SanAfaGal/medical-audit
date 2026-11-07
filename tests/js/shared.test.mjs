// Run with: node --test tests/js
import test from 'node:test';
import assert from 'node:assert/strict';
import { createRequire } from 'node:module';

const require = createRequire(import.meta.url);
const { apiErrorMessage, api } = require('../../app/static/js/api.js');
globalThis.apiErrorMessage = apiErrorMessage;
const { createToastStore } = require('../../app/static/js/toast.js');

test('apiErrorMessage: strings pass through, empty values use the fallback', () => {
  assert.equal(apiErrorMessage('No encontrado'), 'No encontrado');
  assert.equal(apiErrorMessage(undefined, 'Fallo'), 'Fallo');
  assert.equal(apiErrorMessage(''), 'Error inesperado.');
});

test('apiErrorMessage: FastAPI 422 list becomes "campo: mensaje" (never [object Object])', () => {
  const detail = [
    { type: 'string_pattern_mismatch', loc: ['body', 'name'], msg: 'String should match pattern' },
    { loc: ['body', 'nit'], msg: 'Field required' },
  ];
  assert.equal(apiErrorMessage(detail), 'name: String should match pattern · nit: Field required');
  assert.ok(!apiErrorMessage(detail).includes('[object'));
  assert.equal(apiErrorMessage({ msg: 'boom' }), 'boom');
  assert.equal(apiErrorMessage([]), 'Error inesperado.');
});

test('api.request: parses errors, handles 204 and network failures without throwing', async () => {
  const responses = [
    new Response(JSON.stringify({ detail: [{ loc: ['body', 'x'], msg: 'bad' }] }), {
      status: 422,
      headers: { 'content-type': 'application/json' },
    }),
    new Response(null, { status: 204 }),
    new Response(JSON.stringify({ a: 1 }), { status: 200, headers: { 'content-type': 'application/json' } }),
  ];
  const calls = [];
  globalThis.fetch = async (url, init) => {
    calls.push([url, init]);
    const next = responses.shift();
    if (!next) throw new TypeError('offline');
    return next;
  };

  const bad = await api.post('/x', { n: 1 });
  assert.deepEqual([bad.ok, bad.status, bad.error], [false, 422, 'x: bad']);
  assert.equal(calls[0][1].headers['Content-Type'], 'application/json');
  assert.equal(calls[0][1].body, '{"n":1}');

  const empty = await api.delete('/x');
  assert.deepEqual([empty.ok, empty.status, empty.data], [true, 204, null]);

  const ok = await api.get('/x');
  assert.deepEqual(ok.data, { a: 1 });

  const offline = await api.get('/x');
  assert.deepEqual([offline.ok, offline.status], [false, 0]);
  assert.match(offline.error, /conectar/);
});

test('toast store: types, auto-dismiss, loading replacement and legacy signature', () => {
  const timers = [];
  const store = createToastStore((fn, ms) => timers.push({ fn, ms }));

  store.ok('hecho');
  store.error([{ loc: ['body', 'name'], msg: 'inválido' }]);
  assert.deepEqual(
    store.items.map((t) => [t.type, t.message]),
    [['ok', 'hecho'], ['err', 'name: inválido']],
  );
  assert.ok(timers[1].ms > timers[0].ms, 'errors stay on screen longer than successes');

  timers.forEach((t) => t.fn());
  assert.equal(store.items.length, 0);

  store.loading('Procesando');
  assert.equal(store.items[0].type, 'loading');
  assert.equal(timers.length, 2, 'loading toasts have no timer');
  store.legacy(true, 'listo'); // a result replaces the pending loading toast
  assert.deepEqual(store.items.map((t) => t.type), ['ok']);

  store.legacy(false, 'x');
  store.legacy('warn', 'y');
  assert.deepEqual(store.items.map((t) => t.type), ['ok', 'err', 'warn']);
});
