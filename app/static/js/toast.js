/*
 * One toast store for the whole app (replaces the per-page implementations).
 *
 *   Alpine.store('toast').ok('Guardado')           success
 *   Alpine.store('toast').error(err.detail)        accepts strings or FastAPI `detail` lists/objects
 *   Alpine.store('toast').warn('Revisa X')
 *   Alpine.store('toast').loading('Procesando…')   stays until replaced by the next toast
 *
 * `legacy(ok, message)` and `show(message, type)` keep the old per-page call sites working
 * (ok = true | false | 'warn' | 'loading'; type = 'success' | 'error' | 'warn').
 */
(function (root) {
  'use strict';

  var DURATION = { ok: 4500, warn: 6000, err: 9000 };

  function createToastStore(setTimeoutFn) {
    var schedule = setTimeoutFn || function (fn, ms) { return setTimeout(fn, ms); };
    var fmt = function (m) { return root.apiErrorMessage ? root.apiErrorMessage(m) : String(m); };
    return {
      items: [],
      _id: 0,

      push: function (type, message) {
        var self = this;
        // A new toast replaces any "loading" one (the loading toast announces the pending result).
        this.items = this.items.filter(function (t) { return t.type !== 'loading'; });
        var id = ++this._id;
        this.items.push({ id: id, type: type, message: fmt(message) });
        if (type !== 'loading') {
          schedule(function () { self.dismiss(id); }, DURATION[type] || DURATION.ok);
        }
        return id;
      },
      dismiss: function (id) {
        this.items = this.items.filter(function (t) { return t.id !== id; });
      },
      ok: function (message) { return this.push('ok', message); },
      warn: function (message) { return this.push('warn', message); },
      error: function (message) { return this.push('err', message); },
      loading: function (message) { return this.push('loading', message); },

      // Old signature used by the settings page: show(message, 'success' | 'error' | 'warn').
      show: function (message, type) {
        return this.push(type === 'error' ? 'err' : type === 'warn' ? 'warn' : 'ok', message);
      },

      // Old signature used by the explorer and pipeline pages.
      legacy: function (ok, message) {
        var type = ok === 'loading' ? 'loading' : ok === 'warn' ? 'warn' : ok ? 'ok' : 'err';
        return this.push(type, message);
      },
    };
  }

  root.createToastStore = createToastStore;
  if (typeof document !== 'undefined') {
    document.addEventListener('alpine:init', function () {
      root.Alpine.store('toast', createToastStore());
    });
  }
  if (typeof module !== 'undefined' && module.exports) module.exports = { createToastStore: createToastStore };
})(typeof window !== 'undefined' ? window : globalThis);
