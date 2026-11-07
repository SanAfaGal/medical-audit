document.addEventListener('alpine:init', () => {
  Alpine.store('ui', {
    consoleOpen: true,
    flowOpen: true,
    sidebarOpen: true,
  });
});

document.addEventListener('alpine:init', () => {
  Alpine.store('pipeline', {
    taskId: null,
    stage: null,
    status: null,   // "running" | "done" | "error" | "cancelled" | null
    cursor: 0,      // último idx recibido (para reconexión)
    institutionId: null,   // hospital/periodo al que pertenece la tarea de ESTA pestaña
    periodId: null,
    institutionName: '',
    progress: null, // {current, total, invoice} de la descarga en curso

    // sessionStorage: cada pestaña tiene su propia tarea, así se puede trabajar
    // en varios hospitales a la vez abriendo varias pestañas.
    init() {
      try {
        const s = JSON.parse(sessionStorage.getItem('pipeline_task') || 'null');
        if (s) {
          this.taskId = s.taskId;
          this.stage  = s.stage;
          this.status = s.status;
          this.cursor = s.cursor || 0;
          this.institutionId   = s.institutionId ?? null;
          this.periodId        = s.periodId ?? null;
          this.institutionName = s.institutionName || '';
          this.progress        = s.progress || null;
        }
      } catch {}
    },
    setTask(taskId, stage, institutionId = null, periodId = null, institutionName = '') {
      this.taskId = taskId; this.stage = stage; this.status = 'running'; this.cursor = 0;
      this.institutionId = institutionId; this.periodId = periodId;
      this.institutionName = institutionName; this.progress = null;
      this._save();
    },
    setProgress(progress) {
      this.progress = progress; this._save();
    },
    update(status, cursor) {
      this.status = status; this.cursor = cursor; this._save();
    },
    clear() {
      this.taskId = null; this.stage = null; this.status = null; this.cursor = 0;
      this.institutionId = null; this.periodId = null; this.institutionName = ''; this.progress = null;
      try { sessionStorage.removeItem('pipeline_task'); } catch {}
    },
    get isRunning() { return this.status === 'running'; },
    _save() {
      try {
        sessionStorage.setItem('pipeline_task', JSON.stringify({
          taskId: this.taskId, stage: this.stage, status: this.status, cursor: this.cursor,
          institutionId: this.institutionId, periodId: this.periodId,
          institutionName: this.institutionName, progress: this.progress,
        }));
      } catch {}
    },
  });
});

document.addEventListener('alpine:init', () => {
  Alpine.store('ctx', {
    institutions: [],
    institutionId: null,
    institution:   null,
    periods:       [],
    periodId:      null,
    period:        null,
    ready:         false,

    init() {
      try {
        // Per tab (sessionStorage); una pestaña nueva arranca con el último contexto usado (localStorage).
        const raw = sessionStorage.getItem('audit_ctx') || localStorage.getItem('audit_ctx') || '{}';
        const s = JSON.parse(raw);
        this.institutionId = s.institutionId || null;
        this.institution   = s.institution   || null;
        this.periodId      = s.periodId      || null;
        this.period        = s.period        || null;
      } catch {}
      this._loadInstitutions();
    },

    async _loadInstitutions() {
      const res = await api.get('/api/institutions');
      if (!res.ok) Alpine.store('toast').error(res.error);
      this.institutions = res.ok ? res.data : [];
      if (this.institutionId) {
        const found = this.institutions.find(i => i.id == this.institutionId);
        if (found) { this.institution = found; await this._loadPeriods(); }
        else this.clear();
      }
      this.ready = true;
    },

    async _loadPeriods() {
      if (!this.institutionId) { this.periods = []; return; }
      const res = await api.get(`/api/institutions/${this.institutionId}/periods`);
      if (!res.ok) Alpine.store('toast').error(res.error);
      this.periods = res.ok ? res.data : [];
      if (this.periodId) {
        const found = this.periods.find(p => p.id == this.periodId);
        if (found) this.period = found;
        else { this.periodId = null; this.period = null; }
      }
      this._save();
    },

    async selectInstitution(id) {
      this.institutionId = id ? parseInt(id) : null;
      this.institution   = this.institutions.find(i => i.id == this.institutionId) || null;
      this.periodId = null; this.period = null; this.periods = [];
      this._save();
      if (this.institutionId) await this._loadPeriods();
    },

    selectPeriod(id) {
      this.periodId = id ? parseInt(id) : null;
      this.period   = this.periods.find(p => p.id == this.periodId) || null;
      this._save();
    },

    clear() {
      this.institutionId = null; this.institution = null;
      this.periodId      = null; this.period      = null;
      this.periods = [];
      this._save();
    },

    _save() {
      const payload = JSON.stringify({
        institutionId: this.institutionId, institution: this.institution,
        periodId:      this.periodId,      period:      this.period,
      });
      try { sessionStorage.setItem('audit_ctx', payload); } catch {}
      try { localStorage.setItem('audit_ctx', payload); } catch {}  // solo para pestañas nuevas
    }
  });
});
