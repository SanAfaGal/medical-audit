// ── Reusable CRUD panel factory ────────────────────────────────────────────
function crudPanel({ apiBase, label, icon }) {
  return {
    items: [], loading: false, saving: false, search: '',
    modalOpen: false, modalMode: 'create',
    modalForm: { raw_name: '', canonical_name: '' }, modalEditId: null,
    deleteModalOpen: false, deleteTargetId: null, deleteTargetName: '',

    get filtered() {
      const q = this.search.toLowerCase();
      if (!q) return this.items;
      return this.items.filter(i =>
        (i.raw_name || '').toLowerCase().includes(q) ||
        (i.canonical_name || '').toLowerCase().includes(q)
      );
    },
    get pendingCount() { return this.items.filter(i => !i.canonical_name).length; },
    get totalCount()   { return this.items.length; },

    async init() { await this.load(); },
    async load() {
      this.loading = true;
      try {
        const res = await api.get(apiBase);
        if (!res.ok) this.$store.toast.show(res.error, 'error');
        this.items = res.ok ? res.data : [];
      } finally { this.loading = false; }
    },

    openCreate() {
      this.modalForm = { raw_name: '', canonical_name: '' };
      this.modalMode = 'create'; this.modalEditId = null; this.modalOpen = true;
    },
    openEdit(item) {
      this.modalForm = { raw_name: item.raw_name, canonical_name: item.canonical_name || '' };
      this.modalMode = 'edit'; this.modalEditId = item.id; this.modalOpen = true;
    },
    closeModal() { this.modalOpen = false; },

    async saveModal() {
      if (this.saving) return;
      this.saving = true;
      try {
        let res;
        if (this.modalMode === 'create') {
          const body = { raw_name: this.modalForm.raw_name };
          if (this.modalForm.canonical_name) body.canonical_name = this.modalForm.canonical_name;
          res = await api.post(apiBase, body);
        } else {
          res = await api.patch(`${apiBase}/${this.modalEditId}`, { canonical_name: this.modalForm.canonical_name || null });
        }
        if (res.ok) {
          await this.load(); this.closeModal();
          this.$store.toast.show(this.modalMode === 'create' ? `${label} agregado/a.` : 'Actualizado.');
        } else {
          this.$store.toast.show(res.error, 'error');
        }
      } finally { this.saving = false; }
    },

    confirmDelete(item) {
      this.deleteTargetId = item.id; this.deleteTargetName = item.raw_name; this.deleteModalOpen = true;
    },
    cancelDelete() { this.deleteModalOpen = false; this.deleteTargetId = null; this.deleteTargetName = ''; },
    async executeDelete() {
      const res = await api.delete(`${apiBase}/${this.deleteTargetId}`);
      if (res.ok) {
        await this.load(); this.cancelDelete(); this.$store.toast.show('Eliminado.');
      } else {
        this.cancelDelete(); this.$store.toast.show(res.error, 'error');
      }
    },
  };
}

function settingsApp() {
  return {
    tab: 'instituciones',

    navTabs: [
      { id: 'instituciones', icon: 'fa-hospital',          label: 'Instituciones', scope: 'global'      },
      { id: 'normalizacion', icon: 'fa-shuffle',           label: 'Normalización', scope: 'global'      },
      { id: 'reglas',        icon: 'fa-sliders',            label: 'Reglas',        scope: 'global'      },

      { id: 'mapeos',        icon: 'fa-arrows-left-right',  label: 'Mapeos',        scope: 'institution' },
      { id: 'periodos',      icon: 'fa-calendar-days',      label: 'Períodos',      scope: 'institution' },
    ],

    // Instituciones
    institutions: [],
    editingId: null,
    editingName: '',
    confirmDeleteInstId: null,
    currentInstHasDriveCreds: false,
    currentInstHasSihosPassword: false,
    driveCredsStatus: '',
    driveCredsError: '',
    form: { name:'', display_name:'', nit:'', invoice_id_prefix:'', sihos_base_url:'', sihos_doc_code:'', sihos_user:'', sihos_password:'' },

    // Normalización (global)
    normSubTab: 'admins',
    agreements: [],
    contractTypes: [],
    normPendingOnly: false,
    agreementsSearch: '',
    savingAgreementId: null,

    // Mapeos
    mapSubTab: 'services',
    copySourceId: '', copyConfirm: false, copyBusy: false,
    allServices: [], serviceTypeDocs: [],
    editingServiceId: null, editingServiceTypeId: '',
    confirmDeleteServiceId: null,
    newService: { raw_service:'', service_type_id:'' },
    stdPending: {},

    // Períodos
    periods: [],
    confirmDeletePeriodId: null,
    diskUsage: {},          // { [period_id]: { total_bytes, drive_bytes, stage_bytes, audit_bytes, exists, loading } }
    purgingPeriodId: null,
    newPeriod: { period_label:'', date_from:'', date_to:'' },

    // Reglas
    serviceTypes: [], docTypes: [], folderStatuses: [],
    confirmDeleteSTId: null, confirmDeleteDTId: null, confirmDeleteFSId: null,
    editingSTId: null, editingST: { code:'', display_name:'', priority:10 },
    editingDTId: null, editingDT: { code:'', description:'', prefix:'' },
    editingFSId: null, editingFSVal: '',
    newST: { code:'', display_name:'', priority:10 },
    newDT: { code:'', description:'', prefix:'' },
    newFS: { status:'' },

    // Prefijos (now part of Reglas tab)
    prefixCorrections: [],
    confirmDeletePCId: null,
    editingPCId: null, editingPC: { correct_prefix:'', notes:'' },
    newPC: { wrong_prefix:'', correct_prefix:'', notes:'' },

    // GET that reports failures as a toast and falls back to `fallback` instead of breaking the page.
    async _get(url, fallback) {
      const res = await api.get(url);
      if (!res.ok) { this.showToast(res.error, 'error'); return fallback; }
      return res.data;
    },

    async init() {
      [this.institutions, this.serviceTypes, this.docTypes, this.folderStatuses, this.prefixCorrections] =
        await Promise.all([
          this._get('/api/institutions', []),
          this._get('/api/settings/service-types', []),
          this._get('/api/settings/doc-types', []),
          this._get('/api/settings/folder-statuses', []),
          this._get('/api/settings/prefix-corrections', []),
        ]);

      // Auto-load sidebar institution when entering this page
      if (this.tab === 'instituciones') this._autoLoadCtxInstitution();
      if (this.tab === 'normalizacion') this.loadNormalization();
      if (this.tab === 'mapeos') this.loadMappings();

      // Reload tab data when the sidebar institution changes
      this.$watch(() => this.$store.ctx.institutionId, (newId) => {
        if (this.tab === 'instituciones') this._autoLoadCtxInstitution();
        if (this.tab === 'mapeos') this.loadMappings();
        if (!newId) return;
        if (this.tab === 'periodos') this.loadPeriods();
      });
    },

    // Computed: institución actualmente en edición (para logo y otros datos vivos)
    get editingInstitution() {
      return this.institutions.find(i => i.id === this.editingId) || null;
    },

    // Carga automáticamente la institución del sidebar para editar
    _autoLoadCtxInstitution() {
      const ctxId = this.$store.ctx.institutionId;
      if (!ctxId) return;
      const found = this.institutions.find(i => i.id === ctxId);
      if (found && this.editingId !== found.id) this.editInstitution(found);
    },

    showToast(msg, type = 'success') {
      this.$store.toast.show(msg, type);
    },

    switchTab(t) {
      this.tab = t;
      const instId = this.$store.ctx.institutionId;
      if (t === 'instituciones') this._autoLoadCtxInstitution();
      if (t === 'normalizacion') this.loadNormalization();
      if (t === 'mapeos') this.loadMappings();
      if (t === 'periodos' && instId) this.loadPeriods();
    },

    // ---- HELPERS ----
    stName(id) {
      if (id === null || id === undefined || id === '') return 'Sin mapear';
      const st = this.serviceTypes.find(s => s.id === id || s.id === parseInt(id));
      return st ? st.display_name : `#${id}`;
    },
    dtName(id) {
      const dt = this.docTypes.find(d => d.id === id || d.id === parseInt(id));
      return dt ? `${dt.code} — ${dt.description}` : `#${id}`;
    },
    dtCode(id) {
      const dt = this.docTypes.find(d => d.id === id || d.id === parseInt(id));
      return dt ? dt.code : `#${id}`;
    },

    // ---- INSTITUCIONES ----
    resetInstForm() {
      this.editingId = null; this.editingName = '';
      this.currentInstHasDriveCreds = false;
      this.currentInstHasSihosPassword = false;
      this.driveCredsStatus = ''; this.driveCredsError = '';
      this.form = { name:'', display_name:'', nit:'', invoice_id_prefix:'', sihos_base_url:'', sihos_doc_code:'', sihos_user:'', sihos_password:'' };
    },

    editInstitution(h) {
      this.editingId = h.id;
      this.editingName = h.display_name;
      this.currentInstHasDriveCreds = h.has_drive_credentials || false;
      this.currentInstHasSihosPassword = h.has_sihos_password || false;
      this.driveCredsStatus = ''; this.driveCredsError = '';
      this.form = { name:h.name, display_name:h.display_name, nit:h.nit, invoice_id_prefix:h.invoice_id_prefix, sihos_base_url:h.sihos_base_url||'', sihos_doc_code:h.sihos_doc_code||'', sihos_user:h.sihos_user||'', sihos_password:'' };
      window.scrollTo({ top: 0, behavior: 'smooth' });
    },

    async saveInstitution() {
      const wasEditing = this.editingId;
      const method = wasEditing ? 'PUT' : 'POST';
      const url = wasEditing ? `/api/institutions/${wasEditing}` : '/api/institutions';
      const body = Object.fromEntries(Object.entries(this.form).filter(([, v]) => v !== ''));
      const res = await api.request(method, url, body);
      if (res.ok) {
        this.institutions = await this._get('/api/institutions', this.institutions);
        if (wasEditing) {
          // Mantener formulario abierto con datos frescos del servidor
          const updated = this.institutions.find(i => i.id === wasEditing);
          if (updated) {
            this.editInstitution(updated);
            // Sincronizar sidebar si es la institución activa
            if (Alpine.store('ctx').institutionId === updated.id) {
              Alpine.store('ctx').institution = updated;
              Alpine.store('ctx')._save();
            }
          }
          this.showToast('Institución actualizada.');
        } else {
          this.showToast('Institución creada.');
          this.resetInstForm();
        }
      } else {
        this.showToast(res.error, 'error');
      }
    },

    async deleteInstitution(id) {
      const res = await api.delete(`/api/institutions/${id}`);
      if (res.ok) {
        this.institutions = this.institutions.filter(h => h.id !== id);
        this.confirmDeleteInstId = null;
        this.showToast('Institución eliminada.');
      } else {
        this.confirmDeleteInstId = null;
        this.showToast(res.error, 'error');
      }
    },

    async uploadLogo(id, file) {
      if (!file) return;
      const fd = new FormData();
      fd.append('file', file);
      const res = await api.post(`/api/institutions/${id}/logo`, fd);
      if (res.ok) {
        const { logo_url } = res.data;
        const inst = this.institutions.find(i => i.id === id);
        if (inst) inst.logo_url = logo_url + '?t=' + Date.now();
        this.showToast('Logo actualizado', 'success');
      } else {
        this.showToast(res.error, 'error');
      }
    },

    async uploadDriveCredentials(id, file) {
      if (!file || !id) {
        this.driveCredsStatus = 'error';
        this.driveCredsError = id ? 'Sin archivo seleccionado.' : 'Guarda la institución primero.';
        return;
      }
      this.driveCredsStatus = '';
      const fd = new FormData();
      fd.append('file', file);
      const res = await api.post(`/api/institutions/${id}/drive-credentials`, fd);
      if (res.ok) {
        this.driveCredsStatus = 'ok';
        this.currentInstHasDriveCreds = true;
        const inst = this.institutions.find(i => i.id === id);
        if (inst) inst.has_drive_credentials = true;
        this.showToast('Credenciales de Drive guardadas y cifradas.', 'success');
      } else {
        this.driveCredsStatus = 'error';
        this.driveCredsError = res.error;
        this.showToast(this.driveCredsError, 'error');
      }
    },

    // ---- NORMALIZACIÓN ----
    async loadNormalization() {
      [this.agreements, this.contractTypes] = await Promise.all([
        this._get('/api/institutions/agreements', []),
        this._get('/api/institutions/contract-types', []),
      ]);
    },

    get filteredAgreements() {
      let list = this.agreements;
      if (this.normPendingOnly) list = list.filter(a => !a.contract_type_id);
      if (this.agreementsSearch.trim()) {
        const q = this.agreementsSearch.trim().toLowerCase();
        list = list.filter(a => {
          const admin = (a.administrator?.canonical_name || a.administrator?.raw_name || '').toLowerCase();
          const contract = (a.contract?.canonical_name || a.contract?.raw_name || '').toLowerCase();
          return admin.includes(q) || contract.includes(q);
        });
      }
      return list;
    },

    get groupedAgreements() {
      const list = this.filteredAgreements;

      // Nivel 1: agrupar por canonical_admin normalizado
      const adminGroups = {};
      for (const ag of list) {
        const rawAdmin = (ag.administrator?.canonical_name || ag.administrator?.raw_name || '—').trim();
        const adminKey = rawAdmin.toUpperCase();
        if (!adminGroups[adminKey]) adminGroups[adminKey] = { label: rawAdmin, agreements: [] };
        adminGroups[adminKey].agreements.push(ag);
      }

      // Nivel 2: dentro de cada admin, deduplicar por canonical_contract
      const result = [];
      for (const adminGroup of Object.values(adminGroups)) {
        const contractGroups = {};
        for (const ag of adminGroup.agreements) {
          const rawContract = (ag.contract?.canonical_name || ag.contract?.raw_name || '—').trim();
          const contractKey = rawContract.toUpperCase();
          if (!contractGroups[contractKey]) {
            contractGroups[contractKey] = { label: rawContract, primaryAg: ag, allIds: [] };
          }
          contractGroups[contractKey].allIds.push(ag.id);
          // Preferir como primario el que ya tenga contract_type asignado
          if (ag.contract_type_id && !contractGroups[contractKey].primaryAg.contract_type_id) {
            contractGroups[contractKey].primaryAg = ag;
          }
        }
        const items = Object.values(contractGroups).sort((a, b) => a.label.localeCompare(b.label));
        result.push({ label: adminGroup.label, items });
      }
      return result.sort((a, b) => a.label.localeCompare(b.label));
    },

    async setAgreementType(allIds, contractTypeId) {
      const typeVal = contractTypeId ? parseInt(contractTypeId) : null;
      this.savingAgreementId = allIds[0];
      let ok = true, lastError = '';
      for (const id of allIds) {
        const res = await api.patch(`/api/institutions/agreements/${id}`, { contract_type_id: typeVal });
        if (res.ok) {
          const idx = this.agreements.findIndex(a => a.id === id);
          if (idx >= 0) this.agreements[idx].contract_type_id = typeVal;
        } else { ok = false; lastError = res.error; }
      }
      this.savingAgreementId = null;
      this.showToast(ok ? 'Tipo de convenio actualizado.' : lastError, ok ? 'success' : 'error');
    },

    async consolidateAgreements() {
      const res = await api.post('/api/institutions/agreements/consolidate');
      if (res.ok) {
        const data = res.data;
        const msg = data.agreements_deleted > 0
          ? `Consolidado: ${data.agreements_deleted} convenio(s) eliminado(s), ${data.invoices_redirected} factura(s) redirigida(s).`
          : 'Sin duplicados que consolidar.';
        this.showToast(msg);
        await this.loadNormalization();
      } else {
        this.showToast(res.error, 'error');
      }
    },

    ctName(id) {
      if (!id) return null;
      const ct = this.contractTypes.find(c => c.id === id || c.id === parseInt(id));
      return ct ? ct.name : `#${id}`;
    },

    // ---- MAPEOS ----
    async loadMappings() {
      const id = this.$store.ctx.institutionId;
      if (id) {
        [this.allServices, this.serviceTypeDocs] = await Promise.all([
          this._get(`/api/institutions/${id}/services`, []),
          this._get(`/api/institutions/${id}/service-type-documents`, []),
        ]);
      }
    },

    async copyMappings() {
      const id = this.$store.ctx.institutionId;
      if (!id || !this.copySourceId) return;
      this.copyBusy = true;
      try {
        const res = await api.post(`/api/institutions/${id}/copy-mappings`, {
          source_institution_id: parseInt(this.copySourceId),
        });
        if (res.ok) {
          const r = res.data;
          this.copySourceId = ''; this.copyConfirm = false;
          await this.loadMappings();
          this.showToast(`Copiado: ${r.services} servicios y ${r.documents} documentos.`);
        } else {
          this.showToast(res.error, 'error');
        }
      } finally { this.copyBusy = false; }
    },

    // Services
    startEditService(s) { this.editingServiceId = s.id; this.editingServiceTypeId = s.service_type_id; },
    async saveServiceEdit(id) {
      const res = await api.patch(`/api/institutions/services/${id}`, { service_type_id: parseInt(this.editingServiceTypeId) });
      if (res.ok) { this.editingServiceId = null; await this.loadMappings(); this.showToast('Actualizado.'); }
      else this.showToast(res.error, 'error');
    },
    async addService() {
      const id = this.$store.ctx.institutionId;
      if (!id || !this.newService.raw_service.trim() || !this.newService.service_type_id) return;
      const res = await api.post(`/api/institutions/${id}/services`, { raw_service: this.newService.raw_service, service_type_id: parseInt(this.newService.service_type_id) });
      if (res.ok) { this.newService = {raw_service:'',service_type_id:''}; await this.loadMappings(); this.showToast('Servicio agregado.'); }
      else this.showToast(res.error, 'error');
    },
    async deleteService(id) {
      const res = await api.delete(`/api/institutions/services/${id}`);
      if (res.ok) { this.confirmDeleteServiceId = null; await this.loadMappings(); this.showToast('Eliminado.'); }
      else this.showToast(res.error, 'error');
    },

    // ServiceTypeDocs — matrix helpers
    stdHas(stId, dtId) {
      return this.serviceTypeDocs.some(d => d.service_type_id === stId && d.doc_type_id === dtId);
    },
    async toggleSTD(stId, dtId, checked) {
      const instId = this.$store.ctx.institutionId;
      const key = `${stId}-${dtId}`;
      this.stdPending = { ...this.stdPending, [key]: true };
      if (checked) {
        const res = await api.post(`/api/institutions/${instId}/service-type-documents`, { institution_id: parseInt(instId), service_type_id: stId, doc_type_id: dtId });
        if (res.ok) {
          this.serviceTypeDocs.push(res.data);
        } else this.showToast(res.error, 'error');
      } else {
        const res = await api.delete(`/api/institutions/${instId}/service-type-documents/${stId}/${dtId}`);
        if (res.ok) {
          this.serviceTypeDocs = this.serviceTypeDocs.filter(d => !(d.service_type_id === stId && d.doc_type_id === dtId));
        } else this.showToast(res.error, 'error');
      }
      this.stdPending = { ...this.stdPending, [key]: false };
    },

    // ---- PERÍODOS ----
    async loadPeriods() {
      const id = this.$store.ctx.institutionId;
      if (!id) return;
      this.periods = await this._get(`/api/institutions/${id}/periods`, []);
    },
    async addPeriod() {
      const id = this.$store.ctx.institutionId;
      if (!id || !this.newPeriod.period_label.trim() || !this.newPeriod.date_from || !this.newPeriod.date_to) return;
      const res = await api.post(`/api/institutions/${id}/periods`, { institution_id: parseInt(id), period_label: this.newPeriod.period_label, date_from: this.newPeriod.date_from, date_to: this.newPeriod.date_to });
      if (res.ok) { this.newPeriod = {period_label:'',date_from:'',date_to:''}; await this.loadPeriods(); this.showToast('Período creado.'); }
      else this.showToast(res.error, 'error');
    },
    async loadDiskUsage(periodId) {
      if (this.diskUsage[periodId]) return;
      this.diskUsage[periodId] = { loading: true };
      const res = await api.get(`/api/institutions/periods/${periodId}/disk-usage`);
      this.diskUsage[periodId] = res.ok ? { ...res.data, loading: false } : { loading: false, error: true };
    },
    formatBytes(bytes) {
      if (!bytes) return '0 B';
      const units = ['B','KB','MB','GB'];
      let i = 0, v = bytes;
      while (v >= 1024 && i < units.length - 1) { v /= 1024; i++; }
      return `${v.toFixed(i > 0 ? 1 : 0)} ${units[i]}`;
    },
    async confirmDeletePeriod(id) {
      this.confirmDeletePeriodId = id;
      await this.loadDiskUsage(id);
    },
    async deletePeriod(id) {
      const res = await api.delete(`/api/institutions/periods/${id}`);
      if (res.ok) {
        this.confirmDeletePeriodId = null;
        delete this.diskUsage[id];
        await this.loadPeriods();
        this.showToast('Período y archivos eliminados.');
      } else this.showToast(res.error, 'error');
    },
    async purgeDisk(id) {
      this.purgingPeriodId = id;
      const res = await api.post(`/api/institutions/periods/${id}/purge-disk`);
      this.purgingPeriodId = null;
      if (res.ok) {
        delete this.diskUsage[id];
        this.showToast('Archivos del período eliminados del disco.');
      } else this.showToast(res.error, 'error');
    },

    // ---- REGLAS ----
    startEditST(st) { this.editingSTId = st.id; this.editingST = { code: st.code, display_name: st.display_name, priority: st.priority }; },
    async saveSTEdit(id) {
      const res = await api.patch(`/api/settings/service-types/${id}`, { code: this.editingST.code, display_name: this.editingST.display_name, priority: parseInt(this.editingST.priority) });
      if (res.ok) {
        const updated = res.data;
        this.serviceTypes = this.serviceTypes.map(s => s.id === id ? updated : s);
        this.editingSTId = null;
        this.showToast('Tipo de servicio actualizado.');
      } else this.showToast(res.error, 'error');
    },
    startEditDT(dt) { this.editingDTId = dt.id; this.editingDT = { code: dt.code, description: dt.description, prefix: dt.prefix || '' }; },
    async saveDTEdit(id) {
      const body = { code: this.editingDT.code, description: this.editingDT.description };
      if (this.editingDT.prefix.trim()) body.prefix = this.editingDT.prefix.trim();
      const res = await api.patch(`/api/settings/doc-types/${id}`, body);
      if (res.ok) {
        const updated = res.data;
        this.docTypes = this.docTypes.map(d => d.id === id ? updated : d);
        this.editingDTId = null;
        this.showToast('Tipo de documento actualizado.');
      } else this.showToast(res.error, 'error');
    },
    async addServiceType() {
      if (!this.newST.code.trim() || !this.newST.display_name.trim()) return;
      const res = await api.post('/api/settings/service-types', { code: this.newST.code, display_name: this.newST.display_name, priority: parseInt(this.newST.priority) || 10 });
      if (res.ok) { this.serviceTypes = await this._get('/api/settings/service-types', this.serviceTypes); this.newST = {code:'',display_name:'',priority:10}; this.showToast('Tipo de servicio creado.'); }
      else this.showToast(res.error, 'error');
    },
    async deleteServiceType(id) {
      const res = await api.delete(`/api/settings/service-types/${id}`);
      if (res.ok) { this.serviceTypes = this.serviceTypes.filter(s => s.id !== id); this.confirmDeleteSTId = null; this.showToast('Eliminado.'); }
      else { this.confirmDeleteSTId = null; this.showToast(res.error, 'error'); }
    },
    async addDocType() {
      if (!this.newDT.code.trim() || !this.newDT.description.trim()) return;
      const body = { code: this.newDT.code, description: this.newDT.description };
      if (this.newDT.prefix.trim()) body.prefix = this.newDT.prefix.trim();
      const res = await api.post('/api/settings/doc-types', body);
      if (res.ok) { this.docTypes = await this._get('/api/settings/doc-types', this.docTypes); this.newDT = {code:'',description:'',prefix:''}; this.showToast('Tipo de documento creado.'); }
      else this.showToast(res.error, 'error');
    },
    async deleteDocType(id) {
      const res = await api.delete(`/api/settings/doc-types/${id}`);
      if (res.ok) { this.docTypes = this.docTypes.filter(d => d.id !== id); this.confirmDeleteDTId = null; this.showToast('Eliminado.'); }
      else { this.confirmDeleteDTId = null; this.showToast(res.error, 'error'); }
    },
    startEditFS(fs) { this.editingFSId = fs.id; this.editingFSVal = fs.status; },
    async saveFSEdit(id) {
      const res = await api.patch(`/api/settings/folder-statuses/${id}`, { status: this.editingFSVal });
      if (res.ok) { this.folderStatuses = await this._get('/api/settings/folder-statuses', this.folderStatuses); this.editingFSId = null; this.showToast('Estado actualizado.'); }
      else this.showToast(res.error, 'error');
    },
    async addFolderStatus() {
      if (!this.newFS.status.trim()) return;
      const res = await api.post('/api/settings/folder-statuses', { status: this.newFS.status });
      if (res.ok) { this.folderStatuses = await this._get('/api/settings/folder-statuses', this.folderStatuses); this.newFS = {status:''}; this.showToast('Estado creado.'); }
      else this.showToast(res.error, 'error');
    },
    async deleteFolderStatus(id) {
      const res = await api.delete(`/api/settings/folder-statuses/${id}`);
      if (res.ok) { this.folderStatuses = this.folderStatuses.filter(f => f.id !== id); this.confirmDeleteFSId = null; this.showToast('Eliminado.'); }
      else { this.confirmDeleteFSId = null; this.showToast(res.error, 'error'); }
    },

    // ---- PREFIJOS ----
    startEditPC(pc) {
      this.editingPCId = pc.id;
      this.editingPC = { correct_prefix: pc.correct_prefix, notes: pc.notes || '' };
    },
    async savePCEdit(id) {
      const body = { correct_prefix: this.editingPC.correct_prefix };
      if (this.editingPC.notes !== undefined) body.notes = this.editingPC.notes || null;
      const res = await api.patch(`/api/settings/prefix-corrections/${id}`, body);
      if (res.ok) {
        const updated = res.data;
        this.prefixCorrections = this.prefixCorrections.map(p => p.id === id ? updated : p);
        this.editingPCId = null;
        this.showToast('Corrección actualizada.');
      } else this.showToast(res.error, 'error');
    },
    async addPrefixCorrection() {
      if (!this.newPC.wrong_prefix.trim() || !this.newPC.correct_prefix.trim()) return;
      const body = { wrong_prefix: this.newPC.wrong_prefix, correct_prefix: this.newPC.correct_prefix };
      if (this.newPC.notes.trim()) body.notes = this.newPC.notes;
      const res = await api.post('/api/settings/prefix-corrections', body);
      if (res.ok) {
        this.prefixCorrections = await this._get('/api/settings/prefix-corrections', this.prefixCorrections);
        this.newPC = { wrong_prefix:'', correct_prefix:'', notes:'' };
        this.showToast('Corrección agregada.');
      } else this.showToast(res.error, 'error');
    },
    async deletePrefixCorrection(id) {
      const res = await api.delete(`/api/settings/prefix-corrections/${id}`);
      if (res.ok) {
        this.prefixCorrections = this.prefixCorrections.filter(p => p.id !== id);
        this.confirmDeletePCId = null;
        this.showToast('Corrección eliminada.');
      } else { this.confirmDeletePCId = null; this.showToast(res.error, 'error'); }
    },

  };
}
