function auditApp() {
  return {
    // --- Reference data ---
    folderStatuses: [],
    docTypes: [],
    admins: [],
    contracts: [],
    contractTypes: [],
    agreements: [],
    serviceTypes: [],

    // --- Filters ---
    filtersOpen: false,
    filterStatusId: '',
    filterAdminType: '',
    filterAdminCanonical: '',
    filterContractCanonical: '',
    filterServiceTypeId: '',
    searchQuery: '',

    // --- Table state ---
    invoices: [],
    total: 0,
    page: 1,
    pageSize: 50,

    // --- Stats ---
    stats: { total: null, by_status: {}, total_findings: null },
    findingsSummary: [],
    filterFindingDocTypeId: null,

    // --- Selection ---
    selectedIds: new Set(),
    allFilterSelected: false,   // true when "select all N" was clicked
    batchStatusId: '',
    confirmBatchDelete: false,
    confirmBatchDeleteInvoices: false,

    // --- Detail panel ---
    selectedInvoice: null,
    detailStatusId: '',
    missingFiles: [],
    newDocTypeId: '',
    confirmDeleteInvoice: false,

    // ── Computed ──────────────────────────────────────
    get hasActiveFilters() {
      return !!(this.filterStatusId || this.filterAdminType || this.filterAdminCanonical ||
                this.filterContractCanonical || this.filterServiceTypeId || this.searchQuery ||
                this.filterFindingDocTypeId);
    },
    get pageAllSelected() {
      return this.invoices.length > 0 && this.invoices.every(i => this.selectedIds.has(i.id));
    },
    get somethingSelected() {
      return this.invoices.some(i => this.selectedIds.has(i.id));
    },
    get statCards() {
      const cfg = {
        PENDIENTE: { activeBg:'bg-amber-50',   activeBorder:'border-amber-300',  activeRing:'ring-amber-300',  activeLabel:'text-amber-600', activeCount:'text-amber-700', activeSub:'text-amber-500',  rawColor:'#b45309', rawBg:'#fffbeb' },
        PRESENTE:  { activeBg:'bg-emerald-50', activeBorder:'border-emerald-300',activeRing:'ring-emerald-300',activeLabel:'text-emerald-600',activeCount:'text-emerald-700',activeSub:'text-emerald-500',rawColor:'#059669', rawBg:'#ecfdf5' },
        FALTANTE:  { activeBg:'bg-orange-50',  activeBorder:'border-orange-300', activeRing:'ring-orange-300', activeLabel:'text-orange-600',activeCount:'text-orange-700',activeSub:'text-orange-500', rawColor:'#ea580c', rawBg:'#fff7ed' },
        AUDITADA:  { activeBg:'bg-indigo-50',  activeBorder:'border-indigo-300', activeRing:'ring-indigo-300', activeLabel:'text-indigo-600',activeCount:'text-indigo-700',activeSub:'text-indigo-500', rawColor:'#4338ca', rawBg:'#eef2ff' },
        ANULADA:   { activeBg:'bg-red-50',     activeBorder:'border-red-300',    activeRing:'ring-red-300',    activeLabel:'text-red-600',   activeCount:'text-red-700',   activeSub:'text-red-500',    rawColor:'#dc2626', rawBg:'#fef2f2' },
      };
      return this.folderStatuses.map(fs => ({
        ...fs,
        ...(cfg[fs.status] || { activeBg:'bg-sky-50', activeBorder:'border-sky-300', activeRing:'ring-sky-300', activeLabel:'text-sky-600', activeCount:'text-sky-700', activeSub:'text-sky-500', rawColor:'#0284c7', rawBg:'#f0f9ff' }),
      }));
    },
    get uniqueAdminTypes() {
      const seen = new Set();
      return this.contractTypes
        .map(ct => ct.name)
        .filter(n => n && !seen.has(n) && seen.add(n))
        .sort();
    },
    get filteredAdmins() {
      const seen = new Set();
      return this.admins
        .map(a => a.canonical_name)
        .filter(n => n && !seen.has(n) && seen.add(n))
        .sort();
    },
    get uniqueContracts() {
      let contractIds = null;
      if (this.filterAdminCanonical) {
        const admin = this.admins.find(a => a.canonical_name === this.filterAdminCanonical);
        if (admin) {
          contractIds = new Set(
            this.agreements
              .filter(ag => ag.administrator_id === admin.id)
              .map(ag => ag.contract_id)
          );
        }
      }
      const seen = new Set();
      return this.contracts
        .filter(c => c.canonical_name && (!contractIds || contractIds.has(c.id)))
        .map(c => c.canonical_name)
        .filter(n => !seen.has(n) && seen.add(n))
        .sort();
    },

    // ── API helpers: errors always surface as a toast, never as a half-parsed response ──
    async _get(url, fallback) {
      const res = await api.get(url);
      if (!res.ok) { Alpine.store('toast').error(res.error); return fallback; }
      return res.data;
    },
    // Awaits a mutation (api.post/patch/delete promise); toasts on failure. Returns true when it succeeded.
    async _send(call) {
      const res = await call;
      if (!res.ok) Alpine.store('toast').error(res.error);
      return res.ok;
    },

    // ── Init ──────────────────────────────────────────
    async init() {
      [this.folderStatuses, this.docTypes, this.serviceTypes] = await Promise.all([
        this._get('/api/settings/folder-statuses', []),
        this._get('/api/settings/doc-types', []),
        this._get('/api/settings/service-types', []),
      ]);

      if (this.$store.ctx.institutionId) await this.loadFilterOptions();
      if (this.$store.ctx.periodId) { this.loadInvoices(); this.loadStats(); this.loadFindingsSummary(); }

      this.$watch('$store.ctx.periodId', val => {
        this.invoices = []; this.total = 0; this.selectedInvoice = null; this.page = 1;
        this.stats = { total: null, by_status: {}, total_findings: null };
        this.findingsSummary = []; this.filterFindingDocTypeId = null;
        this.clearSelection();
        if (val) { this.loadInvoices(); this.loadStats(); this.loadFindingsSummary(); }
      });
      this.$watch('$store.ctx.institutionId', async val => {
        this.admins = []; this.contracts = []; this.contractTypes = []; this.agreements = [];
        this.filterAdminType = ''; this.filterAdminCanonical = ''; this.filterContractCanonical = '';
        if (val) await this.loadFilterOptions();
      });
    },

    // ── Data loaders ──────────────────────────────────
    async loadFilterOptions() {
      const id = this.$store.ctx.institutionId;
      if (!id) return;
      [this.admins, this.contracts, this.contractTypes, this.agreements] = await Promise.all([
        this._get('/api/institutions/administrators', []),
        this._get('/api/institutions/contracts', []),
        this._get('/api/institutions/contract-types', []),
        this._get('/api/institutions/agreements', []),
      ]);
    },

    async loadStats() {
      const pid = this.$store.ctx.periodId;
      if (!pid) return;
      const stats = await this._get(`/api/invoices/stats?period_id=${pid}`, null);
      if (stats) this.stats = stats;
    },

    async loadFindingsSummary() {
      const pid = this.$store.ctx.periodId;
      if (!pid) return;
      this.findingsSummary = await this._get(`/api/invoices/findings-summary?period_id=${pid}`, []);
    },

    toggleFindingFilter(docTypeId) {
      this.filterFindingDocTypeId = this.filterFindingDocTypeId === docTypeId ? null : docTypeId;
      this.page = 1;
      this.loadInvoices();
    },

    async loadInvoices() {
      const pid = this.$store.ctx.periodId;
      if (!pid) return;
      let url = `/api/invoices?period_id=${pid}&page=${this.page}&page_size=${this.pageSize}`;
      if (this.filterStatusId)          url += `&folder_status_id=${this.filterStatusId}`;
      if (this.filterAdminType)         url += `&admin_type=${encodeURIComponent(this.filterAdminType)}`;
      if (this.filterAdminCanonical)    url += `&admin_canonical=${encodeURIComponent(this.filterAdminCanonical)}`;
      if (this.filterContractCanonical) url += `&contract_canonical=${encodeURIComponent(this.filterContractCanonical)}`;
      if (this.filterServiceTypeId)       url += `&service_type_id=${this.filterServiceTypeId}`;
      if (this.searchQuery)               url += `&search=${encodeURIComponent(this.searchQuery)}`;
      if (this.filterFindingDocTypeId)    url += `&has_finding_doc_type_id=${this.filterFindingDocTypeId}`;
      const data = await this._get(url, null);
      if (!data) return;
      this.invoices = data.items;
      this.total    = data.total;
      // If allFilterSelected, check if newly loaded page is still in set
      // (it would be if user navigated pages while all-filter is active)
    },

    clearFilters() {
      this.filterStatusId = ''; this.filterAdminType = ''; this.filterAdminCanonical = '';
      this.filterContractCanonical = ''; this.filterServiceTypeId = ''; this.searchQuery = '';
      this.filterFindingDocTypeId = null;
      this.page = 1;
      this.allFilterSelected = false;
      this.loadInvoices();
    },
    prevPage() { if (this.page > 1) { this.page--; this.loadInvoices(); } },
    nextPage() { if (this.page * this.pageSize < this.total) { this.page++; this.loadInvoices(); } },

    // ── Selection ─────────────────────────────────────
    toggleSelection(id) {
      const next = new Set(this.selectedIds);
      if (next.has(id)) { next.delete(id); this.allFilterSelected = false; }
      else next.add(id);
      this.selectedIds = next;
      this.confirmBatchDelete = false;
    },

    togglePageSelection(event) {
      const next = new Set(this.selectedIds);
      if (event.target.checked) {
        this.invoices.forEach(i => next.add(i.id));
      } else {
        this.invoices.forEach(i => next.delete(i.id));
        this.allFilterSelected = false;
      }
      this.selectedIds = next;
      this.confirmBatchDelete = false;
    },

    async selectAllFilter() {
      // Fetch all IDs matching current filter
      const pid = this.$store.ctx.periodId;
      if (!pid) return;
      let url = `/api/invoices/ids?period_id=${pid}`;
      if (this.filterStatusId)          url += `&folder_status_id=${this.filterStatusId}`;
      if (this.filterAdminType)         url += `&admin_type=${encodeURIComponent(this.filterAdminType)}`;
      if (this.filterAdminCanonical)    url += `&admin_canonical=${encodeURIComponent(this.filterAdminCanonical)}`;
      if (this.filterContractCanonical) url += `&contract_canonical=${encodeURIComponent(this.filterContractCanonical)}`;
      if (this.filterServiceTypeId)       url += `&service_type_id=${this.filterServiceTypeId}`;
      if (this.searchQuery)               url += `&search=${encodeURIComponent(this.searchQuery)}`;
      if (this.filterFindingDocTypeId)    url += `&has_finding_doc_type_id=${this.filterFindingDocTypeId}`;
      const ids = await this._get(url, null);
      if (!ids) return;
      this.selectedIds = new Set(ids);
      this.allFilterSelected = true;
    },

    clearSelection() {
      this.selectedIds = new Set();
      this.batchStatusId = '';
      this.confirmBatchDelete = false;
      this.confirmBatchDeleteInvoices = false;
      this.allFilterSelected = false;
    },

    // ── Batch operations ──────────────────────────────
    async applyBatchStatus() {
      if (!this.batchStatusId || this.selectedIds.size === 0) return;
      await this._send(api.post('/api/invoices/batch-status', {
        invoice_ids: [...this.selectedIds], folder_status_id: parseInt(this.batchStatusId),
      }));
      this.batchStatusId = '';
      this.clearSelection();
      await Promise.all([this.loadInvoices(), this.loadStats(), this.loadFindingsSummary()]);
    },

    async doBatchDeleteInvoices() {
      if (this.selectedIds.size === 0) return;
      await this._send(api.post('/api/invoices/batch-delete', { invoice_ids: [...this.selectedIds] }));
      if (this.selectedInvoice && this.selectedIds.has(this.selectedInvoice.id)) {
        this.selectedInvoice = null;
        this.missingFiles = [];
      }
      this.clearSelection();
      await Promise.all([this.loadInvoices(), this.loadStats(), this.loadFindingsSummary()]);
    },

    async doBatchDeleteFindings() {
      if (this.selectedIds.size === 0) return;
      await this._send(api.post('/api/missing-files/batch-delete', { invoice_ids: [...this.selectedIds] }));
      this.clearSelection();
      await Promise.all([this.loadInvoices(), this.loadStats(), this.loadFindingsSummary()]);
      if (this.selectedInvoice) {
        this.missingFiles = await this._get(`/api/missing-files/${this.selectedInvoice.id}`, []);
      }
    },

    // ── Detail panel ──────────────────────────────────
    async selectInvoice(inv) {
      this.selectedInvoice = { ...inv };
      this.detailStatusId  = inv.folder_status_id;
      this.newDocTypeId    = '';
      this.confirmDeleteInvoice = false;
      this.missingFiles = await this._get(`/api/missing-files/${inv.id}`, []);
    },

    async updateStatus() {
      if (!this.selectedInvoice || !this.detailStatusId) return;
      await this._send(api.patch(`/api/invoices/${this.selectedInvoice.id}/status`, {
        folder_status_id: parseInt(this.detailStatusId),
      }));
      await Promise.all([this.loadInvoices(), this.loadStats(), this.loadFindingsSummary()]);
    },

    async addFile() {
      if (!this.newDocTypeId || !this.selectedInvoice) return;
      await this._send(api.post('/api/missing-files', {
        invoice_id: this.selectedInvoice.id, doc_type_id: parseInt(this.newDocTypeId), expected_path: '',
      }));
      this.newDocTypeId = '';
      await this.selectInvoice(this.selectedInvoice);
      await Promise.all([this.loadInvoices(), this.loadStats(), this.loadFindingsSummary()]);
    },

    async deleteFile(docTypeId) {
      if (!this.selectedInvoice) return;
      await this._send(api.delete(`/api/missing-files/${this.selectedInvoice.id}/${docTypeId}`));
      await this.selectInvoice(this.selectedInvoice);
      await Promise.all([this.loadInvoices(), this.loadStats(), this.loadFindingsSummary()]);
    },

    async deleteInvoice() {
      if (!this.selectedInvoice) return;
      if (await this._send(api.delete(`/api/invoices/${this.selectedInvoice.id}`))) {
        this.selectedInvoice = null;
        this.confirmDeleteInvoice = false;
        await Promise.all([this.loadInvoices(), this.loadStats(), this.loadFindingsSummary()]);
      }
    },

    // ── Helpers ───────────────────────────────────────
    docTypeLabel(docTypeId) {
      const dt = this.docTypes.find(d => d.id === docTypeId);
      return dt ? dt.code : `Doc #${docTypeId}`;
    },

    statusBadgeClass(status) {
      return {
        PRESENTE:  'bg-emerald-100 text-emerald-700',
        PENDIENTE: 'bg-amber-100   text-amber-700',
        FALTANTE:  'bg-orange-100  text-orange-700',
        AUDITADA:  'bg-indigo-100  text-indigo-700',
        ANULADA:   'bg-red-100     text-red-700',
      }[status] || 'bg-gray-100 text-gray-600';
    },
  };
}
