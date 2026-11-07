document.addEventListener('alpine:init', () => {
  Alpine.data('explorerApp', () => ({
    explorerOpen: false,
    rootFolder: 'STAGE',
    currentPath: '',
    entries: [],
    selectedEntry: null,
    previewUrl: null,
    pdfMode: 'none',
    renamingPath: null,
    renameValue: '',
    renameExt: '',
    selectedPdfs: [],
    mergeOutputName: 'resultado.pdf',
    splitRanges: '',
    splitGroups: [],
    splitPreviewError: '',
    splitPalette: [
      'border-sky-400 bg-sky-50 text-sky-700',
      'border-amber-400 bg-amber-50 text-amber-700',
      'border-teal-400 bg-teal-50 text-teal-700',
      'border-indigo-400 bg-indigo-50 text-indigo-700',
      'border-violet-400 bg-violet-50 text-violet-700',
    ],
    thumbnails: [],
    pageOrder: [],
    reorderDragIdx: null,
    confirmingDelete: null,
    movingEntries: [],
    moveBrowserPath: '',
    moveBrowserRoot: '',
    moveBrowserEntries: [],
    copyingEntries: [],
    destSearchQuery: '',
    destSearchResults: [],
    destSearchLoading: false,
    searchQuery: '',
    searchResults: [],
    searchLoading: false,
    panelWidth: 560,
    listWidth: 220,
    _resizing: false,
    _resizeStartX: 0,
    _resizeStartWidth: 0,
    _listResizing: false,
    // ── Selección en lote ──
    selectedEntries: [],
    batchDeleteConfirm: false,
    // ── Upload ──
    uploadProgress: null,   // null | {done, total, errors}
    isDragOver: false,
    uploadMenuOpen: false,
    // ── Crear carpeta ──
    creatingFolder: false,
    newFolderName: '',

    get institutionId() { return Alpine.store('ctx').institutionId; },
    get periodId()      { return Alpine.store('ctx').periodId; },

    init() {
      // Recargar automáticamente cuando cambia hospital o período estando abierto
      const _resetToStage = () => {
        if (!this.explorerOpen) return;
        this.rootFolder = 'STAGE';
        this.currentPath = 'STAGE';
        this.selectedEntry = null;
        this.previewUrl = null;
        this.selectedEntries = [];
        this.pdfMode = 'none';
        this.load();
      };
      this.$watch(() => Alpine.store('ctx').institutionId, _resetToStage);
      this.$watch(() => Alpine.store('ctx').periodId, _resetToStage);
    },

    // Wrapper sobre api.request que conserva el contrato del explorador: devuelve el JSON de la
    // respuesta, o {ok:false, message} si falla (errores FastAPI, 422 en lista y caída de red).
    async _apiFetch(url, opts = {}) {
      const body = typeof opts.body === 'string' ? JSON.parse(opts.body) : opts.body;  // FormData pasa tal cual
      const res = await api.request(opts.method || 'GET', url, body);
      if (!res.ok) return { ok: false, message: res.error };
      return res.data;
    },

    isSearchActive() {
      return this.searchQuery.trim().length > 0;
    },

    clearSearch() {
      this.searchQuery = '';
      this.searchResults = [];
    },

    async load() {
      if (!this.institutionId || !this.periodId) return;
      const root = this.rootFolder;
      const subPath = this.currentPath.split('/').slice(1).join('/');
      const url = `/api/explorer/list?institution_id=${this.institutionId}&period_id=${this.periodId}&root=${root}&path=${encodeURIComponent(subPath)}`;
      const res = await api.get(url);
      this.entries = res.ok ? (res.data.entries || []) : [];
      // Limpiar selecciones que ya no existen en el listado actual
      if (this.selectedEntries.length) {
        const currentPaths = new Set(this.entries.map(e => e.path));
        this.selectedEntries = this.selectedEntries.filter(p => currentPaths.has(p));
      }
    },

    async selectEntry(entry) {
      this.selectedEntry = entry;
      if (!entry.is_dir) {
        if (entry.is_pdf === false) {
          this.previewUrl = null;  // sin preview: /file solo sirve PDFs
          this.pdfMode = 'none';
          return;
        }
        this.previewUrl = `/api/explorer/file?institution_id=${this.institutionId}&period_id=${this.periodId}&path=${encodeURIComponent(entry.path)}`;
        this.pdfMode = 'none';
      }
    },

    navigateTo(path) {
      this.currentPath = path;
      this.selectedEntry = null;
      this.previewUrl = null;
      this.pdfMode = 'none';
      this.clearSearch();
      this.load();
    },

    switchRoot(tab) {
      this.rootFolder = tab;
      this.currentPath = tab;
      this.selectedEntry = null;
      this.previewUrl = null;
      this.pdfMode = 'none';
      this.entries = [];
      this.clearSearch();
      this.load();
    },

    goUp() {
      const parts = this.currentPath.split('/').filter(Boolean);
      if (parts.length <= 1) {
        this.currentPath = this.rootFolder;
      } else {
        parts.pop();
        this.currentPath = parts.join('/');
      }
      this.selectedEntry = null;
      this.previewUrl = null;
      this.clearSearch();
      this.load();
    },

    startRename(entry) {
      this.renamingPath = entry.path;
      // Archivos: se edita solo el stem; la extensión queda fija
      const dot = entry.name.lastIndexOf('.');
      const hasExt = !entry.is_dir && dot > 0;
      this.renameExt = hasExt ? entry.name.slice(dot) : '';
      this.renameValue = hasExt ? entry.name.slice(0, dot) : entry.name;
      this.$nextTick(() => this.$refs.renameInput && this.$refs.renameInput.focus());
    },

    async commitRename(entry) {
      const newName = this.renameValue.trim() + this.renameExt;
      if (!this.renameValue.trim() || newName === entry.name) {
        this.renamingPath = null;
        return;
      }
      const data = await this._apiFetch('/api/explorer/rename', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          institution_id: this.institutionId,
          period_id: this.periodId,
          path: entry.path,
          new_name: newName,
        }),
      });
      this.renamingPath = null;
      this.showToast(data.ok, data.message);
      if (data.ok) { this.isSearchActive() ? this.runSearch() : this.load(); }
    },

    // Drag-to-move
    onDragStart(event, entry) {
      event.dataTransfer.setData('text/plain', entry.path);
    },

    async onDrop(event, targetFolder) {
      // Archivos externos del SO tienen prioridad sobre drag interno
      if (event.dataTransfer.files && event.dataTransfer.files.length > 0) {
        event.stopPropagation();
        await this.uploadFiles(event.dataTransfer.files, [], targetFolder.path);
        return;
      }
      const srcPath = event.dataTransfer.getData('text/plain');
      if (!srcPath || srcPath === targetFolder.path) return;
      // Prevent dropping a folder into one of its own descendants
      if (targetFolder.path.startsWith(srcPath + '/')) return;
      const data = await this._apiFetch('/api/explorer/move', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          institution_id: this.institutionId,
          period_id: this.periodId,
          src: srcPath,
          dst_folder: targetFolder.path,
        }),
      });
      this.showToast(data.ok, data.message);
      if (data.ok) this.load();
    },

    toggleMergePdf(path) {
      const idx = this.selectedPdfs.indexOf(path);
      if (idx >= 0) this.selectedPdfs.splice(idx, 1);
      else this.selectedPdfs.push(path);
    },

    async executeMerge() {
      if (this.selectedPdfs.length < 2) return;
      const data = await this._apiFetch('/api/explorer/merge', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          institution_id: this.institutionId,
          period_id: this.periodId,
          paths: this.selectedPdfs,
          output_name: this.mergeOutputName || 'resultado.pdf',
        }),
      });
      this.showToast(data.ok, data.message);
      if (data.ok) { this.pdfMode = 'none'; this.selectedPdfs = []; this.load(); }
    },

    async executeSplit() {
      if (!this.selectedEntry) return;
      const data = await this._apiFetch('/api/explorer/split', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          institution_id: this.institutionId,
          period_id: this.periodId,
          path: this.selectedEntry.path,
          ranges: this.splitRanges.trim() || null,
        }),
      });
      this.showToast(data.ok, data.message);
      if (data.ok) { this.pdfMode = 'none'; this.load(); }
    },

    // Carga this.thumbnails para el PDF seleccionado. Devuelve false y resetea
    // pdfMode si falla — usado por startReorder() y startSplit().
    async _loadThumbnails() {
      this.thumbnails = [];
      const url = `/api/explorer/thumbnails?institution_id=${this.institutionId}&period_id=${this.periodId}&path=${encodeURIComponent(this.selectedEntry.path)}`;
      const data = await this._apiFetch(url);
      if (!data.ok && data.ok !== undefined) {
        this.showToast(false, data.message);
        this.pdfMode = 'none';
        return false;
      }
      this.thumbnails = data.thumbnails || [];
      if (!this.thumbnails.length) {
        this.showToast(false, 'No se pudieron cargar las miniaturas');
        this.pdfMode = 'none';
        return false;
      }
      return true;
    },

    async startReorder() {
      if (!this.selectedEntry) return;
      this.pdfMode = 'reorder';
      this.pageOrder = [];
      if (!(await this._loadThumbnails())) return;
      this.pageOrder = this.thumbnails.map((_, i) => i);
    },

    async startSplit() {
      if (!this.selectedEntry) return;
      this.pdfMode = 'split';
      this.splitRanges = '';
      this.splitGroups = [];
      this.splitPreviewError = '';
      if (!(await this._loadThumbnails())) return;
      await this.updateSplitPreview();
    },

    async updateSplitPreview() {
      if (!this.selectedEntry) return;
      const ranges = this.splitRanges.trim();
      const url = `/api/explorer/split-preview?institution_id=${this.institutionId}&period_id=${this.periodId}`
                + `&path=${encodeURIComponent(this.selectedEntry.path)}`
                + (ranges ? `&ranges=${encodeURIComponent(ranges)}` : '');
      const data = await this._apiFetch(url);
      if (data.groups) {
        this.splitGroups = data.groups;
        this.splitPreviewError = '';
      } else {
        this.splitPreviewError = data.message || 'Rango inválido';
      }
    },

    // -1 si la página no pertenece a ningún grupo del preview actual
    splitGroupOf(pageIdx) {
      return this.splitGroups.findIndex(g => g.includes(pageIdx));
    },

    reorderDragStart(event, pos) {
      this.reorderDragIdx = pos;
    },

    reorderDrop(event, toPos) {
      if (this.reorderDragIdx === null || this.reorderDragIdx === toPos) return;
      const item = this.pageOrder.splice(this.reorderDragIdx, 1)[0];
      this.pageOrder.splice(toPos, 0, item);
      this.reorderDragIdx = null;
    },

    reorderDeletePage(pos) {
      if (this.pageOrder.length <= 1) {
        this.showToast(false, 'Debe quedar al menos 1 página');
        return;
      }
      this.pageOrder.splice(pos, 1);
    },

    async executeReorder() {
      if (!this.selectedEntry || !this.pageOrder.length) return;
      const data = await this._apiFetch('/api/explorer/reorder', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          institution_id: this.institutionId,
          period_id: this.periodId,
          path: this.selectedEntry.path,
          page_order: Array.from(this.pageOrder),  // desenvuelve Proxy reactivo
        }),
      });
      this.showToast(data.ok, data.message);
      if (data.ok) this.pdfMode = 'none';
    },

    async deleteEntry(entry) {
      const data = await this._apiFetch('/api/explorer/delete', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          institution_id: this.institutionId,
          period_id: this.periodId,
          path: entry.path,
        }),
      });
      this.confirmingDelete = null;
      this.showToast(data.ok, data.message);
      if (data.ok) {
        if (this.selectedEntry?.path === entry.path) {
          this.selectedEntry = null;
          this.previewUrl = null;
        }
        this.load();
      }
    },

    // ── Search ────────────────────────────────────────────────────────
    async runSearch() {
      const q = this.searchQuery.trim();
      if (!q) { this.searchResults = []; return; }
      this.searchLoading = true;
      const subPath = this.currentPath.split('/').slice(1).join('/');
      const url = `/api/explorer/search?institution_id=${this.institutionId}`
                + `&period_id=${this.periodId}&root=${this.rootFolder}`
                + `&path=${encodeURIComponent(subPath)}&q=${encodeURIComponent(q)}`;
      try {
        const data = await this._apiFetch(url);
        this.searchResults = data.entries || [];
      } catch { this.searchResults = []; }
      this.searchLoading = false;
    },

    navigateToSearchResult(entry) {
      this.clearSearch();
      if (entry.is_dir) {
        // Carpeta → entrar directamente
        this.navigateTo(entry.path);
      } else {
        // Archivo → ir al padre y seleccionarlo para preview
        const parts = entry.path.split('/');
        const parentPath = parts.length > 1 ? parts.slice(0, -1).join('/') : this.rootFolder;
        this.navigateTo(parentPath);
        this.$nextTick(() => { this.selectedEntry = entry; });
      }
    },

    // ── Destination search (compartido entre move y copy) ────────────
    async searchDestFolders() {
      const q = this.destSearchQuery.trim();
      if (!q) { this.destSearchResults = []; return; }
      this.destSearchLoading = true;
      const url = `/api/explorer/search?institution_id=${this.institutionId}`
                + `&period_id=${this.periodId}&root=${this.moveBrowserRoot || this.rootFolder}`
                + `&q=${encodeURIComponent(q)}`;
      try {
        const data = await this._apiFetch(url);
        this.destSearchResults = (data.entries || []).filter(e => e.is_dir);
      } catch { this.destSearchResults = []; }
      this.destSearchLoading = false;
    },

    clearDestSearch() {
      this.destSearchQuery = '';
      this.destSearchResults = [];
    },

    setMoveDestination(path) {
      this.clearDestSearch();
      this.loadMoveBrowser(path);
    },

    // ── Merge mode ────────────────────────────────────────────────────
    startMerge() {
      this.selectedPdfs = [];
      this.pdfMode = 'merge';
    },

    // ── Copy mode ─────────────────────────────────────────────────────
    startCopy(entry) {
      this.copyingEntries = [entry];
      this.moveBrowserRoot = this.rootFolder;
      this.moveBrowserPath = this.currentPath;
      this.pdfMode = 'copy';
      this.clearDestSearch();
      this.loadMoveBrowser(this.moveBrowserPath);
    },

    toggleCopyEntry(entry) {
      const idx = this.copyingEntries.findIndex(e => e.path === entry.path);
      if (idx >= 0) this.copyingEntries.splice(idx, 1);
      else this.copyingEntries.push(entry);
    },

    async executeCopy() {
      if (!this.copyingEntries.length) return;
      const dst = this.moveBrowserPath;
      let ok = true;
      for (const entry of this.copyingEntries) {
        const data = await this._apiFetch('/api/explorer/copy', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            institution_id: this.institutionId,
            period_id: this.periodId,
            src: entry.path,
            dst_folder: dst,
          }),
        });
        if (!data.ok) { this.showToast(false, data.message); ok = false; break; }
      }
      if (ok) {
        this.showToast(true, `${this.copyingEntries.length} elemento(s) copiado(s).`);
        this.pdfMode = 'none';
        this.copyingEntries = [];
        this.load();
      }
    },

    // ── Move mode ─────────────────────────────────────────────────────
    startMove(entry) {
      this.movingEntries = [entry];
      this.moveBrowserRoot = this.rootFolder;
      this.moveBrowserPath = this.currentPath;
      this.pdfMode = 'move';
      this.clearDestSearch();
      this.loadMoveBrowser(this.moveBrowserPath);
    },

    startBatchMove() {
      this.movingEntries = this.selectedEntries.map(path => {
        const entry = this.entries.find(e => e.path === path);
        return entry || { path, name: path.split('/').pop(), is_dir: false };
      });
      this.moveBrowserRoot = this.rootFolder;
      this.moveBrowserPath = this.currentPath;
      this.pdfMode = 'move';
      this.selectedEntries = [];
      this.clearDestSearch();
      this.loadMoveBrowser(this.moveBrowserPath);
    },

    switchMoveBrowserRoot(root) {
      this.moveBrowserRoot = root;
      this.moveBrowserPath = root;
      this.loadMoveBrowser(root);
    },

    toggleMoveEntry(entry) {
      const idx = this.movingEntries.findIndex(e => e.path === entry.path);
      if (idx >= 0) this.movingEntries.splice(idx, 1);
      else this.movingEntries.push(entry);
    },

    async loadMoveBrowser(path) {
      this.moveBrowserPath = path;
      const root = this.moveBrowserRoot || this.rootFolder;
      const subPath = path.split('/').slice(1).join('/');
      const url = `/api/explorer/list?institution_id=${this.institutionId}&period_id=${this.periodId}&root=${root}&path=${encodeURIComponent(subPath)}`;
      const res = await api.get(url);
      this.moveBrowserEntries = res.ok ? (res.data.entries || []).filter(e => e.is_dir) : [];
    },

    moveBrowserUp() {
      const root = this.moveBrowserRoot || this.rootFolder;
      const parts = this.moveBrowserPath.split('/').filter(Boolean);
      if (parts.length <= 1) this.loadMoveBrowser(root);
      else { parts.pop(); this.loadMoveBrowser(parts.join('/')); }
    },

    async executeMove() {
      if (!this.movingEntries.length) return;
      // Prevent moving into self or a child of a folder being moved
      const dst = this.moveBrowserPath;
      const invalid = this.movingEntries.find(e =>
        dst === e.path || dst.startsWith(e.path + '/')
      );
      if (invalid) {
        this.showToast(false, 'No puedes mover una carpeta dentro de sí misma.');
        return;
      }
      let ok = true;
      for (const entry of this.movingEntries) {
        const data = await this._apiFetch('/api/explorer/move', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            institution_id: this.institutionId,
            period_id: this.periodId,
            src: entry.path,
            dst_folder: dst,
          }),
        });
        if (!data.ok) { this.showToast(false, data.message); ok = false; break; }
      }
      if (ok) {
        this.showToast(true, `${this.movingEntries.length} elemento(s) movido(s).`);
        this.pdfMode = 'none';
        this.movingEntries = [];
        this.load();
      }
    },

    startListResize(event) {
      this._listResizing = true;
      const startX = event.clientX;
      const startW = this.listWidth;
      const onMove = (e) => {
        if (!this._listResizing) return;
        this.listWidth = Math.max(150, Math.min(480, startW + (e.clientX - startX)));
      };
      const onUp = () => {
        this._listResizing = false;
        document.removeEventListener('mousemove', onMove);
        document.removeEventListener('mouseup', onUp);
      };
      document.addEventListener('mousemove', onMove);
      document.addEventListener('mouseup', onUp);
    },

    startResize(event) {
      this._resizing = true;
      this._resizeStartX = event.clientX;
      this._resizeStartWidth = this.panelWidth;
      const onMove = (e) => {
        if (!this._resizing) return;
        const delta = this._resizeStartX - e.clientX;
        this.panelWidth = Math.max(320, Math.min(1020, this._resizeStartWidth + delta));
      };
      const onUp = () => {
        this._resizing = false;
        document.removeEventListener('mousemove', onMove);
        document.removeEventListener('mouseup', onUp);
      };
      document.addEventListener('mousemove', onMove);
      document.addEventListener('mouseup', onUp);
    },

    // Kept as a thin wrapper so existing calls work; the state lives in Alpine.store('toast').
    showToast(ok, message) {
      Alpine.store('toast').legacy(ok, message);
    },

    // ── Selección en lote ──────────────────────────────────────────────

    startSelectMode() {
      this.pdfMode = 'select';
      this.selectedEntries = [];
      this.batchDeleteConfirm = false;
    },

    toggleSelectEntry(path) {
      const idx = this.selectedEntries.indexOf(path);
      if (idx >= 0) this.selectedEntries.splice(idx, 1);
      else this.selectedEntries.push(path);
    },

    selectAllEntries() {
      if (this.selectedEntries.length === this.entries.length) {
        this.selectedEntries = [];
      } else {
        this.selectedEntries = this.entries.map(e => e.path);
      }
    },

    async executeBatchDelete() {
      if (!this.selectedEntries.length) return;
      const data = await this._apiFetch('/api/explorer/delete-batch', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          institution_id: this.institutionId,
          period_id: this.periodId,
          paths: Array.from(this.selectedEntries),
        }),
      });
      this.batchDeleteConfirm = false;
      if (data.deleted) {
        const n = data.deleted.length;
        const errMsg = data.errors?.length ? ` (${data.errors.length} error(es))` : '';
        this.showToast(n > 0, `${n} elemento(s) eliminado(s)${errMsg}`);
        this.pdfMode = 'none';
        this.selectedEntries = [];
        if (this.selectedEntry && data.deleted.includes(this.selectedEntry.path)) {
          this.selectedEntry = null;
          this.previewUrl = null;
        }
        this.load();
      } else {
        this.showToast(false, data.message || 'Error al eliminar');
      }
    },

    // ── Upload ────────────────────────────────────────────────────────

    async uploadFiles(fileList, relativePaths = [], destPath = null) {
      const files = Array.from(fileList);
      if (!files.length) return;

      // Para upload de carpeta enviamos todos los archivos (el backend filtra no-PDFs
      // y los cuenta en non_pdf_count). Para upload de PDFs sueltos pre-filtramos.
      const isFolderUpload = relativePaths.length === files.length && relativePaths.some(Boolean);
      const filesToSend = isFolderUpload
        ? files
        : files.filter(f => f.name.toLowerCase().endsWith('.pdf'));

      if (!filesToSend.length) {
        this.showToast(false, 'No se encontraron archivos PDF para subir');
        return;
      }

      const targetPath = destPath ?? this.currentPath;
      this.uploadProgress = { done: 0, total: filesToSend.length, errors: 0 };

      const relPaths = isFolderUpload
        ? filesToSend.map(f => relativePaths[files.indexOf(f)])
        : [];

      const formData = new FormData();
      formData.append('institution_id', this.institutionId);
      formData.append('period_id', this.periodId);
      formData.append('path', targetPath);
      formData.append('relative_paths_json', JSON.stringify(relPaths));
      filesToSend.forEach(f => formData.append('files', f));

      const data = await this._apiFetch('/api/explorer/upload', { method: 'POST', body: formData });

      this.uploadProgress = null;

      if (data.uploaded !== undefined) {
        const n = data.uploaded.length;
        const nonPdf = data.non_pdf_count ?? 0;
        const errSkipped = (data.skipped?.length ?? 0) - nonPdf;

        if (n > 0)         this.showToast(true,   `${n} PDF(s) subido(s) correctamente`);
        else               this.showToast(false,  'No se subió ningún archivo');
        if (nonPdf > 0)    this.showToast('warn', `${nonPdf} omitido(s): no son PDF`);
        if (errSkipped > 0) this.showToast(false, `${errSkipped} error(es) al escribir`);

        if (n > 0) this.load();
      } else {
        this.showToast(false, data.message || 'Error al subir archivos');
      }
    },

    onExplorerDragOver(event) {
      // Solo activar overlay si son archivos externos (no drag interno de entradas)
      if (event.dataTransfer.types.includes('Files')) {
        event.preventDefault();
        this.isDragOver = true;
      }
    },

    onExplorerDragLeave(event) {
      // Solo desactivar si el puntero sale del contenedor completo
      if (!event.currentTarget.contains(event.relatedTarget)) {
        this.isDragOver = false;
      }
    },

    async onExplorerFileDrop(event) {
      event.preventDefault();
      this.isDragOver = false;
      const files = event.dataTransfer.files;
      if (!files || !files.length) return;
      // Obtener rutas relativas (solo disponibles para folder drop en Chrome/Edge)
      const relativePaths = Array.from(files).map(f => f.webkitRelativePath || '');
      await this.uploadFiles(files, relativePaths);
    },

    // ── Crear carpeta ─────────────────────────────────────────────────

    async createFolder() {
      const name = this.newFolderName.trim();
      if (!name) return;
      const data = await this._apiFetch('/api/explorer/mkdir', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          institution_id: this.institutionId,
          period_id: this.periodId,
          path: this.currentPath,
          name,
        }),
      });
      this.showToast(data.ok, data.message);
      this.creatingFolder = false;
      this.newFolderName = '';
      if (data.ok) this.load();
    },

    // ── Descarga ZIP ──────────────────────────────────────────────────

    async downloadZip(paths) {
      const params = new URLSearchParams({
        institution_id: this.institutionId,
        period_id: this.periodId,
      });
      paths.forEach(p => params.append('rel_paths', p));

      const label = paths.length === 1
        ? paths[0].split('/').pop()
        : `${paths.length} elementos`;
      this.showToast('loading', `Preparando ZIP: ${label}…`);

      try {
        // Descarga binaria (blob): se queda en fetch, api.* solo entiende JSON/texto.
        const response = await fetch(`/api/explorer/download-zip?${params}`);
        if (!response.ok) {
          const err = await response.json().catch(() => ({}));
          this.showToast(false, apiErrorMessage(err.detail, 'Error al generar el ZIP'));
          return;
        }
        const blob = await response.blob();
        const filename = (response.headers.get('Content-Disposition') || '')
          .match(/filename="([^"]+)"/)?.[1] ?? 'descarga.zip';
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = filename;
        a.click();
        URL.revokeObjectURL(url);
        const mb = (blob.size / (1024 * 1024)).toFixed(1);
        this.showToast(true, `ZIP listo — ${mb} MB`);
      } catch {
        this.showToast(false, 'Error de red al descargar');
      }
    },

    // Abrir explorador — siempre arranca en STAGE
    open() {
      this.explorerOpen = true;
      this.rootFolder = 'STAGE';
      this.currentPath = 'STAGE';
      this.load();
    },
  }));
});
