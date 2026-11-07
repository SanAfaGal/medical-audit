function pipeline() {
  return {
    selectedStages: [],
    running: false,
    logLines: [],
    currentStageLabel: '',
    excelFile: null,
    dragOver: false,
    zipFile: null,
    zipDragOver: false,
    invoiceDocTypeId: '',
    medSheetDocTypeId: '',
    downloadDriveFolders: '',
    docTypes: [],
    unmappedConfirm: null,
    surplusData: null,
    annulData: null,
    nonPdfData: null,
    exportReady: false,
    previewFile: null,
    deleteConfirmPending: null,
    stageStatuses: {},
    stageResults: {},
    hoveredStage: null,
    taskId: null,
    _evtSource: null,
    // Inactividad: segundos desde la última línea de log (los latidos SSE no cuentan)
    lastLineAt: Date.now(),
    idleSeconds: 0,
    idleWarnSeconds: 30,  // umbral del aviso; ajustable desde la consola para pruebas
    beat: null,        // último latido del servidor {idle, elapsed, at}; llega en CUALQUIER etapa
    beatFresh: false,  // true si el último latido es reciente (el servidor sigue conectado y trabajando)
    streamActive: false,  // hay un stream SSE abierto (las subidas por fetch no tienen latido ni aviso)
    _idleTimer: null,

    // La tarea de esta pestaña pertenece a otro hospital/periodo que el seleccionado ahora
    get taskContextMismatch() {
      const p = this.$store.pipeline, c = this.$store.ctx;
      return this.running && p.institutionId != null
        && (p.institutionId != c.institutionId || p.periodId != c.periodId);
    },

    stageFlow: [

      // ── 1. CARGA DE DATOS ────────────────────────────────────────────────
      { type:'phase', label:'Carga de datos', color:'#0284c7', icon:'fa-solid fa-database' },
      { type:'stage', id:'LOAD_AND_PROCESS',    label:'Ingestar reporte SIHOS',    icon:'fa-solid fa-file-excel',    phaseColor:'#0284c7' },
      { type:'stage', id:'RECATEGORIZE_SERVICES', label:'Re-categorizar servicios', icon:'fa-solid fa-arrows-rotate', phaseColor:'#0284c7' },
      { type:'stage', id:'LOAD_DRIVE_ZIP',      label:'Descomprimir archivos',     icon:'fa-solid fa-file-zipper',   phaseColor:'#0284c7', heavy:true },
      { type:'stage', id:'RUN_STAGING',         label:'Mover archivos a STAGE',    icon:'fa-solid fa-truck-moving',  phaseColor:'#0284c7' },

      // ── 2. LIMPIEZA ──────────────────────────────────────────────────────
      { type:'phase', label:'Limpieza', color:'#7c3aed', icon:'fa-solid fa-broom' },
      { type:'stage', id:'REMOVE_NON_PDF',                label:'Eliminar archivos inválidos',        icon:'fa-solid fa-file-circle-xmark',      phaseColor:'#7c3aed', heavy:true },
      { type:'stage', id:'CHECK_NESTED_FOLDERS',          label:'Detectar subcarpetas',               icon:'fa-solid fa-folder-tree',            phaseColor:'#7c3aed' },
      { type:'stage', id:'CHECK_FOLDERS_WITH_EXTRA_TEXT', label:'Detectar carpetas con texto extra',  icon:'fa-solid fa-folder-open',            phaseColor:'#7c3aed' },
      { type:'stage', id:'NORMALIZE_DIR_NAMES',           label:'Corregir nombres de carpetas',       icon:'fa-solid fa-folder',                 phaseColor:'#7c3aed' },
      { type:'stage', id:'NORMALIZE_FILES',               label:'Corregir nombres de archivos',       icon:'fa-solid fa-tag',                    phaseColor:'#7c3aed' },

      // ── 3. CALIDAD DE PDFs ───────────────────────────────────────────────
      { type:'phase', label:'Calidad de PDFs', color:'#4338ca', icon:'fa-solid fa-file-pdf' },
      { type:'stage', id:'LIST_UNREADABLE_PDFS', label:'Detectar PDFs ilegibles', icon:'fa-solid fa-list-check', phaseColor:'#4338ca' },
      { type:'stage', id:'DELETE_UNREADABLE_PDFS', label:'Eliminar PDFs ilegibles',   icon:'fa-solid fa-file-circle-minus',    phaseColor:'#dc2626' },

      // ── 4. AUDITORÍA ─────────────────────────────────────────────────────
      { type:'phase', label:'Auditoría', color:'#b45309', icon:'fa-solid fa-magnifying-glass-chart' },
      { type:'stage', id:'VERIFY_INVOICE_CODE',           label:'Verificar número en PDF',         icon:'fa-solid fa-magnifying-glass',       phaseColor:'#b45309' },
      { type:'stage', id:'CHECK_INVOICE_NUMBER_ON_FILES', label:'Verificar nombres de archivos',   icon:'fa-solid fa-file-circle-check',      phaseColor:'#b45309' },
      { type:'stage', id:'CHECK_DIRS',                    label:'Detectar carpetas faltantes',     icon:'fa-solid fa-folder-minus',           phaseColor:'#b45309' },
      { type:'info', label:'Revisión manual', text:'Las facturas de tipo SOAT deben ser auditadas manualmente por el auditor responsable. Actualiza su estado en Auditoría antes de ejecutar las siguientes etapas para excluirlas del procesamiento automático.' },
      { type:'stage', id:'MARK_UNKNOWN_DIRS',             label:'Marcar carpetas desconocidas',    icon:'fa-solid fa-circle-question',           phaseColor:'#b45309' },
      { type:'stage', id:'CHECK_REQUIRED_DOCS',           label:'Verificar documentos requeridos', icon:'fa-solid fa-file-circle-question',   phaseColor:'#b45309' },
      { type:'stage', id:'REVISAR_SOBRANTES',             label:'Revisar archivos sobrantes',      icon:'fa-solid fa-file-circle-exclamation',phaseColor:'#b45309' },
      { type:'stage', id:'VERIFY_CUFE',                   label:'Verificar CUFE',                  icon:'fa-solid fa-qrcode',                 phaseColor:'#b45309' },

      // ── 5. CIERRE ────────────────────────────────────────────────────────
      { type:'phase', label:'Cierre', color:'#059669', icon:'fa-solid fa-box-archive' },
      { type:'stage', id:'ORGANIZE',       label:'Organizar en AUDIT',   icon:'fa-solid fa-boxes-stacked', phaseColor:'#059669' },
      { type:'stage', id:'COMPRESS_AUDIT',       label:'Comprimir documentos',      icon:'fa-solid fa-file-zipper',   phaseColor:'#059669', heavy:true },
      { type:'stage', id:'EXPORTAR_AUDITADOS',   label:'Exportar documentos auditados', icon:'fa-solid fa-file-export', phaseColor:'#059669', heavy:true },

      // ── 6. DESCARGAS — SIHOS ────────────────────────────────────────────
      { type:'phase', label:'Descargas · SIHOS', color:'#0369a1', icon:'fa-solid fa-hospital' },
      { type:'stage', id:'DOWNLOAD_INVOICES_FROM_SIHOS', label:'Descargar facturas desde SIHOS',    icon:'fa-solid fa-cloud-arrow-down',  phaseColor:'#0369a1', heavy:true },
      { type:'stage', id:'DOWNLOAD_MEDICATION_SHEETS',   label:'Descargar hojas de medicamentos',  icon:'fa-solid fa-pills',             phaseColor:'#0369a1', heavy:true },

      // ── 7. DESCARGAS — DRIVE ────────────────────────────────────────────
      { type:'phase', label:'Descargas · Drive', color:'#166534', icon:'fa-brands fa-google-drive' },
      { type:'note', label:'Requiere reprocesamiento', text:'Los archivos descargados desde Drive no han sido procesados. Tras ejecutar estas etapas, vuelve a correr el ciclo desde Limpieza para normalizarlos, verificarlos y auditarlos.' },
      { type:'stage', id:'DOWNLOAD_DRIVE',               label:'Descargar carpetas faltantes',     icon:'fa-brands fa-google-drive',     phaseColor:'#166534', heavy:true },
      { type:'stage', id:'DOWNLOAD_MISSING_DOCS',        label:'Descargar documentos faltantes',   icon:'fa-solid fa-file-arrow-down',   phaseColor:'#166534', heavy:true },

    ],

    stages: [
      { id:'LOAD_AND_PROCESS',        label:'Ingestar reporte SIHOS',           description:'Lee el Excel exportado desde SIHOS e inserta las facturas del período en la base de datos. Registra administradoras, contratos y servicios nuevos en crudo. Filas sin número de factura o fecha válida son omitidas.' },
      { id:'RECATEGORIZE_SERVICES',   label:'Re-categorizar servicios',          description:'Reaplica los mapeos de servicios actuales a todas las facturas del período sin volver a cargar el Excel. Útil tras corregir un mapeo en Configuración → Servicios.' },
      { id:'LOAD_DRIVE_ZIP',          label:'Descomprimir archivos',             description:'Extrae un archivo .zip, .7z o .rar directamente en la carpeta DRIVE del período. Ideal para cargar en bloque las carpetas de facturas descargadas manualmente desde Drive.' },
      { id:'RUN_STAGING',             label:'Mover archivos a STAGE',            description:'Mueve las carpetas hoja (sin subcarpetas) de DRIVE a STAGE. STAGE es el área de trabajo donde ocurren todas las operaciones de auditoría.' },
      { id:'REMOVE_NON_PDF',          label:'Eliminar archivos inválidos',        description:'Elimina de cada carpeta en STAGE: (1) archivos que no son PDF, y (2) PDFs corruptos o dañados que no se pueden abrir. Deja las carpetas listas para el procesamiento.' },
      { id:'CHECK_NESTED_FOLDERS',    label:'Detectar subcarpetas',              description:'Lista las carpetas de STAGE que contienen subcarpetas. Los archivos dentro de subcarpetas no se procesan correctamente; deben moverse al nivel raíz de la carpeta de factura.' },
      { id:'CHECK_FOLDERS_WITH_EXTRA_TEXT', label:'Detectar carpetas con texto extra', description:'Detecta carpetas con texto adicional en el nombre más allá del número de factura (ej. "360052_SINCUFE", "360099 copia"). Solo lista; no modifica nada. Presta atención a las que dicen "ANULAR".' },
      { id:'NORMALIZE_DIR_NAMES',     label:'Corregir nombres de carpetas',      description:'Renombra las carpetas con texto adicional eliminando el texto sobrante. Las carpetas _SINCUFE se renombran conservando ese sufijo; el CUFE se verificará en la etapa correspondiente.' },
      { id:'NORMALIZE_FILES',         label:'Corregir nombres de archivos',      description:'Corrige nombres en dos pasos: (1) aplica sustituciones de prefijo configuradas (ej. FVE→FEV); (2) renombra archivos con nombre inválido al formato {PREFIJO}_{NIT}_{FACTURA}.pdf. Reporta los que no se pueden renombrar.' },
      { id:'LIST_UNREADABLE_PDFS',    label:'Detectar PDFs ilegibles',           description:'Detecta PDFs de factura sin capa de texto (escaneados). Solo lista los archivos afectados; no los modifica. Úsala para decidir si eliminarlos y re-descargarlos.' },
      { id:'DELETE_UNREADABLE_PDFS',  label:'Eliminar PDFs ilegibles',           description:'Elimina permanentemente los PDFs detectados sin capa de texto. Combínala con "Descargar PDFs desde SIHOS" para reemplazarlos con versiones digitales correctas.' },
      { id:'VERIFY_INVOICE_CODE',     label:'Verificar número en PDF',           description:'Lee el texto de cada PDF de factura y verifica que el número de la carpeta aparezca dentro del documento. Reporta con [WARN] las facturas cuyo PDF no contiene su propio número.' },
      { id:'CHECK_INVOICE_NUMBER_ON_FILES', label:'Verificar nombres de archivos', description:'Recorre cada carpeta de STAGE y verifica que todos los archivos contengan el número de factura de la carpeta en su nombre. Reporta los archivos desajustados.' },
      { id:'CHECK_DIRS',              label:'Detectar carpetas faltantes',        description:'Comprueba que cada factura en estado PRESENTE tenga su carpeta en STAGE. Las que no la tienen se marcan como FALTANTE para recuperarlas desde Drive.' },
      { id:'MARK_UNKNOWN_DIRS',       label:'Marcar carpetas desconocidas',       description:'Detecta carpetas en STAGE con formato de número de factura (PREFIX+dígitos) que no tienen registro en la base de datos del período. Las renombra con el prefijo "(DESCONOCIDO)" para identificación visual. La operación es segura de re-ejecutar; las carpetas ya marcadas se omiten.' },
      { id:'CHECK_REQUIRED_DOCS',     label:'Verificar documentos requeridos',   description:'Para cada factura PRESENTE, verifica que su carpeta contenga todos los documentos exigidos según el tipo de servicio. Registra un hallazgo por cada documento faltante y cambia el estado de la factura a PENDIENTE.' },
      { id:'REVISAR_SOBRANTES',       label:'Revisar archivos sobrantes',         description:'Detecta archivos cuyo nombre no coincide con ningún prefijo requerido. Sugiere renombrarlos al tipo de documento faltante y permite confirmar o corregir cada sugerencia antes de aplicar el cambio en disco.' },
      { id:'VERIFY_CUFE',             label:'Verificar CUFE',                     description:'Valida que cada PDF de factura contenga un código CUFE válido. Las carpetas sin CUFE se renombran añadiendo el sufijo _SINCUFE para identificación visual.' },
      { id:'ORGANIZE',                label:'Organizar en AUDIT',                 description:'Mueve de STAGE a AUDIT las carpetas de facturas PRESENTE sin hallazgos pendientes, organizándolas en subcarpetas por administradora y contrato. Actualiza su estado a AUDITADA en la base de datos.' },
      { id:'COMPRESS_AUDIT',          label:'Comprimir documentos',               description:'Comprime con Ghostscript (calidad ebook, 150 dpi) todos los PDFs en AUDIT. Reduce el tamaño de archivo sin pérdida visual significativa. Reporta ahorro total en MB. Los archivos que no puedan comprimirse se registran como advertencia y se omiten.' },
      { id:'EXPORTAR_AUDITADOS',      label:'Exportar documentos auditados',      description:'Empaqueta toda la carpeta AUDIT en un archivo ZIP descargable. El ZIP queda guardado en la carpeta de exportaciones del período y puede descargarse desde el botón que aparece al finalizar la etapa.' },
      { id:'DOWNLOAD_INVOICES_FROM_SIHOS', label:'Descargar facturas desde SIHOS', description:'Requiere sesión en SIHOS. Descarga el PDF de factura de cada factura PENDIENTE con hallazgo sin resolver del tipo de documento seleccionado. Solo descarga la factura en sí; no otros documentos del expediente.' },
      { id:'DOWNLOAD_MEDICATION_SHEETS',   label:'Descargar hojas de medicamentos', description:'Requiere sesión en SIHOS. Descarga las hojas de medicamentos de facturas PENDIENTE con hallazgo sin resolver del tipo seleccionado y número de admisión registrado.' },
      { id:'DOWNLOAD_DRIVE',          label:'Descargar carpetas (Drive)',          description:'Busca en Google Drive las carpetas de facturas marcadas como FALTANTE y las descarga a STAGE. Su estado se actualiza automáticamente a PRESENTE.' },
      { id:'DOWNLOAD_MISSING_DOCS',   label:'Descargar documentos faltantes',     description:'Para cada hallazgo pendiente, busca en Google Drive el archivo esperado ({prefijo}_{NIT}_{factura}.pdf) y lo descarga a la carpeta correspondiente en STAGE.' },
    ],

    get completedCount() {
      return this.selectedStages.filter(id => ['done','warn','error'].includes(this.stageStatuses[id])).length;
    },
    get progressPct() {
      if (!this.selectedStages.length) return 0;
      return Math.round(this.completedCount / this.selectedStages.length * 100);
    },

    stageById(id) { return this.stages.find(s => s.id === id) || null; },

    selectAll(checked) {
      const ids = this.stageFlow.filter(i => i.type === 'stage').map(i => i.id);
      this.selectedStages = checked ? ids : [];
    },

    setFile(ev) { this.excelFile = ev.target.files[0] || null; },
    handleDrop(ev) {
      this.dragOver = false;
      const f = ev.dataTransfer.files[0];
      if (f && /\.(xlsx|xls)$/i.test(f.name)) this.excelFile = f;
    },
    setZipFile(ev) { this.zipFile = ev.target.files[0] || null; },
    handleZipDrop(ev) {
      this.zipDragOver = false;
      const f = ev.dataTransfer.files[0];
      if (f && /\.(zip|7z|rar)$/i.test(f.name)) this.zipFile = f;
    },

    lineClass(line) {
      if (line.startsWith('───'))    return '';
      if (line.startsWith('[ERROR]')) return 'text-red-400';
      if (line.startsWith('[WARN]'))  return 'text-amber-300';
      if (line.startsWith('[OK]'))    return 'text-emerald-400';
      if (line.startsWith('[INFO]'))  return 'text-slate-400';
      return 'text-slate-500';
    },
    linePrefix(line) {
      if (line.startsWith('[ERROR]')) return '<span style="color:#ef4444;font-weight:700;user-select:none;font-size:9.5px">ERR</span>';
      if (line.startsWith('[WARN]'))  return '<span style="color:#f59e0b;font-weight:700;user-select:none;font-size:9.5px">WRN</span>';
      if (line.startsWith('[OK]'))    return '<span style="color:#22c55e;font-weight:700;user-select:none;font-size:9.5px"> OK</span>';
      if (line.startsWith('[INFO]'))  return '<span style="color:#3b82f6;font-weight:700;user-select:none;font-size:9.5px">INF</span>';
      return '<span style="user-select:none;font-size:9.5px">   </span>';
    },
    lineBody(line) {
      const pfx = ['[ERROR]','[WARN]','[OK]','[INFO]'].find(p => line.startsWith(p));
      return pfx ? line.slice(pfx.length).trimStart() : line;
    },

    _markLine(stageId, line) {
      if (line.startsWith('[ERROR]')) this.stageStatuses[stageId] = 'error';
      else if (line.startsWith('[WARN]') && this.stageStatuses[stageId] !== 'error') this.stageStatuses[stageId] = 'warn';
    },

    _updateStageResult(stageId, line) {
      if (!line.startsWith('[INFO]') && !line.startsWith('[OK]')) return;
      const text = line.replace(/^\[(INFO|OK)\]\s*/, '').trim();
      if (/:\s*\d+/.test(text) || /\d+\/\d+/.test(text)) {
        this.stageResults[stageId] = text.length > 52 ? text.slice(0,49)+'…' : text;
      }
    },

    clearLog() {
      this.logLines = [];
      this.currentStageLabel = '';
      this.stageStatuses = {};
      this.stageResults = {};
      this.exportReady = false;
      ['pipeline_logLines','pipeline_currentStageLabel','pipeline_stageStatuses','pipeline_stageResults'].forEach(k => sessionStorage.removeItem(k));
    },

    // Stages that must run synchronously (require user interaction or file upload)
    _isSyncStage(stageId) {
      return stageId === 'LOAD_AND_PROCESS'
          || stageId === 'LOAD_DRIVE_ZIP'
          || stageId === 'REVISAR_SOBRANTES'
          || stageId === 'REMOVE_NON_PDF'
          || stageId === 'CHECK_FOLDERS_WITH_EXTRA_TEXT';
    },

    async _runBackgroundStage(stageId, institutionId, periodId) {
      const params = new URLSearchParams({ institution_id: institutionId, period_id: periodId });
      if (stageId === 'DOWNLOAD_INVOICES_FROM_SIHOS' && this.invoiceDocTypeId)
        params.set('doc_type_id', this.invoiceDocTypeId);
      if (stageId === 'DOWNLOAD_MEDICATION_SHEETS' && this.medSheetDocTypeId)
        params.set('doc_type_id', this.medSheetDocTypeId);
      if (stageId === 'DOWNLOAD_DRIVE' && this.downloadDriveFolders.trim()) {
        const nums = this.downloadDriveFolders.split('\n').map(l => l.trim()).filter(Boolean).join(',');
        if (nums) params.set('invoice_numbers', nums);
      }

      const res = await api.post(`/api/pipeline/run/${stageId}?${params}`);
      if (!res.ok) throw new Error(res.error || `Error al iniciar ${stageId}`);
      const { task_id } = res.data;
      this.taskId = task_id;
      this.$store.pipeline.setTask(
        task_id, stageId, parseInt(institutionId), parseInt(periodId),
        this.$store.ctx.institution?.display_name || '',
      );

      await new Promise((resolve) => {
        this._subscribeToTask(task_id, stageId, 0, resolve);
      });
    },

    _subscribeToTask(taskId, stageId, fromCursor, onDone) {
      if (this._evtSource) { this._evtSource.close(); this._evtSource = null; }
      const evtSource = new EventSource(`/api/pipeline/stream/${taskId}?from=${fromCursor}`);
      this._evtSource = evtSource;
      this.streamActive = true;
      this._attachHeartbeat(evtSource);
      let cursor = fromCursor;

      evtSource.onmessage = (event) => {
        let parsed;
        try { parsed = JSON.parse(event.data); } catch { return; }
        const line = parsed.msg;
        cursor = (parsed.idx != null ? parsed.idx : cursor - 1) + 1;
        this._noteActivity();

        if (line === '[DONE]') {
          evtSource.close(); this._evtSource = null; this.streamActive = false;
          const finalStatus = parsed.status || 'done';
          if (this.stageStatuses[stageId] === 'running')
            this.stageStatuses[stageId] = finalStatus === 'cancelled' ? 'error' : 'done';
          this.$store.pipeline.update(finalStatus, cursor);
          if (finalStatus === 'done' || finalStatus === 'cancelled') this.$store.pipeline.clear();
          // In reconnect mode (no onDone), we own the running state
          if (!onDone) { this.running = false; this.currentStageLabel = ''; }
          if (onDone) onDone();
          return;
        }

        this.$store.pipeline.update('running', cursor);

        if (this._consumeProgress(line)) return;

        // Handle [DATA] signals — do not push to log
        if (line.startsWith('[DATA] export_ready:')) {
          this.exportReady = true;
          return;
        }

        this._markLine(stageId, line);
        this._updateStageResult(stageId, line);
        this.logLines.push(line);
        this.$nextTick(() => this.scrollLog());
      };

      evtSource.onerror = () => {
        evtSource.close(); this._evtSource = null; this.streamActive = false;
        this.logLines.push('[WARN] Conexión interrumpida. Verificando estado de la tarea…');
        this.$nextTick(() => this.scrollLog());
        setTimeout(async () => {
          if (!this.$store.pipeline.isRunning || this.$store.pipeline.taskId !== taskId) {
            if (onDone) onDone();
            return;
          }
          // Verify the task still exists on the server before reconnecting
          try {
            const res = await api.get(`/api/pipeline/status/${taskId}`);
            if (res.status === 404) {
              this.logLines.push('[WARN] La tarea ya no existe en el servidor (reinicio detectado). Estado limpiado.');
              this.$nextTick(() => this.scrollLog());
              this.$store.pipeline.clear();
              this.running = false;
              this.currentStageLabel = '';
              if (onDone) onDone();
              return;
            }
          } catch { /* network error — try to reconnect anyway */ }
          this._subscribeToTask(taskId, stageId, this.$store.pipeline.cursor, onDone);
        }, 2000);
      };
    },

    // ── Manejo central de streams ────────────────────────────────────────────────────────────
    // Todas las etapas (SSE directo o tarea en segundo plano) pasan por estas funciones, así
    // el log, la barra de progreso y el aviso de inactividad se comportan igual en todas.

    // Llegó algo del servidor: reinicia el contador de inactividad
    _noteActivity() {
      this.lastLineAt = Date.now(); this.idleSeconds = 0;
    },

    // Líneas '[DATA] {"type":"progress",...}' alimentan la barra N/total y no van al log.
    // Devuelve true si la línea fue consumida.
    _consumeProgress(line) {
      if (!line.startsWith('[DATA] {')) return false;
      try {
        const d = JSON.parse(line.slice(7));
        if (d.type !== 'progress') return false;
        this.$store.pipeline.setProgress({
          current: d.current, total: d.total, item: d.item || '', label: d.label || 'Elementos',
        });
        return true;
      } catch { return false; }
    },

    // Latido del servidor (evento SSE 'heartbeat'): confirma que la etapa sigue ejecutándose
    _attachHeartbeat(evtSource) {
      evtSource.addEventListener('heartbeat', (e) => {
        try {
          const d = JSON.parse(e.data);
          this.beat = { idle: d.idle, elapsed: d.elapsed, at: Date.now() };
          this.beatFresh = true;
        } catch {}
      });
    },

    _fmtDuration(sec) {
      sec = Math.max(0, Math.round(sec || 0));
      const m = Math.floor(sec / 60), s = sec % 60;
      return m > 0 ? `${m} min ${s} s` : `${s} s`;
    },

    // Etapa interactiva por SSE directo (GET /run/{stage}). onData recibe el JSON de las líneas [DATA].
    _runSyncStage(stageId, institutionId, periodId, label, onData) {
      const url = `/api/pipeline/run/${stageId}?institution_id=${encodeURIComponent(institutionId)}&period_id=${encodeURIComponent(periodId)}`;
      const evtSource = new EventSource(url);
      this.streamActive = true;
      this.$store.pipeline.progress = null;
      this.beat = null;
      this._noteActivity();
      this._attachHeartbeat(evtSource);

      return new Promise((resolve) => {
        evtSource.onmessage = (event) => {
          let line = event.data;
          try { const p = JSON.parse(event.data); line = p.msg || event.data; } catch {}
          if (line === '[DONE]') { evtSource.close(); this.streamActive = false; resolve(); return; }
          this._noteActivity();
          if (this._consumeProgress(line)) return;
          if (line.startsWith('[DATA] ')) {
            try { onData && onData(JSON.parse(line.slice(7))); } catch (e) {}
            return;
          }
          this._markLine(stageId, line);
          this._updateStageResult(stageId, line);
          this.logLines.push(line);
          this.$nextTick(() => this.scrollLog());
        };
        evtSource.onerror = () => {
          evtSource.close();
          this.streamActive = false;
          const msg = `[ERROR] Conexión interrumpida en etapa ${label || stageId}`;
          this.logLines.push(msg); this._markLine(stageId, msg); resolve();
        };
      });
    },

    // Re-engancha una tarea en curso del hospital/periodo seleccionado (p. ej. tras cerrar la pestaña)
    async _reattachActiveTask() {
      const ctx = this.$store.ctx;
      if (!ctx.institutionId || !ctx.periodId || this.running) return;
      try {
        const res = await api.get(`/api/pipeline/active?institution_id=${ctx.institutionId}&period_id=${ctx.periodId}`);
        if (!res.ok) return;
        const [run] = res.data;
        if (!run || this.running) return;
        this.$store.pipeline.setTask(run.task_id, run.stage, run.institution_id, run.period_id,
                                     ctx.institution?.display_name || '');
        this.taskId = run.task_id;
        this.running = true;
        this.currentStageLabel = this.stageById(run.stage)?.label || run.stage;
        this.stageStatuses[run.stage] = 'running';
        this.lastLineAt = Date.now();
        this.logLines = ['─── Reconectado a una tarea en curso para este hospital ───'];
        this._subscribeToTask(run.task_id, run.stage, 0, null);
      } catch { /* sin conexión: se reintenta al cambiar de periodo */ }
    },

    async cancelPipeline() {
      this.streamActive = false;
      const taskId = this.taskId || this.$store.pipeline.taskId;
      if (this._evtSource) { this._evtSource.close(); this._evtSource = null; }
      if (taskId) await api.delete(`/api/pipeline/${taskId}`);
      this.running = false;
      this.currentStageLabel = '';
      this.$store.pipeline.clear();
      this.logLines.push('[WARN] Pipeline detenido por el usuario.');
      this.$nextTick(() => this.scrollLog());
    },

    init() {
      try {
        const sl  = sessionStorage.getItem('pipeline_logLines');
        const sl2 = sessionStorage.getItem('pipeline_currentStageLabel');
        const ss  = sessionStorage.getItem('pipeline_selectedStages');
        const st  = sessionStorage.getItem('pipeline_stageStatuses');
        const sr  = sessionStorage.getItem('pipeline_stageResults');
        if (sl)  this.logLines          = JSON.parse(sl);
        if (sl2) this.currentStageLabel = sl2;
        if (ss)  this.selectedStages    = JSON.parse(ss);
        if (st)  this.stageStatuses     = JSON.parse(st);
        if (sr)  this.stageResults      = JSON.parse(sr);
      } catch {}
      this.$watch('logLines',          v => sessionStorage.setItem('pipeline_logLines',          JSON.stringify(v)));
      this.$watch('currentStageLabel', v => sessionStorage.setItem('pipeline_currentStageLabel', v));
      this.$watch('selectedStages',    v => sessionStorage.setItem('pipeline_selectedStages',    JSON.stringify(v)));
      this.$watch('stageStatuses',     v => sessionStorage.setItem('pipeline_stageStatuses',     JSON.stringify(v)));
      this.$watch('stageResults',      v => sessionStorage.setItem('pipeline_stageResults',      JSON.stringify(v)));
      api.get('/api/settings/doc-types').then(r => { if (r.ok) this.docTypes = r.data; });

      // Detección de inactividad: avisa si no llegan líneas nuevas y consulta al servidor
      this._idleTimer = setInterval(() => {
        this.idleSeconds = this.running && this.streamActive ? Math.floor((Date.now() - this.lastLineAt) / 1000) : 0;
        this.beatFresh = !!this.beat && Date.now() - this.beat.at < 45000;
      }, 5000);

      // Si cambia de hospital/periodo y no hay tarea local, buscar una activa en el servidor
      this.$watch('$store.ctx.periodId', () => { if (!this.running) this._reattachActiveTask(); });

      // Reconectar si hay una tarea activa al volver a la página
      if (!(this.$store.pipeline.isRunning && this.$store.pipeline.taskId)) {
        this._reattachActiveTask();
      }
      if (this.$store.pipeline.isRunning && this.$store.pipeline.taskId) {
        const { taskId, stage, cursor } = this.$store.pipeline;
        this.running = true;
        this.taskId = taskId;
        const stageInfo = this.stageById(stage);
        this.currentStageLabel = stageInfo?.label || stage || '';
        this.stageStatuses[stage] = 'running';
        this.logLines.push('─── Reconectando al pipeline en curso... ───');
        this._subscribeToTask(taskId, stage, cursor, null);
      }
    },

    async runPipeline() {
      if (this.running) return;
      const institutionId = this.$store.ctx.institutionId;
      const periodId      = this.$store.ctx.periodId;

      this.running = true;
      this.logLines = [];
      this.stageStatuses = {};
      this.stageResults = {};
      this.selectedStages.forEach(id => { this.stageStatuses[id] = 'idle'; });

      for (const stageId of this.selectedStages) {
        const stageInfo = this.stageById(stageId);
        this.currentStageLabel = stageInfo?.label || stageId;
        this.stageStatuses[stageId] = 'running';
        this.logLines.push(`─── ${(stageInfo?.label || stageId).toUpperCase()} ───`);
        this.$nextTick(() => this.scrollLog());

        try {
          if (stageId === 'LOAD_DRIVE_ZIP' && this.zipFile) {
            this.logLines.push(`[INFO] Descomprimiendo ${this.zipFile.name}...`);
            const fd = new FormData();
            fd.append('institution_id', institutionId);
            fd.append('period_id', periodId);
            fd.append('file', this.zipFile);
            const res = await api.post('/api/pipeline/load-drive-zip', fd);
            if (!res.ok) {
              const errMsg = res.status === 0
                ? 'No se pudo subir el archivo. Verifica la conexión o que el servidor esté disponible.'
                : res.status === 413
                  ? 'El archivo excede el límite de tamaño permitido por el servidor'
                  : res.error;
              const msg = `[ERROR] ${errMsg}`;
              this.logLines.push(msg); this._markLine(stageId, msg); this.scrollLog(); continue;
            }
            const data = res.data;
            const okMsg = `[OK] Archivos extraídos en DRIVE: ${data.extracted}`;
            this.logLines.push(okMsg);
            this._updateStageResult(stageId, okMsg);
            this.scrollLog();

          } else if (stageId === 'LOAD_AND_PROCESS' && this.excelFile) {
            const buildForm = (scanOnly) => {
              const fd = new FormData();
              fd.append('institution_id', institutionId);
              fd.append('period_id', periodId);
              fd.append('file', this.excelFile);
              if (scanOnly) fd.append('scan_only', 'true');
              return fd;
            };

            this.logLines.push('[INFO] Verificando mapeos del Excel...');
            const scanRes = await api.post('/api/invoices/ingest', buildForm(true));
            if (!scanRes.ok) {
              const msg = `[ERROR] ${scanRes.error}`;
              this.logLines.push(msg); this._markLine(stageId, msg); this.scrollLog(); continue;
            }
            const scan = scanRes.data;

            const hasUnmapped = scan.unknown_admins?.length || scan.unknown_contracts?.length || scan.unknown_services?.length;
            if (hasUnmapped) {
              const action = await new Promise(resolve => {
                this.unmappedConfirm = {
                  admins: scan.unknown_admins || [],
                  contracts: scan.unknown_contracts || [],
                  services: scan.unknown_services || [],
                  resolve: (v) => { this.unmappedConfirm = null; resolve(v); },
                };
              });

              if (action === 'cancel') {
                const msg = '[WARN] Ingestión cancelada — corrige los mapeos y vuelve a ejecutar.';
                this.logLines.push(msg); this._markLine(stageId, msg); this.scrollLog(); continue;
              }

              if (action === 'save') {
                const fd = new FormData();
                fd.append('institution_id', institutionId);
                fd.append('period_id', periodId);
                fd.append('file', this.excelFile);
                fd.append('save_mappings_only', 'true');
                const saveRes = await api.post('/api/invoices/ingest', fd);
                if (saveRes.ok) {
                  const m = '[OK] Registros sin mapear guardados. Ve a Configuración → Mapeos para asignarlos, luego vuelve a cargar el Excel.';
                  this.logLines.push(m); this._markLine(stageId, m);
                } else {
                  const m = `[ERROR] ${saveRes.error}`;
                  this.logLines.push(m); this._markLine(stageId, m);
                }
                this.scrollLog(); continue;
              }

              // action === 'continue'
              this.logLines.push('[INFO] Continuando con la carga de facturas...');
            } else {
              this.logLines.push('[INFO] Todos los mapeos completos, cargando...');
            }

            const res = await api.post('/api/invoices/ingest', buildForm(false));
            if (!res.ok) {
              const msg = `[ERROR] ${res.error}`;
              this.logLines.push(msg); this._markLine(stageId, msg); this.scrollLog(); continue;
            }
            const data = res.data;
            const okMsg = `[OK] Facturas insertadas: ${data.inserted}`;
            this.logLines.push(okMsg);
            this._updateStageResult(stageId, okMsg);
            if (data.skipped) this.logLines.push(`[INFO] Filas omitidas: ${data.skipped}`);
            if (data.unknown_admins?.length)    { const m=`[WARN] Admins sin mapear: ${data.unknown_admins.join(', ')}`; this.logLines.push(m); this._markLine(stageId,m); }
            if (data.unknown_contracts?.length) { const m=`[WARN] Contratos sin mapear: ${data.unknown_contracts.join(', ')}`; this.logLines.push(m); this._markLine(stageId,m); }
            if (data.unknown_services?.length)  { const m=`[WARN] Servicios sin mapear: ${data.unknown_services.join(', ')}`; this.logLines.push(m); this._markLine(stageId,m); }
            this.scrollLog();

          } else if (stageId === 'CHECK_FOLDERS_WITH_EXTRA_TEXT') {
            // Sync SSE — sends folder list, auditor decides per-folder action
            await this._runSyncStage(stageId, institutionId, periodId, stageId, (folders) => {
              this.annulData = folders.map(folder => ({ ...folder, action: folder.action || 'skip' }));
            });

          } else if (stageId === 'REVISAR_SOBRANTES') {
            // Sync SSE — blocks waiting for user interaction via [DATA] events
            await this._runSyncStage(stageId, institutionId, periodId, stageInfo?.label || stageId, (invoices) => {
              this.surplusData = invoices.map(inv => ({
                ...inv,
                sobrantes: inv.sobrantes.map(s => ({
                  ...s,
                  selected_doc_type_id: s.suggested_doc_type_id || '',
                  _done: false,
                  _new_filename: null,
                  _deleted: false,
                })),
              }));
            });

          } else if (stageId === 'REMOVE_NON_PDF') {
            await this._runSyncStage(stageId, institutionId, periodId, stageId, (payload) => {
              this.nonPdfData = {
                ...payload,
                non_pdf: payload.non_pdf.map(f => ({ ...f, action: f.is_image ? 'convert' : 'delete', _done: false })),
                corrupt_pdfs: payload.corrupt_pdfs.map(f => ({ ...f, action: 'delete', _done: false })),
              };
            });

          } else {
            // Background stage — runs independently of the HTTP connection
            await this._runBackgroundStage(stageId, institutionId, periodId);
          }
        } catch (e) {
          const msg = `[ERROR] ${e.message}`;
          this.logLines.push(msg); this._markLine(stageId, msg);
        }

        if (this.stageStatuses[stageId] === 'running') this.stageStatuses[stageId] = 'done';
        this.scrollLog();
      }

      this.running = false;
      this.currentStageLabel = '';
      this.logLines.push('─── PIPELINE FINALIZADO ───');
      this.scrollLog();
    },

    async applyAnnulDecisions() {
      if (!this.annulData || this.annulData.length === 0) return;
      const toAnnul = this.annulData.filter(f => f.action === 'annul');
      if (toAnnul.length === 0) {
        this.annulData = null;
        return;
      }
      const institutionId = this.$store.ctx.institutionId;
      const periodId = this.$store.ctx.periodId;
      const res = await api.post('/api/invoices/annul-folders', {
        institution_id: parseInt(institutionId),
        period_id: parseInt(periodId),
        decisions: this.annulData.map(f => ({
          folder_path: f.folder_path,
          invoice_id: f.invoice_id,
          action: f.action,
        })),
      });
      if (res.ok) {
        const data = res.data;
        const msg = `[INFO] Anuladas: ${data.annulled} carpeta(s), omitidas: ${data.skipped}`;
        this.logLines.push(msg);
        if (data.errors?.length) {
          data.errors.forEach(e => this.logLines.push(`[WARN] ${e}`));
        }
        this.scrollLog();
        this.annulData = null;
      } else {
        this.showToast(false, 'Error al aplicar decisiones: ' + res.error);
      }
    },

    async applyDelete(inv, sob) {
      this.confirmDeleteSurplus(inv, sob);
    },

    async applyRename(inv, sob) {
      const docTypeId = parseInt(sob.selected_doc_type_id);
      if (!docTypeId) return;
      const res = await api.post(`/api/invoices/${inv.invoice_id}/rename-surplus`, {
        folder_path: inv.folder_path,
        old_filename: sob.filename,
        doc_type_id: docTypeId,
      });
      if (res.ok) {
        sob._new_filename = res.data.new_filename;
        sob._done = true;
      } else {
        this.showToast(false, res.error);
      }
    },

    async confirmNonPdf() {
      const institutionId = this.$store.ctx.institutionId;
      const periodId      = this.$store.ctx.periodId;

      const decisions = [
        ...(this.nonPdfData?.non_pdf || []),
        ...(this.nonPdfData?.corrupt_pdfs || []),
      ].filter(f => !f._done).map(f => ({ rel_path: f.rel_path, action: f.action }));

      if (!decisions.length) { this.nonPdfData = null; return; }

      const res = await api.post('/api/pipeline/process-non-pdf', {
        institution_id: institutionId, period_id: periodId, decisions,
      });

      if (res.ok) {
        const result = res.data;
        [...(this.nonPdfData.non_pdf), ...(this.nonPdfData.corrupt_pdfs)]
          .forEach(f => { if (!f._done) f._done = true; });
        const summary = `[OK] No-PDF: ${result.deleted} eliminados, ${result.converted} convertidos a PDF` +
                        (result.errors.length ? ` — ${result.errors.length} error(es)` : '');
        this.logLines.push(summary);
        this._markLine('REMOVE_NON_PDF', summary);
        this.$nextTick(() => this.scrollLog());
        if (!result.errors.length) this.nonPdfData = null;
      } else {
        this.logLines.push(`[ERROR] ${res.error}`);
        this.$nextTick(() => this.scrollLog());
      }
    },

    scrollLog() {
      const el = this.$refs.logContainer;
      if (el) el.scrollTop = el.scrollHeight;
    },

    // Thin wrapper over the shared store (static/js/toast.js).
    showToast(ok, message) {
      Alpine.store('toast').legacy(ok, message);
    },

    confirmDeleteSurplus(inv, sob) {
      this.deleteConfirmPending = { inv, sob };
    },

    async executeDeleteSurplus() {
      const { inv, sob } = this.deleteConfirmPending;
      this.deleteConfirmPending = null;
      const res = await api.post(`/api/invoices/${inv.invoice_id}/delete-surplus`, {
        folder_path: inv.folder_path, filename: sob.filename,
      });
      if (res.ok) {
        sob._deleted = true;
        sob._done = true;
      } else {
        this.showToast(false, res.error);
      }
    },
  };
}
