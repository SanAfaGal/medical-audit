const OFFICE_SCRIPT = `function main(workbook: ExcelScript.Workbook): void {
    const sheet = workbook.getActiveWorksheet();

    // ─── 1. Eliminar primeras 11 filas ───────────────────────────────────────────
    // Única operación de delete necesaria — no hay forma de evitarla.
    const initUsed = sheet.getUsedRange();
    sheet.getRangeByIndexes(0, 0, 11, initUsed.getColumnCount()).delete(
        ExcelScript.DeleteShiftDirection.up
    );

    // ─── 2. Lectura única de toda la hoja ────────────────────────────────────────
    // Se lee TODO de una sola vez. El resto del script opera en memoria
    // y solo escribe al final, minimizando viajes a Excel.
    const fullRange = sheet.getUsedRange();
    const totalRows = fullRange.getRowCount();
    const totalCols = fullRange.getColumnCount();
    let data = fullRange.getValues();

    // ─── 3. Separar celda combinada "Paciente" y nombrar sub-columnas ────────────
    // El unmerge es necesario hacerlo en Excel (no se puede en memoria),
    // pero solo se ejecuta una vez sobre la fila de encabezados.
    sheet.getRangeByIndexes(0, 0, 1, totalCols).unmerge();

    // Localizar "Paciente" en los encabezados y asignar sub-columnas por posición
    let pacienteSourceIdx = -1;
    for (let c = 0; c < totalCols; c++) {
        if (String(data[0][c]).trim().toLowerCase().includes("paciente")) {
            pacienteSourceIdx = c;
            break;
        }
    }
    if (pacienteSourceIdx >= 0) {
        data[0][pacienteSourceIdx] = "Documento";
        data[0][pacienteSourceIdx + 1] = "Numero";
        data[0][pacienteSourceIdx + 2] = "Paciente";
    }

    // ─── 4. Renombrar encabezados en memoria ─────────────────────────────────────
    const renameMap: { [key: string]: string } = {
        "num factura": "Factura",
        "admisión": "Admision",
        "tipo usuario": "Contrato",
        "tipo servicio": "Servicio",
        "actividad/suministro": "Actividad",
        "operario": "Operario",
        "paciente": "Paciente",
        "fecha": "Fecha",
        "administradora": "Administradora",
        "documento": "Documento",
        "numero": "Numero",
    };
    for (let c = 0; c < totalCols; c++) {
        const normalized = String(data[0][c]).trim().toLowerCase();
        if (renameMap[normalized] !== undefined) {
            data[0][c] = renameMap[normalized];
        }
    }

    // ─── 5. Identificar columnas a conservar por nombre ──────────────────────────
    const headersToKeep: string[] = [
        "Fecha", "Factura", "Admision", "Administradora",
        "Contrato", "Servicio", "Actividad", "Operario",
        "Documento", "Numero", "Paciente",
    ];

    // Construir mapa de columnas a conservar y sus índices destino
    const keepColIndices: number[] = [];
    for (let c = 0; c < totalCols; c++) {
        const hVal = String(data[0][c]).trim();
        if (headersToKeep.includes(hVal)) keepColIndices.push(c);
    }

    // Filtrar columnas en memoria: conservar solo las necesarias
    const dataFiltered: (string | number | boolean)[][] = [];
    for (let r = 0; r < totalRows; r++) {
        const newRow: (string | number | boolean)[] = [];
        for (const c of keepColIndices) {
            newRow.push(data[r][c] as string | number | boolean);
        }
        dataFiltered.push(newRow);
    }
    data = dataFiltered;
    const filteredCols = keepColIndices.length;

    // ─── 6. Eliminar filas donde Factura sea 0 o vacía ───────────────────────────
    // Se hace filtrando la matriz en memoria — sin ningún delete() en loop.
    let facturaColIdx = -1;
    for (let c = 0; c < filteredCols; c++) {
        if (String(data[0][c]).trim() === "Factura") { facturaColIdx = c; break; }
    }

    const dataNoEmpty: (string | number | boolean)[][] = [data[0]]; // conservar encabezado
    for (let r = 1; r < data.length; r++) {
        const cellVal = data[r][facturaColIdx];
        const isEmpty = cellVal === null || String(cellVal).trim() === "";
        const isZero = !isEmpty && (Number(cellVal) === 0 || String(cellVal).trim() === "0");
        if (!isEmpty && !isZero) dataNoEmpty.push(data[r]);
    }
    data = dataNoEmpty;

    // ─── 7. Convertir todo a mayúsculas en memoria ───────────────────────────────
    for (let r = 0; r < data.length; r++) {
        for (let c = 0; c < data[r].length; c++) {
            const v = data[r][c];
            if (typeof v === "string" && v.trim() !== "") {
                data[r][c] = v.toUpperCase();
            }
        }
    }

    // ─── 8. Normalizar "TOMA Y*" → "TOMA Y ENVIO" en columna SERVICIO ────────────
    let actividadIdx = -1;
    let servicioIdx = -1;
    for (let c = 0; c < data[0].length; c++) {
        const h = String(data[0][c]).trim();
        if (h === "ACTIVIDAD") actividadIdx = c;
        if (h === "SERVICIO") servicioIdx = c;
    }
    if (actividadIdx >= 0 && servicioIdx >= 0) {
        for (let r = 1; r < data.length; r++) {
            if (String(data[r][actividadIdx]).includes("TOMA Y")) {
                data[r][servicioIdx] = "TOMA Y ENVIO";
            }
        }
    }

    // ─── 9. Reordenar columnas en memoria ─────────────────────────────────────────
    const desiredOrder: string[] = [
        "FACTURA", "ADMISION", "FECHA", "DOCUMENTO", "NUMERO",
        "PACIENTE", "ADMINISTRADORA", "CONTRATO", "SERVICIO",
        "ACTIVIDAD", "OPERARIO",
    ];

    const headerIndexMap: { [key: string]: number } = {};
    for (let c = 0; c < data[0].length; c++) {
        headerIndexMap[String(data[0][c]).trim()] = c;
    }

    const dataReordered: (string | number | boolean)[][] = [];
    for (let r = 0; r < data.length; r++) {
        const newRow: (string | number | boolean)[] = [];
        for (const colName of desiredOrder) {
            const srcCol = headerIndexMap[colName];
            newRow.push(srcCol !== undefined ? data[r][srcCol] : "");
        }
        dataReordered.push(newRow);
    }
    data = dataReordered;

    // ─── 10. ESCRITURA ÚNICA en Excel ────────────────────────────────────────────
    // Limpiar hoja y escribir toda la matriz de una sola vez.
    sheet.getUsedRange().clear(ExcelScript.ClearApplyTo.all);
    const outputRows = data.length;
    const outputCols = desiredOrder.length;
    sheet.getRangeByIndexes(0, 0, outputRows, outputCols).setValues(data);

    // ─── 11. Formatos de columna ─────────────────────────────────────────────────
    if (outputRows > 1) {
        const fechaIdx = desiredOrder.indexOf("FECHA");
        const admisionIdx = desiredOrder.indexOf("ADMISION");

        if (fechaIdx >= 0) {
            sheet.getRangeByIndexes(1, fechaIdx, outputRows - 1, 1)
                .setNumberFormat("dd/mm/yyyy");
        }
        if (admisionIdx >= 0) {
            sheet.getRangeByIndexes(1, admisionIdx, outputRows - 1, 1)
                .setNumberFormat("0");
        }
    }

    // ─── 12. Alineación ──────────────────────────────────────────────────────────
    const finalUsed = sheet.getUsedRange();
    finalUsed.getFormat().setVerticalAlignment(ExcelScript.VerticalAlignment.center);
    finalUsed.getFormat().setHorizontalAlignment(ExcelScript.HorizontalAlignment.center);

    // ─── 13. Formato de tabla ─────────────────────────────────────────────────────
    const existingTable = workbook.getTable("TblFacturas");
    if (existingTable) existingTable.delete();

    const table = sheet.addTable(
        sheet.getRangeByIndexes(0, 0, outputRows, outputCols),
        true
    );
    table.setName("TblFacturas");
    table.setPredefinedTableStyle("TableStyleMedium2");

    // ─── 14. Autofit ──────────────────────────────────────────────────────────────
    sheet.getUsedRange().getFormat().autofitColumns();
    sheet.getUsedRange().getFormat().autofitRows();
}`;

function downloadScript() {
    const blob = new Blob([OFFICE_SCRIPT], { type: 'text/plain' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = 'preparar_sihos.ts';
    a.click();
    URL.revokeObjectURL(url);
}
