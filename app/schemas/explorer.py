"""Pydantic schemas para el explorador de archivos PDF."""

from __future__ import annotations

from pydantic import BaseModel


class FileNode(BaseModel):
    name: str
    path: str  # relativo al sandbox, ej: "DRIVE/sub/doc.pdf"
    is_dir: bool
    size: int | None = None        # bytes para archivos
    file_count: int | None = None  # total de archivos (recursivo) para carpetas


class ListResponse(BaseModel):
    entries: list[FileNode]
    current_path: str


class RenameRequest(BaseModel):
    institution_id: int
    period_id: int
    path: str
    new_name: str


class MoveRequest(BaseModel):
    institution_id: int
    period_id: int
    src: str
    dst_folder: str


class MergeRequest(BaseModel):
    institution_id: int
    period_id: int
    paths: list[str]  # PDFs a unir, en orden
    output_name: str  # nombre del PDF resultante


class SplitRequest(BaseModel):
    institution_id: int
    period_id: int
    path: str
    ranges: str | None = None  # "1-3, 5" o None para página a página


class SplitPreviewResult(BaseModel):
    groups: list[list[int]]  # índices 0-based por parte, en el orden en que se generarían los archivos


class ReorderRequest(BaseModel):
    institution_id: int
    period_id: int
    path: str
    page_order: list[int]  # índices 0-based a conservar, en el orden deseado; los no incluidos se eliminan


class CopyRequest(BaseModel):
    institution_id: int
    period_id: int
    src: str
    dst_folder: str


class DeleteRequest(BaseModel):
    institution_id: int
    period_id: int
    path: str


class DeleteBatchRequest(BaseModel):
    institution_id: int
    period_id: int
    paths: list[str]


class BatchDeleteResult(BaseModel):
    deleted: list[str]
    errors: list[str]


class MkdirRequest(BaseModel):
    institution_id: int
    period_id: int
    path: str = ""  # carpeta padre relativa al sandbox (vacío = raíz)
    name: str  # nombre de la nueva carpeta


class UploadResult(BaseModel):
    uploaded: list[str]
    skipped: list[str]
    non_pdf_count: int = 0  # archivos omitidos por no ser PDF


class OperationResult(BaseModel):
    ok: bool
    message: str
