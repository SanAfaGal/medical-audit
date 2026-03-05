"""Data ingestion and normalisation pipeline for SIHOS invoice reports."""

import logging
from pathlib import Path

import pandas as pd

from core.helpers import save_dataframe

logger = logging.getLogger(__name__)


class BillingIngester:
    """Handles ingestion and normalisation of SIHOS Excel invoice reports.

    Args:
        admin_contract_map: Mapping of raw (admin, contract) pairs to their
            canonical counterparts.
    """

    def __init__(
        self,
        admin_contract_map: dict[tuple[str, str], tuple[str, str | None]],
    ) -> None:
        self._raw_df: pd.DataFrame | None = None
        self._processed_df: pd.DataFrame | None = None
        self.admin_contract_map = admin_contract_map

    def load_excel(self, file_path: Path, columns: list[str]) -> None:
        """Load the SIHOS Excel report into memory.

        Args:
            file_path: Path to the ``.xlsx`` report file.
            columns: Column names to load.

        Raises:
            FileNotFoundError: If the file does not exist.
        """
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError("File not found: %s" % path)

        self._raw_df = pd.read_excel(path, usecols=columns, dtype=str)
        logger.info("File loaded: %d rows detected.", len(self._raw_df))

    def validate_admin_contract_pairs(self) -> bool:
        """Audit the raw data for unmapped administrator/contract pairs.

        Returns:
            True if all (admin, contract) pairs are present in the mapping.

        Raises:
            ValueError: If no data has been loaded yet.
        """
        if self._raw_df is None:
            raise ValueError("No data loaded — call load_excel first.")

        raw_pairs: set[tuple[str, str]] = set(
            zip(
                self._raw_df["Administradora"].dropna(),
                self._raw_df["Contrato"].dropna(),
            )
        )
        missing = raw_pairs - set(self.admin_contract_map.keys())
        self._log_mapping_report(missing)
        return len(missing) == 0

    def _log_mapping_report(self, missing_pairs: set[tuple[str, str]]) -> None:
        """Log the pre-audit mapping validation results.

        Args:
            missing_pairs: (admin, contract) pairs absent from the map.
        """
        logger.info("--- Pre-mapping analysis ---")
        if not missing_pairs:
            logger.info("All pairs are present in the mapping dictionary.")
        else:
            logger.warning("Unmapped pairs (%d):", len(missing_pairs))
            for admin, contract in sorted(missing_pairs):
                logger.warning("  Missing: admin=%s | contract=%s", admin, contract)

    def export_to_excel(self, df: pd.DataFrame, dest: Path, columns: list[str]) -> None:
        """Export the processed DataFrame to an Excel file.

        Args:
            df: DataFrame to export.
            dest: Destination path for the Excel report.
            columns: Columns to include in the export.
        """
        try:
            save_dataframe(df[columns], dest)
            logger.info("Excel report saved to %s", dest)
        except (PermissionError, OSError, ValueError, KeyError) as exc:
            logger.error("Failed to save Excel report: %s", exc)

    def _normalize_raw_rows(self) -> pd.DataFrame:
        """Produce the base working DataFrame from raw input.

        Returns:
            Cleaned DataFrame with normalised document numbers and a composite
            ``Factura`` column.
        """
        df = self._raw_df.dropna(subset=["Doc", "No Doc", "Administradora"]).copy()  # type: ignore[union-attr]
        df["No Doc"] = (
            pd.to_numeric(df["No Doc"], errors="coerce")
            .astype("Int64")
            .astype(str)
        )
        df["Doc"] = df["Doc"].str.strip().str.upper()
        df["Factura"] = df["Doc"] + df["No Doc"]
        return df

    def _apply_canonical_mapping(self, df: pd.DataFrame) -> pd.DataFrame:
        """Map raw (admin, contract) pairs to their canonical values.

        Args:
            df: DataFrame with raw ``Administradora`` and ``Contrato`` columns.

        Returns:
            DataFrame with normalised values in both columns.
        """
        resolved = df.apply(
            lambda row: self.admin_contract_map.get(
                (row["Administradora"], row["Contrato"]), (None, None)
            ),
            axis=1,
            result_type="expand",
        )
        df["Administradora"] = resolved[0]
        df["Contrato"] = resolved[1]
        return df

    def _compute_storage_paths(self, df: pd.DataFrame) -> pd.DataFrame:
        """Compute the relative filesystem path for each invoice row.

        Args:
            df: DataFrame containing ``Administradora``, ``Contrato``, and
                ``Factura`` columns.

        Returns:
            DataFrame with a new ``Ruta`` column holding the relative path.
        """
        from pathlib import PurePosixPath

        def build_path(row: pd.Series) -> str:
            path = PurePosixPath(str(row["Administradora"]))
            if pd.notna(row["Contrato"]):
                path = path / str(row["Contrato"])
            return str(path / str(row["Factura"]))

        df["Ruta"] = df.apply(build_path, axis=1)
        return df

    def build_invoice_dataframe(self) -> pd.DataFrame:
        """Run the full normalisation pipeline.

        Returns:
            Processed DataFrame indexed by invoice ID.

        Raises:
            ValueError: If no data has been loaded yet.
        """
        if self._raw_df is None:
            raise ValueError("No data loaded — call load_excel first.")

        df = self._normalize_raw_rows()
        df = self._apply_canonical_mapping(df)
        df = self._compute_storage_paths(df)
        df = df.dropna(subset=["Administradora", "Ruta"])
        df.set_index("Factura", inplace=True)

        self._processed_df = df
        return self._processed_df
