import csv
import json
import logging
from pathlib import Path
from typing import Any, Dict, Iterable, List

from openpyxl import Workbook

logger = logging.getLogger(__name__)

class DataExporter:
    """
    Export a list of dictionaries into various formats.

    Supported formats:
      - json
      - jsonl
      - csv
      - html
      - xml
      - excel
    """

    SUPPORTED_FORMATS = {"json", "jsonl", "csv", "html", "xml", "excel"}

    def __init__(self, output_path: Path, fmt: str = "json") -> None:
        fmt = fmt.lower()
        if fmt not in self.SUPPORTED_FORMATS:
            raise ValueError(f"Unsupported export format: {fmt}")

        self.output_path = Path(output_path)
        self.fmt = fmt

    def export(self, records: List[Dict[str, Any]]) -> None:
        if not isinstance(records, list):
            raise TypeError("Records must be a list of dictionaries.")
        if records and not isinstance(records[0], dict):
            raise TypeError("Each record must be a dictionary.")

        self.output_path.parent.mkdir(parents=True, exist_ok=True)

        logger.info("Exporting %d record(s) as %s to %s", len(records), self.fmt, self.output_path)

        if self.fmt == "json":
            self._export_json(records)
        elif self.fmt == "jsonl":
            self._export_jsonl(records)
        elif self.fmt == "csv":
            self._export_csv(records)
        elif self.fmt == "html":
            self._export_html(records)
        elif self.fmt == "xml":
            self._export_xml(records)
        elif self.fmt == "excel":
            self._export_excel(records)
        else:
            raise RuntimeError(f"Format {self.fmt} not implemented")

    # ---- JSON ----
    def _export_json(self, records: List[Dict[str, Any]]) -> None:
        with self.output_path.open("w", encoding="utf-8") as f:
            json.dump(records, f, ensure_ascii=False, indent=2)

    # ---- JSONL ----
    def _export_jsonl(self, records: List[Dict[str, Any]]) -> None:
        with self.output_path.open("w", encoding="utf-8") as f:
            for rec in records:
                f.write(json.dumps(rec, ensure_ascii=False))
                f.write("\n")

    # ---- CSV ----
    def _export_csv(self, records: List[Dict[str, Any]]) -> None:
        if not records:
            # Create an empty CSV with no rows
            with self.output_path.open("w", encoding="utf-8", newline="") as f:
                writer = csv.writer(f)
                writer.writerow([])
            return

        # Build header with union of all keys
        header = self._collect_all_keys(records)

        with self.output_path.open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=header, extrasaction="ignore")
            writer.writeheader()
            for rec in records:
                flattened = self._flatten(rec)
                writer.writerow({k: flattened.get(k, "") for k in header})

    # ---- HTML ----
    def _export_html(self, records: List[Dict[str, Any]]) -> None:
        header = self._collect_all_keys(records)

        lines: List[str] = [
            "<!DOCTYPE html>",
            "<html>",
            "<head>",
            "<meta charset='utf-8'>",
            "<title>Snapchat Popular Accounts</title>",
            "<style>",
            "table { border-collapse: collapse; width: 100%; }",
            "th, td { border: 1px solid #ddd; padding: 8px; font-family: sans-serif; font-size: 14px; }",
            "th { background-color: #f2f2f2; text-align: left; }",
            "</style>",
            "</head>",
            "<body>",
            "<h1>Snapchat Popular Accounts</h1>",
            "<table>",
            "<thead>",
            "<tr>",
        ]

        for col in header:
            lines.append(f"<th>{self._escape_html(col)}</th>")
        lines.extend(["</tr>", "</thead>", "<tbody>"])

        for rec in records:
            flattened = self._flatten(rec)
            lines.append("<tr>")
            for col in header:
                value = flattened.get(col, "")
                lines.append(f"<td>{self._escape_html(str(value))}</td>")
            lines.append("</tr>")

        lines.extend(["</tbody>", "</table>", "</body>", "</html>"])

        with self.output_path.open("w", encoding="utf-8") as f:
            f.write("\n".join(lines))

    # ---- XML ----
    def _export_xml(self, records: List[Dict[str, Any]]) -> None:
        lines: List[str] = ["<?xml version='1.0' encoding='UTF-8'?>", "<profiles>"]

        for rec in records:
            flattened = self._flatten(rec)
            lines.append("  <profile>")
            for key, value in flattened.items():
                key_safe = self._sanitize_xml_tag(key)
                value_safe = self._escape_xml(str(value))
                lines.append(f"    <{key_safe}>{value_safe}</{key_safe}>")
            lines.append("  </profile>")

        lines.append("</profiles>")

        with self.output_path.open("w", encoding="utf-8") as f:
            f.write("\n".join(lines))

    # ---- Excel (.xlsx) ----
    def _export_excel(self, records: List[Dict[str, Any]]) -> None:
        wb = Workbook()
        ws = wb.active
        ws.title = "Profiles"

        header = self._collect_all_keys(records)
        ws.append(list(header))

        for rec in records:
            flattened = self._flatten(rec)
            row = [flattened.get(col, "") for col in header]
            ws.append(row)

        # Ensure parent directories exist (already done in export()).
        wb.save(str(self.output_path))

    # ---- Helpers ----
    @staticmethod
    def _collect_all_keys(records: Iterable[Dict[str, Any]]) -> List[str]:
        keys = set()
        for rec in records:
            flat = DataExporter._flatten(rec)
            keys.update(flat.keys())
        return sorted(keys)

    @staticmethod
    def _flatten(rec: Dict[str, Any], parent_key: str = "", sep: str = ".") -> Dict[str, Any]:
        """
        Flatten nested dictionaries using dot notation.

        Example:
            {"location": {"country": "US"}} -> {"location.country": "US"}
        """
        items: Dict[str, Any] = {}
        for k, v in rec.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.update(DataExporter._flatten(v, new_key, sep=sep))
            else:
                items[new_key] = v
        return items

    @staticmethod
    def _escape_html(text: str) -> str:
        return (
            text.replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace('"', "&quot;")
        )

    @staticmethod
    def _sanitize_xml_tag(tag: str) -> str:
        # Basic sanitization to ensure a valid XML tag
        safe = "".join(ch if ch.isalnum() or ch in ("_", "-") else "_" for ch in tag)
        if safe and safe[0].isdigit():
            safe = f"f_{safe}"
        return safe or "field"

    @staticmethod
    def _escape_xml(text: str) -> str:
        return (
            text.replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace('"', "&quot;")
            .replace("'", "&apos;")
        )