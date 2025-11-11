import json
import logging
import os
import sys
from pathlib import Path
from typing import List, Dict, Any, Optional

from extractors.snapchat_parser import SnapchatParser
from outputs.exporters import DataExporter

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "src" / "config" / "settings.json"
DEFAULT_CONFIG_FALLBACK_PATH = PROJECT_ROOT / "src" / "config" / "settings.example.json"
DEFAULT_INPUTS_PATH = PROJECT_ROOT / "data" / "inputs.sample.txt"

def load_config(explicit_path: Optional[str] = None) -> Dict[str, Any]:
    """
    Load configuration JSON.

    Priority:
    1. Explicit path argument
    2. ENV: SNAPCHAT_SCRAPER_CONFIG