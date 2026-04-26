from runpy import run_path
from pathlib import Path

if __name__ == "__main__":
    here = Path(__file__).resolve().parent
    scanner_main = here / "scanner" / "legacy_main_patched.py"
    root_main = here / "legacy_main_patched.py"
    target = scanner_main if scanner_main.exists() else root_main
    run_path(str(target), run_name="__main__")
