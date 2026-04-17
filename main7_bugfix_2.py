from runpy import run_path
from pathlib import Path

if __name__ == "__main__":
    run_path(str(Path(__file__).resolve().parent / "scanner" / "legacy_main_patched.py"), run_name="__main__")
