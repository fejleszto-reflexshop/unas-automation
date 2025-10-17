import subprocess
import sys
import time
import logging
from pathlib import Path

# Paths
PYTHON = sys.executable
ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"
LOG_DIR = ROOT / "logs"
LOG_DIR.mkdir(exist_ok=True)

# Log file: logs/run_YYYY-MM-DD_HH-MM.log
log_file = LOG_DIR / f"run_{time.strftime('%Y-%m-%d_%H-%M')}.log"

# -----------------------------
#  LOGGING CONFIGURATION
# -----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    handlers=[
        logging.FileHandler(log_file, encoding="utf-8", mode='a'),
        logging.StreamHandler(sys.stdout),  # also show live output
    ],
)

# -----------------------------
#  PIPELINE STEPS
# -----------------------------
STEPS = [
    ("1/3 Selenium export", SRC / "download_data_selenium.py"),
    ("2/3 Modify Excel files", SRC / "handle_excel.py"),
    ("3/3 Upload to Google Cloud", SRC / "google_cloud_actions.py"),
]

def run_step(name: str, script: Path):
    if not script.exists():
        logging.error(f"❌ Missing script: {script}")
        sys.exit(1)

    logging.info("=" * 70)
    logging.info(f"▶ {name}")
    logging.info(f"   {script}")
    logging.info("=" * 70)
    t0 = time.time()

    try:
        # run and wait; capture output to keep logs cleaner
        result = subprocess.run(
            [PYTHON, str(script)],
            cwd=ROOT,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        dt = time.time() - t0

        if result.returncode == 0:
            logging.info(f"✅ Done {name} in {dt:.1f}s")
            if result.stdout.strip():
                logging.info(result.stdout)
        else:
            logging.error(f"❌ {name} failed (exit code {result.returncode}) in {dt:.1f}s")
            logging.error(result.stderr)
            sys.exit(result.returncode)
    except Exception as e:
        logging.exception(f"Unhandled error while running {name}: {e}")
        sys.exit(1)

def main():
    logging.info(f"Starting automation pipeline at {time.strftime('%Y-%m-%d %H:%M:%S')}")
    for name, script in STEPS:
        run_step(name, script)
    logging.info("🎉 ALL STEPS FINISHED SUCCESSFULLY")

if __name__ == "__main__":
    main()
