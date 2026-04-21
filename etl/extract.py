"""
Extract module. Reads the active Tycho CSV plus all reference data files
and returns pandas DataFrames for downstream transformation.

The active dataset filename is configurable through the environment
variable TYCHO_DATA_FILE (defaults to tycho_level1.csv). This allows the
same ETL pipeline to ingest either:

    * the real Project Tycho Level 1 export (tycho_level1_real.csv), or
    * the synthetic benchmark produced by generate_synthetic_data.py
      (tycho_level1_synthetic.csv).

A small bootstrap copies whichever source is present into
data/raw/tycho_level1.csv when the active dataset is missing, so the
container does not require the user to edit code paths.
"""

import os
import shutil
from dataclasses import dataclass

import pandas as pd


@dataclass
class RawData:
    tycho: pd.DataFrame
    regions: pd.DataFrame
    diseases: pd.DataFrame
    borders: pd.DataFrame
    populations: pd.DataFrame
    source_label: str


def _resolve_active_csv(raw_dir):
    """Pick which raw CSV to load and ensure it is present at the canonical name."""
    canonical = os.path.join(raw_dir, os.environ.get("TYCHO_DATA_FILE", "tycho_level1.csv"))
    if os.path.exists(canonical):
        return canonical, _label_from_filename(canonical)

    fallbacks = [
        os.path.join(raw_dir, "tycho_level1_real.csv"),
        os.path.join(raw_dir, "tycho_level1_synthetic.csv"),
    ]
    for fb in fallbacks:
        if os.path.exists(fb):
            shutil.copyfile(fb, canonical)
            print("Bootstrapped %s from %s" % (canonical, fb))
            return canonical, _label_from_filename(fb)

    raise FileNotFoundError(
        "No Tycho CSV found in %s. Expected tycho_level1.csv, "
        "tycho_level1_real.csv, or tycho_level1_synthetic.csv." % raw_dir
    )


def _label_from_filename(path):
    name = os.path.basename(path).lower()
    if "real" in name:
        return "real_tycho_level1"
    if "synth" in name:
        return "synthetic"
    return "unspecified"


def extract_all(raw_dir, ref_dir):
    """Read all source data files and return as DataFrames."""

    tycho_path, source_label = _resolve_active_csv(raw_dir)
    print("Reading raw data from %s (source label %s)" % (tycho_path, source_label))
    tycho = pd.read_csv(tycho_path)
    print("  Raw rows: %d" % len(tycho))

    regions = pd.read_csv(os.path.join(ref_dir, "us_regions.csv"))
    print("  Regions: %d states" % len(regions))

    diseases = pd.read_csv(os.path.join(ref_dir, "disease_metadata.csv"))
    print("  Diseases: %d entries" % len(diseases))

    borders = pd.read_csv(os.path.join(ref_dir, "state_borders.csv"))
    print("  Borders: %d entries" % len(borders))

    populations = pd.read_csv(os.path.join(ref_dir, "state_populations.csv"))
    print("  Populations: %d entries" % len(populations))

    return RawData(
        tycho=tycho,
        regions=regions,
        diseases=diseases,
        borders=borders,
        populations=populations,
        source_label=source_label,
    )


if __name__ == "__main__":
    base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    raw = extract_all(
        os.path.join(base, "data", "raw"),
        os.path.join(base, "data", "reference"),
    )
    print("\nExtraction complete. Tycho shape: %s" % str(raw.tycho.shape))
