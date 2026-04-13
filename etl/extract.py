"""
Extract module: reads raw Tycho CSV and all reference data files.
Returns pandas DataFrames for downstream transformation.
"""

import os
from dataclasses import dataclass

import pandas as pd


@dataclass
class RawData:
    tycho: pd.DataFrame
    regions: pd.DataFrame
    diseases: pd.DataFrame
    borders: pd.DataFrame
    populations: pd.DataFrame


def extract_all(raw_dir: str, ref_dir: str) -> RawData:
    """Read all source data files and return as DataFrames."""

    tycho_path = os.path.join(raw_dir, "tycho_level1.csv")
    print(f"Reading raw data from {tycho_path}...")
    tycho = pd.read_csv(tycho_path)
    print(f"  Raw rows: {len(tycho):,}")

    regions = pd.read_csv(os.path.join(ref_dir, "us_regions.csv"))
    print(f"  Regions: {len(regions)} states")

    diseases = pd.read_csv(os.path.join(ref_dir, "disease_metadata.csv"))
    print(f"  Diseases: {len(diseases)} entries")

    borders = pd.read_csv(os.path.join(ref_dir, "state_borders.csv"))
    print(f"  Borders: {len(borders)} entries")

    populations = pd.read_csv(os.path.join(ref_dir, "state_populations.csv"))
    print(f"  Populations: {len(populations)} entries")

    return RawData(
        tycho=tycho,
        regions=regions,
        diseases=diseases,
        borders=borders,
        populations=populations,
    )


if __name__ == "__main__":
    base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    raw = extract_all(
        os.path.join(base, "data", "raw"),
        os.path.join(base, "data", "reference"),
    )
    print(f"\nExtraction complete. Tycho shape: {raw.tycho.shape}")
