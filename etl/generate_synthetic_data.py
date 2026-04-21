"""
Generate realistic synthetic epidemiological data aligned with the
Project Tycho Level 1 schema.

Design notes
    1. Time coverage mirrors the Project Tycho Level 1 dataset, which covers
       epi-weeks from 1916 through 2011. The default run generates all 96
       calendar years so the synthetic corpus can serve as a drop-in
       benchmark for the real data.
    2. Reference state populations are stored at ten-year grain. Values
       from 1930 onward use US Census decennial data. Values for 1910 and
       1920 are extrapolated linearly backward from the observed
       1930-1940 slope on a per-state basis, since per-state Census figures
       that far back are unreliable at small magnitudes and synthetic data
       does not require them to be exact. The extrapolation is clamped at
       a minimum of 30 thousand to avoid negative populations for small
       or newly admitted states.
    3. Disease parameters use Poisson-distributed weekly counts scaled by
       population, with sinusoidal seasonal modulation and exponential
       post-vaccine decay. Smallpox is treated specially because it was
       already in sharp decline before 1916.
    4. All ratios are per 100 000 inhabitants, matching CDC standard
       reporting.
"""

import csv
import math
import os
import random

import numpy as np

# Census-backed per-state populations in thousands.
# Values for the decades 1930 through 2010 come from the US Census Bureau
# decennial counts. Pre-1930 values are extrapolated, see module docstring.
REFERENCE_POPS = {
    "AL": {1930: 2646, 1940: 2833, 1950: 3062, 1960: 3267, 1970: 3444, 1980: 3894, 1990: 4041, 2000: 4447, 2010: 4779},
    "AK": {1930: 59,   1940: 73,   1950: 129,  1960: 226,  1970: 303,  1980: 402,  1990: 550,  2000: 627,  2010: 710},
    "AZ": {1930: 436,  1940: 499,  1950: 750,  1960: 1302, 1970: 1771, 1980: 2718, 1990: 3665, 2000: 5131, 2010: 6392},
    "AR": {1930: 1854, 1940: 1949, 1950: 1910, 1960: 1786, 1970: 1923, 1980: 2286, 1990: 2351, 2000: 2673, 2010: 2916},
    "CA": {1930: 5677, 1940: 6907, 1950: 10586,1960: 15717,1970: 19953,1980: 23668,1990: 29760,2000: 33872,2010: 37254},
    "CO": {1930: 1036, 1940: 1123, 1950: 1326, 1960: 1754, 1970: 2207, 1980: 2890, 1990: 3294, 2000: 4301, 2010: 5029},
    "CT": {1930: 1607, 1940: 1709, 1950: 2007, 1960: 2535, 1970: 3032, 1980: 3108, 1990: 3287, 2000: 3406, 2010: 3574},
    "DE": {1930: 238,  1940: 267,  1950: 318,  1960: 446,  1970: 548,  1980: 594,  1990: 666,  2000: 784,  2010: 897},
    "FL": {1930: 1468, 1940: 1897, 1950: 2771, 1960: 4952, 1970: 6789, 1980: 9747, 1990: 12938,2000: 15982,2010: 18801},
    "GA": {1930: 2909, 1940: 3124, 1950: 3445, 1960: 3943, 1970: 4590, 1980: 5463, 1990: 6478, 2000: 8186, 2010: 9688},
    "HI": {1930: 368,  1940: 423,  1950: 500,  1960: 633,  1970: 770,  1980: 965,  1990: 1108, 2000: 1212, 2010: 1360},
    "ID": {1930: 445,  1940: 525,  1950: 589,  1960: 667,  1970: 713,  1980: 944,  1990: 1007, 2000: 1294, 2010: 1568},
    "IL": {1930: 7631, 1940: 7897, 1950: 8712, 1960: 10081,1970: 11114,1980: 11427,1990: 11431,2000: 12419,2010: 12831},
    "IN": {1930: 3239, 1940: 3428, 1950: 3934, 1960: 4662, 1970: 5194, 1980: 5490, 1990: 5544, 2000: 6080, 2010: 6484},
    "IA": {1930: 2471, 1940: 2538, 1950: 2621, 1960: 2758, 1970: 2825, 1980: 2914, 1990: 2777, 2000: 2926, 2010: 3046},
    "KS": {1930: 1881, 1940: 1801, 1950: 1905, 1960: 2179, 1970: 2247, 1980: 2364, 1990: 2478, 2000: 2688, 2010: 2853},
    "KY": {1930: 2615, 1940: 2846, 1950: 2945, 1960: 3038, 1970: 3219, 1980: 3661, 1990: 3685, 2000: 4042, 2010: 4339},
    "LA": {1930: 2102, 1940: 2364, 1950: 2684, 1960: 3257, 1970: 3641, 1980: 4206, 1990: 4220, 2000: 4469, 2010: 4533},
    "ME": {1930: 797,  1940: 847,  1950: 914,  1960: 969,  1970: 993,  1980: 1125, 1990: 1228, 2000: 1275, 2010: 1328},
    "MD": {1930: 1632, 1940: 1821, 1950: 2343, 1960: 3101, 1970: 3922, 1980: 4217, 1990: 4781, 2000: 5296, 2010: 5774},
    "MA": {1930: 4250, 1940: 4317, 1950: 4691, 1960: 5149, 1970: 5689, 1980: 5737, 1990: 6016, 2000: 6349, 2010: 6548},
    "MI": {1930: 4842, 1940: 5256, 1950: 6372, 1960: 7823, 1970: 8882, 1980: 9262, 1990: 9295, 2000: 9938, 2010: 9884},
    "MN": {1930: 2564, 1940: 2792, 1950: 2982, 1960: 3414, 1970: 3805, 1980: 4076, 1990: 4375, 2000: 4919, 2010: 5304},
    "MS": {1930: 2010, 1940: 2184, 1950: 2179, 1960: 2178, 1970: 2217, 1980: 2521, 1990: 2573, 2000: 2845, 2010: 2967},
    "MO": {1930: 3629, 1940: 3785, 1950: 3955, 1960: 4320, 1970: 4677, 1980: 4917, 1990: 5117, 2000: 5595, 2010: 5989},
    "MT": {1930: 538,  1940: 559,  1950: 591,  1960: 675,  1970: 694,  1980: 787,  1990: 799,  2000: 902,  2010: 989},
    "NE": {1930: 1378, 1940: 1316, 1950: 1326, 1960: 1411, 1970: 1483, 1980: 1570, 1990: 1578, 2000: 1711, 2010: 1826},
    "NV": {1930: 91,   1940: 110,  1950: 160,  1960: 285,  1970: 489,  1980: 801,  1990: 1202, 2000: 1998, 2010: 2701},
    "NH": {1930: 465,  1940: 492,  1950: 533,  1960: 607,  1970: 738,  1980: 921,  1990: 1109, 2000: 1236, 2010: 1316},
    "NJ": {1930: 4041, 1940: 4160, 1950: 4835, 1960: 6067, 1970: 7168, 1980: 7365, 1990: 7730, 2000: 8414, 2010: 8792},
    "NM": {1930: 423,  1940: 532,  1950: 681,  1960: 951,  1970: 1016, 1980: 1303, 1990: 1515, 2000: 1819, 2010: 2059},
    "NY": {1930: 12588,1940: 13479,1950: 14830,1960: 16782,1970: 18237,1980: 17558,1990: 17990,2000: 18976,2010: 19378},
    "NC": {1930: 3170, 1940: 3572, 1950: 4062, 1960: 4556, 1970: 5082, 1980: 5882, 1990: 6629, 2000: 8049, 2010: 9535},
    "ND": {1930: 681,  1940: 642,  1950: 620,  1960: 632,  1970: 618,  1980: 653,  1990: 639,  2000: 642,  2010: 673},
    "OH": {1930: 6647, 1940: 6908, 1950: 7947, 1960: 9706, 1970: 10652,1980: 10798,1990: 10847,2000: 11353,2010: 11537},
    "OK": {1930: 2396, 1940: 2336, 1950: 2233, 1960: 2328, 1970: 2559, 1980: 3025, 1990: 3146, 2000: 3451, 2010: 3751},
    "OR": {1930: 954,  1940: 1090, 1950: 1521, 1960: 1769, 1970: 2091, 1980: 2633, 1990: 2842, 2000: 3421, 2010: 3831},
    "PA": {1930: 9631, 1940: 9900, 1950: 10498,1960: 11319,1970: 11794,1980: 11864,1990: 11882,2000: 12281,2010: 12702},
    "RI": {1930: 687,  1940: 713,  1950: 792,  1960: 859,  1970: 947,  1980: 947,  1990: 1003, 2000: 1048, 2010: 1053},
    "SC": {1930: 1739, 1940: 1900, 1950: 2117, 1960: 2383, 1970: 2591, 1980: 3122, 1990: 3487, 2000: 4012, 2010: 4625},
    "SD": {1930: 693,  1940: 643,  1950: 653,  1960: 681,  1970: 666,  1980: 691,  1990: 696,  2000: 755,  2010: 814},
    "TN": {1930: 2617, 1940: 2916, 1950: 3292, 1960: 3567, 1970: 3924, 1980: 4591, 1990: 4877, 2000: 5689, 2010: 6346},
    "TX": {1930: 5825, 1940: 6415, 1950: 7711, 1960: 9580, 1970: 11197,1980: 14229,1990: 16987,2000: 20852,2010: 25146},
    "UT": {1930: 508,  1940: 550,  1950: 689,  1960: 891,  1970: 1059, 1980: 1461, 1990: 1723, 2000: 2233, 2010: 2764},
    "VT": {1930: 360,  1940: 359,  1950: 378,  1960: 390,  1970: 445,  1980: 511,  1990: 563,  2000: 609,  2010: 626},
    "VA": {1930: 2422, 1940: 2678, 1950: 3319, 1960: 3967, 1970: 4648, 1980: 5347, 1990: 6187, 2000: 7079, 2010: 8001},
    "WA": {1930: 1563, 1940: 1736, 1950: 2379, 1960: 2853, 1970: 3409, 1980: 4132, 1990: 4867, 2000: 5894, 2010: 6724},
    "WV": {1930: 1729, 1940: 1902, 1950: 2006, 1960: 1860, 1970: 1744, 1980: 1950, 1990: 1793, 2000: 1808, 2010: 1853},
    "WI": {1930: 2939, 1940: 3138, 1950: 3435, 1960: 3952, 1970: 4418, 1980: 4706, 1990: 4892, 2000: 5364, 2010: 5687},
    "WY": {1930: 226,  1940: 251,  1950: 291,  1960: 330,  1970: 332,  1980: 470,  1990: 454,  2000: 494,  2010: 564},
}

# Extrapolate 1920 and 1910 backward from the 1930-1940 slope,
# clamped at 30 to keep all synthetic populations positive.
def _extrapolate_pre_1930():
    for state, pops in REFERENCE_POPS.items():
        slope_per_decade = pops[1940] - pops[1930]
        for decade in (1920, 1910):
            offset = (decade - 1930) // 10
            projected = pops[1930] + slope_per_decade * offset
            pops[decade] = max(30, int(projected))

_extrapolate_pre_1930()

STATES = list(REFERENCE_POPS.keys())
DECADES = [1910, 1920, 1930, 1940, 1950, 1960, 1970, 1980, 1990, 2000, 2010]

# Default coverage matches Project Tycho Level 1 (1916 through 2011).
DEFAULT_START_YEAR = 1916
DEFAULT_END_YEAR = 2011

# Disease parameters drive the Poisson mean for each weekly observation.
DISEASE_PARAMS = {
    "MEASLES":    {"base_rate": 4.0,  "seasonal_amp": 0.6, "peak_week": 12, "vaccine_year": 1963, "decay": 0.15},
    "PERTUSSIS":  {"base_rate": 1.5,  "seasonal_amp": 0.3, "peak_week": 28, "vaccine_year": 1948, "decay": 0.10},
    "POLIO":      {"base_rate": 0.8,  "seasonal_amp": 0.7, "peak_week": 32, "vaccine_year": 1955, "decay": 0.25},
    "DIPHTHERIA": {"base_rate": 1.2,  "seasonal_amp": 0.4, "peak_week": 8,  "vaccine_year": 1923, "decay": 0.08},
    "SMALLPOX":   {"base_rate": 0.5,  "seasonal_amp": 0.3, "peak_week": 10, "vaccine_year": 1796, "decay": 0.05},
    "HEPATITIS A":{"base_rate": 0.6,  "seasonal_amp": 0.2, "peak_week": 42, "vaccine_year": 1995, "decay": 0.12},
    "MUMPS":      {"base_rate": 1.0,  "seasonal_amp": 0.5, "peak_week": 14, "vaccine_year": 1967, "decay": 0.13},
    "RUBELLA":    {"base_rate": 0.9,  "seasonal_amp": 0.5, "peak_week": 16, "vaccine_year": 1969, "decay": 0.18},
}


def get_population(state, year):
    """Linear interpolation between known decennial populations."""
    pops = REFERENCE_POPS[state]
    decades = sorted(pops.keys())
    if year <= decades[0]:
        return pops[decades[0]] * 1000
    if year >= decades[-1]:
        return pops[decades[-1]] * 1000
    for i in range(len(decades) - 1):
        if decades[i] <= year < decades[i + 1]:
            frac = (year - decades[i]) / (decades[i + 1] - decades[i])
            pop = pops[decades[i]] + frac * (pops[decades[i + 1]] - pops[decades[i]])
            return int(pop * 1000)
    return pops[decades[-1]] * 1000


def compute_expected_cases(disease, state, year, week, baseline_year):
    """Expected Poisson mean for a disease, state, year, week."""
    params = DISEASE_PARAMS[disease]
    pop = get_population(state, year)

    # Base rate scales linearly with population.
    base = params["base_rate"] * (pop / 100000.0)

    # Sinusoidal seasonal modulation.
    seasonal = 1.0 + params["seasonal_amp"] * math.cos(
        2 * math.pi * (week - params["peak_week"]) / 52.0
    )

    # Post-vaccine exponential decline.
    vaccine_year = params["vaccine_year"]
    if disease == "SMALLPOX":
        if year >= 1949:
            vaccine_effect = 0.001
        elif year >= 1900:
            vaccine_effect = max(0.01, 1.0 - (year - 1900) * 0.018)
        else:
            vaccine_effect = 1.0
    elif year > vaccine_year:
        years_since = year - vaccine_year
        vaccine_effect = math.exp(-params["decay"] * years_since)
    else:
        vaccine_effect = 1.0

    # Slow global decline in baseline incidence as public health infrastructure improved.
    if year > vaccine_year:
        decade_factor = max(0.3, 1.0 - (year - baseline_year) * 0.003)
    else:
        decade_factor = 1.0

    expected = base * seasonal * vaccine_effect * decade_factor
    return max(0.0, expected)


def generate_data(output_path, start_year=DEFAULT_START_YEAR, end_year=DEFAULT_END_YEAR):
    """Generate the full synthetic dataset and write to CSV."""
    np.random.seed(42)
    random.seed(42)

    diseases = list(DISEASE_PARAMS.keys())
    rows = []
    row_count = 0

    for year in range(start_year, end_year + 1):
        # Years divisible by five get 53 epi-weeks, matching the Tycho convention.
        max_week = 53 if year % 5 == 0 else 52

        for week in range(1, max_week + 1):
            epi_week = year * 100 + week

            for state in STATES:
                pop = get_population(state, year)

                for disease in diseases:
                    expected = compute_expected_cases(
                        disease, state, year, week, baseline_year=start_year
                    )

                    if expected < 0.01:
                        continue

                    # Poisson-distributed actual cases.
                    cases = int(np.random.poisson(max(0.1, expected)))

                    if cases <= 0:
                        continue

                    incidence = round(cases * 100000.0 / pop, 4) if pop > 0 else 0.0

                    rows.append({
                        "epi_week": epi_week,
                        "state": state,
                        "loc": state,
                        "loc_type": "STATE",
                        "disease": disease,
                        "cases": cases,
                        "incidence_per_100000": incidence,
                    })
                    row_count += 1

        if year % 10 == 0:
            print("  Generated through year %d, %d rows so far" % (year, row_count))

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["epi_week", "state", "loc", "loc_type", "disease", "cases", "incidence_per_100000"],
        )
        writer.writeheader()
        writer.writerows(rows)

    print("\nGenerated %d rows to %s" % (row_count, output_path))
    return row_count


def generate_state_populations(output_path):
    """Write a long-format state_populations.csv covering every decade in DECADES."""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["state_code", "decade", "population", "source"])
        for state in STATES:
            for decade in DECADES:
                pop = REFERENCE_POPS[state][decade] * 1000
                source = "census" if decade >= 1930 else "extrapolated_from_1930_1940"
                writer.writerow([state, decade, pop, source])
    print("Generated state_populations.csv with %d rows" % (len(STATES) * len(DECADES)))


if __name__ == "__main__":
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_dir = os.path.join(base_dir, "data")

    print("Generating state populations reference data...")
    generate_state_populations(os.path.join(data_dir, "reference", "state_populations.csv"))

    print("\nGenerating synthetic Project Tycho dataset (1916 through 2011)...")
    generate_data(os.path.join(data_dir, "raw", "tycho_level1_synthetic.csv"))
