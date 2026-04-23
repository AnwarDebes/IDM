"""
Query registry — maps query IDs to metadata and backend service methods.
"""

QUERY_REGISTRY = {
    "Q1": {
        "name": "Total cases by disease and decade",
        "description": "Roll-up from weekly to decade granularity. Aggregates all case counts per disease per decade.",
        "olap_operation": "Roll-up",
        "backends": ["postgres", "mongodb", "neo4j"],
        "params": [],
    },
    "Q2": {
        "name": "Measles incidence by state for a specific year",
        "description": "Slice on disease=MEASLES and a given year, showing state-level incidence.",
        "olap_operation": "Slice",
        "backends": ["postgres", "mongodb", "neo4j"],
        "params": [{"name": "year", "type": "int", "default": 1960}],
    },
    "Q3": {
        "name": "Top 10 states by total cases for disease and time range",
        "description": "Dice on disease and year range, returning top 10 states by case count.",
        "olap_operation": "Dice",
        "backends": ["postgres", "mongodb", "neo4j"],
        "params": [
            {"name": "disease", "type": "str", "default": "MEASLES"},
            {"name": "start_year", "type": "int", "default": 1950},
            {"name": "end_year", "type": "int", "default": 1970},
        ],
    },
    "Q4": {
        "name": "Seasonal pattern avg weekly cases per month",
        "description": "Roll-up + aggregation showing average weekly cases per calendar month for a disease.",
        "olap_operation": "Roll-up",
        "backends": ["postgres", "mongodb", "neo4j"],
        "params": [{"name": "disease", "type": "str", "default": "MEASLES"}],
    },
    "Q5": {
        "name": "Year-over-year change in incidence by state",
        "description": "Window function / pivot showing YoY percentage change per state for a disease.",
        "olap_operation": "Pivot",
        "backends": ["postgres", "mongodb", "neo4j"],
        "params": [{"name": "disease", "type": "str", "default": "MEASLES"}],
    },
    "Q6": {
        "name": "Disease co-occurrence by state and time",
        "description": "Dice + correlation analysis showing which diseases spike together across states and years.",
        "olap_operation": "Dice",
        "backends": ["postgres", "mongodb", "neo4j"],
        "params": [],
    },
    "Q7": {
        "name": "Geographic spread rank states by first report",
        "description": "Drill-down showing when each state first reported a disease, ranked chronologically.",
        "olap_operation": "Drill-down",
        "backends": ["postgres", "mongodb", "neo4j"],
        "params": [{"name": "disease", "type": "str", "default": "MEASLES"}],
    },
    "Q8": {
        "name": "Vaccination impact before/after comparison",
        "description": "Slice + aggregation comparing average annual cases 10 years before vs 10 years after vaccine introduction.",
        "olap_operation": "Slice",
        "backends": ["postgres", "mongodb", "neo4j"],
        "params": [],
    },
    "Q9": {
        "name": "Anomaly detection states > 2 std dev above mean",
        "description": "Statistical analysis finding state-year combinations with cases more than 2 standard deviations above national mean.",
        "olap_operation": "Statistical",
        "backends": ["postgres", "mongodb", "neo4j"],
        "params": [],
    },
    "Q10": {
        "name": "Cross-disease normalized trend comparison",
        "description": "Pivot + normalization showing each disease's annual cases as a percentage of its historical maximum.",
        "olap_operation": "Pivot",
        "backends": ["postgres", "mongodb", "neo4j"],
        "params": [],
    },
    "Q11": {
        "name": "Disease spread by state borders (graph-exclusive)",
        "description": "Graph traversal analyzing disease spread patterns between neighboring states via BORDERS relationships.",
        "olap_operation": "Graph Traversal",
        "backends": ["neo4j"],
        "params": [
            {"name": "disease", "type": "str", "default": "MEASLES"},
            {"name": "threshold", "type": "int", "default": 50},
        ],
    },
    "Q12": {
        "name": "State similarity by disease profile (graph-exclusive)",
        "description": "Graph pattern matching to compute disease composition profiles for each state.",
        "olap_operation": "Graph Pattern",
        "backends": ["neo4j"],
        "params": [],
    },
    "Q13": {
        "name": "Disease centrality and coverage (graph-exclusive)",
        "description": "Graph centrality analysis measuring each disease's presence across states and years.",
        "olap_operation": "Graph Centrality",
        "backends": ["neo4j"],
        "params": [],
    },
}


def get_query_method(query_id: str, backend: str):
    """Return the bound method for a query on a given backend service instance."""
    entry = QUERY_REGISTRY.get(query_id)
    if not entry:
        return None
    if backend not in entry["backends"]:
        return None
    method_name = query_id.lower()  # e.g. "Q1" -> "q1"
    return method_name
