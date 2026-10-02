"""
Constants for transform operations.
"""

FAMILY_CONFIG_MAP = {
    "1 adult": {"adults": 1, "working_adults": 1, "children": 0},
    "1 adult 1 child": {"adults": 1, "working_adults": 1, "children": 1},
    "1 adult 2 children": {"adults": 1, "working_adults": 1, "children": 2},
    "1 adult 3 children": {"adults": 1, "working_adults": 1, "children": 3},
    "2 adults (1 working)": {"adults": 2, "working_adults": 1, "children": 0},
    "2 adults (1 working) 1 child": {"adults": 2, "working_adults": 1, "children": 1},
    "2 adults (1 working) 2 children": {"adults": 2, "working_adults": 1, "children": 2},
    "2 adults (1 working) 3 children": {"adults": 2, "working_adults": 1, "children": 3},
    "2 adults": {"adults": 2, "working_adults": 2, "children": 0},
    "2 adults 1 child": {"adults": 2, "working_adults": 2, "children": 1},
    "2 adults 2 children": {"adults": 2, "working_adults": 2, "children": 2},
    "2 adults 3 children": {"adults": 2, "working_adults": 2, "children": 3},
}

CATEGORY_MAP = {
    # Wage categories
    "living wage": "living",
    "poverty wage": "poverty",
    "minimum wage": "minimum",

    # Expense categories
    "food": "food",
    "child care": "childcare",
    "childcare": "childcare",
    "housing": "housing",
    "transportation": "transportation",
    "medical": "healthcare",
    "medical care": "healthcare",
    "health care": "healthcare",
    "other": "other",
    "civic": "civic",
    "internet & mobile": "internet_mobile",
    "internet_mobile": "internet_mobile",

    # Derived income categories
    "required annual income after taxes": "required_after_tax",
    "annual taxes": "annual_taxes",
    "required annual income before taxes": "required_before_tax",
}
