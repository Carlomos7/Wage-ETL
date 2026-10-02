import re
from src.transform.constants import CATEGORY_MAP, FAMILY_CONFIG_MAP


def normalize_header_for_lookup(header: str) -> str:
    """
    Normalize a header string to match FAMILY_CONFIG_MAP keys.

    Normalize variations in case, spacing, and formatting from scraped data.
    """
    # Lowercase first for case-insensitive matching
    normalized = header.lower()

    # Remove separator between adult config and child count
    normalized = normalized.replace(" - ", " ")

    # Normalize spacing around parentheses: "2 adults(1 working)" -> "2 adults (1 working)"
    normalized = re.sub(r"(\w)\(", r"\1 (", normalized)

    # Collapse multiple spaces
    normalized = " ".join(normalized.split())

    # Remove "both working" variants - these map to 2 working adults
    normalized = normalized.replace("(both working)", "").strip()

    # Remove explicit zero-child cases
    normalized = normalized.replace(
        " 0 children", "").replace(" 0 child", "").strip()

    # Final cleanup: collapse any remaining multiple spaces
    normalized = " ".join(normalized.split())

    return normalized


def get_family_config_metadata(header: str) -> dict[str, int] | None:
    """
    Look up family configuration metadata for a header.

    Args:
        header: Header string from DataFrame column

    Returns:
        Dict with keys: adults, working_adults, children. Returns None if not found.
    """
    normalized = normalize_header_for_lookup(header)
    return FAMILY_CONFIG_MAP.get(normalized)


def normalize_category_key(text: str) -> str:
    """
    Normalize raw category text into a lookup key.
    """
    raw = str(text).strip().lower()

    # Clean multiple spaces / punctuation to a single space
    cleaned = re.sub(r"[^\w]+", " ", raw).strip()

    return cleaned


def lookup_category_value(key: str) -> str:
    """
    Return the canonical category value if known, otherwise fallback to slugified key.
    """
    normalized = normalize_category_key(key)

    if normalized in CATEGORY_MAP:
        return CATEGORY_MAP[normalized]

    # Fallback slug
    return normalized.replace(" ", "_")
