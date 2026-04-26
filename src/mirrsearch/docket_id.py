"""Docket ID parsing and normalization.

Recognizes the docket-style identifiers used on Regulations.gov and the
Federal Register and canonicalizes user-typed variants ("cms 9115 f",
"CMS_9115_F", "cms-9115-f") to a single hyphen-uppercase form.

Two families are supported:

1. Regulations.gov / eRulemaking docket folder IDs — AGENCY[-LOCATION][-OFFICE]-YYYY-NNNN.
   Examples: FAA-2026-0534, DOT-OST-2023-0145, FDA-2024-N-0019,
   EPA-HQ-OPP-2009-0634.

2. Federal Register agency rule/file codes — AGENCY-RULENUMBER[-STAGE], where
   STAGE is one of P (proposed), F (final), IFC (interim final w/ comment),
   N (notice), CN (correction notice), D (draft).
   Examples: CMS-9115-F, CMS-9115-P, CMS-1830.
"""
import re

# Stage suffixes for the agency rule-code family.
_RULE_STAGES = {"P", "F", "IFC", "N", "CN", "D"}

# Year segment must be 19xx or 20xx so we don't confuse a 4-digit rule
# number (e.g. "9115" in CMS-9115-F) for a year.
_YEAR_RE = re.compile(r"^(19|20)\d{2}$")

# Each segment is short and alphanumeric. Real-world segments top out at
# ~5 chars (IFC, R09, 0634); 6 leaves headroom for any agency drift.
_SEGMENT_RE = re.compile(r"^[A-Z0-9]{1,6}$")

# Separators we accept between segments: hyphen, underscore, whitespace.
_SPLIT_RE = re.compile(r"[-_\s]+")


def _split_segments(query):
    """Uppercase, split on separators, return segments or None if malformed."""
    segments = [s for s in _SPLIT_RE.split(query.strip().upper()) if s]
    if not (2 <= len(segments) <= 6):
        return None
    if not all(_SEGMENT_RE.match(s) for s in segments):
        return None
    if not segments[0].isalpha():
        return None
    return segments


def _is_regulations_gov_format(segments):
    """True if a YYYY year appears anywhere after the agency segment."""
    return any(_YEAR_RE.match(s) for s in segments[1:])


def _is_rule_code_format(segments):
    """True for AGENCY-NUMBER or AGENCY-NUMBER-STAGE (CMS-9115-F style)."""
    if len(segments) == 2:
        return segments[1].isdigit()
    if len(segments) == 3:
        return segments[1].isdigit() and segments[2] in _RULE_STAGES
    return False


def normalize_docket_id(query):
    """Canonical hyphen-uppercase form of a docket-ID-shaped query, or None.

    Returns None when the input doesn't structurally match any recognized
    docket ID format, so callers can fall back to a normal substring search.
    """
    if not query:
        return None
    segments = _split_segments(query)
    if segments is None:
        return None
    if _is_regulations_gov_format(segments) or _is_rule_code_format(segments):
        return "-".join(segments)
    return None


def looks_like_docket_id(query):
    return normalize_docket_id(query) is not None
