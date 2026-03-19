import re
from typing import List, Dict, Any


class MockDBLayer:  # pylint: disable=too-few-public-methods
    """
    Mock DB layer that returns hardcoded dummy data for testing.
    Mirrors the interface of DBLayer without any DB connection.
    """

    def _items(self) -> List[Dict[str, Any]]:
        return [
            {
                "docket_id": "CMS-2025-0240",
                "title": (
                    "CY 2026 Changes to the End-Stage Renal Disease (ESRD) "
                    "Prospective Payment System and Quality Incentive Program. "
                    "CMS1830-P Display"
                ),
                "cfrPart": "42 CFR Parts 413 and 512",
                "agency_id": "CMS",
                "document_type": "Proposed Rule",
            },
            {
                "docket_id": "CMS-2025-0240",
                "title": (
                    "Medicare Program: End-Stage Renal Disease Prospective "
                    "Payment System, Payment for Renal Dialysis Services "
                    "Furnished to Individuals with Acute Kidney Injury, "
                    "End-Stage Renal Disease Quality Incentive Program, and "
                    "End-Stage Renal Disease Treatment Choices Model"
                ),
                "cfrPart": "42 CFR Parts 413 and 512",
                "agency_id": "CMS",
                "document_type": "Proposed Rule",
            },
        ]

    def get_all(self) -> List[Dict[str, Any]]:
        """Return all dummy records without filtering."""
        return self._items()

    def search(
            self,
            query: str,
            document_type_param: str = None,
            agency: List[str] = None,
            cfr_part_param: List[str] = None) \
            -> List[Dict[str, Any]]:
        q = re.sub(r'[^\w\s-]', '', (query or "")).strip().lower()
        results = [
            item for item in self._items()
            if not q
            or q in item["docket_id"].lower()
            or q in item["title"].lower()
            or q in item["agency_id"].lower()
        ]
        if document_type_param:
            results = [
                item for item in results
                if item["document_type"].lower() == document_type_param.lower()
            ]
        if agency:
            results = [
                item for item in results
                if any(a.lower() in item["agency_id"].lower() for a in agency)
            ]
        if cfr_part_param:
            results = [
                item for item in results
                if any(c.lower() in item["cfrPart"].lower() for c in cfr_part_param)
            ]
        return results

    def _opensearch_items(self) -> List[Dict[str, Any]]:
        """Dummy OpenSearch data for text_match_terms testing"""
        return [
            # DEA-2024-0059 - 3 docs, 2 comments with "meaningful use"
            {"docket_id": "DEA-2024-0059", "type": "document",
            "text": "This document discusses meaningful use criteria"},
            {"docket_id": "DEA-2024-0059", "type": "document",
            "text": "Additional meaningful use requirements"},
            {"docket_id": "DEA-2024-0059", "type": "document",
            "text": "Final meaningful use guidelines"},
            {"docket_id": "DEA-2024-0059", "type": "comment",
            "text": "I support the meaningful use standards"},
            {"docket_id": "DEA-2024-0059", "type": "comment",
            "text": "The meaningful use criteria seem reasonable"},

            # CMS-2025-0240 - 2 docs, 4 comments with "medicare" and "updates"
            {"docket_id": "CMS-2025-0240", "type": "document",
            "text": "Medicare program updates for 2025 including payment changes"},
            {"docket_id": "CMS-2025-0240", "type": "document",
            "text": "Medicare Advantage plan modifications and updates"},
            {"docket_id": "CMS-2025-0240", "type": "comment",
            "text": "These medicare changes will help seniors"},
            {"docket_id": "CMS-2025-0240", "type": "comment",
            "text": "I have concerns about medicare funding"},
            {"docket_id": "CMS-2025-0240", "type": "comment",
            "text": "Medicare should cover more services"},
            {"docket_id": "CMS-2025-0240", "type": "comment",
            "text": "Support the medicare updates"},
        ]

    def text_match_terms(self, terms: List[str]) -> List[Dict[str, Any]]:
        """
        Mock version of text_match_terms - searches through dummy OpenSearch data.
        """
        # Find matching items
        matching_items = [
            item for item in self._opensearch_items()
            if any(term.lower() in item["text"].lower() for term in terms)
        ]

        # Group by docket and count
        docket_counts = {}
        for item in matching_items:
            docket_id = item["docket_id"]
            if docket_id not in docket_counts:
                docket_counts[docket_id] = {"document_match_count": 0, "comment_match_count": 0}

            if item["type"] == "document":
                docket_counts[docket_id]["document_match_count"] += 1
            elif item["type"] == "comment":
                docket_counts[docket_id]["comment_match_count"] += 1

        # Format results
        return [
            {
                "docket_id": docket_id,
                "document_match_count": counts["document_match_count"],
                "comment_match_count": counts["comment_match_count"]
            }
            for docket_id, counts in docket_counts.items()
        ]
