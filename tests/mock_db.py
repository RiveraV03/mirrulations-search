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

def _opensearch_items(self) -> Dict[str, List[Dict[str, Any]]]:
    return {
        "documents": [
            {
                "agencyId": "DEA",
                "comment": "",
                "docketId": "DEA-2024-0059",
                "documentId": "DEA-2024-0059-0001",
                "documentType": "Proposed Rule",
                "modifyDate": "2024-01-15",
                "postedDate": "2024-01-10",
                "title": "This document discusses meaningful use criteria for healthcare"
            },
            {
                "agencyId": "DEA",
                "comment": "",
                "docketId": "DEA-2024-0059",
                "documentId": "DEA-2024-0059-0002",
                "documentType": "Rule",
                "modifyDate": "2024-02-20",
                "postedDate": "2024-02-15",
                "title": "Additional meaningful use requirements and standards"
            },
            {
                "agencyId": "DEA",
                "comment": "Meaningful use standards are important for improving healthcare",
                "docketId": "DEA-2024-0059",
                "documentId": "DEA-2024-0059-0003",
                "documentType": "Rule",
                "modifyDate": "2024-03-10",
                "postedDate": "2024-03-05",
                "title": "Final meaningful use reporting guidelines"
            },
            {
                "agencyId": "CMS",
                "comment": "Medicare updates will help seniors",
                "docketId": "CMS-2025-0240",
                "documentId": "CMS-2025-0240-0001",
                "documentType": "Proposed Rule",
                "modifyDate": "2025-01-20",
                "postedDate": "2025-01-15",
                "title": "Medicare program updates for 2025 including payment changes"
            },
            {
                "agencyId": "CMS",
                "comment": "",
                "docketId": "CMS-2025-0240",
                "documentId": "CMS-2025-0240-0002",
                "documentType": "Rule",
                "modifyDate": "2025-02-10",
                "postedDate": "2025-02-05",
                "title": "Medicare Advantage plan modifications and updates"
            },
        ],
        "comments": [
            {
                "commentId": "DEA-2024-0059-0001",
                "commentText": "I support the meaningful use standards proposed",
                "docketId": "DEA-2024-0059"
            },
            {
                "commentId": "DEA-2024-0059-0002",
                "commentText": "The meaningful use criteria seem reasonable",
                "docketId": "DEA-2024-0059"
            },
            {
                "commentId": "CMS-2025-0240-0001",
                "commentText": "These medicare changes will help seniors",
                "docketId": "CMS-2025-0240"
            },
            {
                "commentId": "CMS-2025-0240-0002",
                "commentText": "I have concerns about medicare funding",
                "docketId": "CMS-2025-0240"
            },
            {
                "commentId": "CMS-2025-0240-0003",
                "commentText": "Medicare should cover more services",
                "docketId": "CMS-2025-0240"
            },
            {
                "commentId": "CMS-2025-0240-0004",
                "commentText": "Support the medicare updates proposed here",
                "docketId": "CMS-2025-0240"
            },
        ]
    }

def text_match_terms(self, terms: List[str]) -> List[Dict[str, Any]]:
    """
    Mock version that mirrors OpenSearch behavior:
    - documents: searches title + comment
    - comments: simulates phrase matching
    """
    data = self._opensearch_items()

    def matches_phrase(text: str, term: str) -> bool:
        return term.lower() in text.lower()

    # Match documents (title OR comment)
    matching_docs = [
        doc for doc in data["documents"]
        if any(
            matches_phrase(doc.get("title", ""), term) or
            matches_phrase(doc.get("comment", ""), term)
            for term in terms
        )
    ]

    # Match comments (phrase-style)
    matching_comments = [
        comment for comment in data["comments"]
        if any(
            matches_phrase(comment["commentText"], term)
            for term in terms
        )
    ]

    # Group by docket
    docket_counts = {}

    for doc in matching_docs:
        docket_id = doc["docketId"]
        docket_counts.setdefault(docket_id, {
            "document_match_count": 0,
            "comment_match_count": 0
        })
        docket_counts[docket_id]["document_match_count"] += 1

    for comment in matching_comments:
        docket_id = comment["docketId"]
        docket_counts.setdefault(docket_id, {
            "document_match_count": 0,
            "comment_match_count": 0
        })
        docket_counts[docket_id]["comment_match_count"] += 1

    return [
        {
            "docket_id": docket_id,
            "document_match_count": counts["document_match_count"],
            "comment_match_count": counts["comment_match_count"]
        }
        for docket_id, counts in docket_counts.items()
    ]