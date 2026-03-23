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

    def search( # pylint: disable=too-many-arguments,too-many-positional-arguments,too-many-locals,unused-argument
            self,
            query: str,
            document_type_param: str = None,
            agency: List[str] = None,
            cfr_part_param: List[str] = None,
            start_date: str = None,
            end_date: str = None) \
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
                if any(
                    c['title'].lower() in item["cfrPart"].lower()
                    and c['part'].lower() in item["cfrPart"].lower()
                    for c in cfr_part_param
                )
            ]
        return results

    # pylint: disable=line-too-long
    def _opensearch_items(self) -> Dict[str, List[Dict[str, Any]]]:
        """Dummy OpenSearch data matching real production structure"""
        return {
            "documents": [
                # DEA-2024-0059 - 5 documents about marijuana rescheduling
                {
                    "agencyId": "DEA",
                    "comment": None,
                    "docketId": "DEA-2024-0059",
                    "documentId": "DEA-2024-0059-0001",
                    "documentType": "Proposed Rule",
                    "modifyDate": "2024-12-21 02:00:50+00:00",
                    "postedDate": "2024-05-21 04:00:00+00:00",
                    "title": "Schedules of Controlled Substances: Rescheduling of Marijuana"
                },
                {
                    "agencyId": "DEA",
                    "comment": None,
                    "docketId": "DEA-2024-0059",
                    "documentId": "DEA-2024-0059-42928",
                    "documentType": "Proposed Rule",
                    "modifyDate": "2024-09-03 22:48:28+00:00",
                    "postedDate": "2024-08-29 04:00:00+00:00",
                    "title": "Schedules of Controlled Substances: Rescheduling of Marijuana"
                },
                # CMS-2025-0240 - 2 documents mentioning "medicare", 1 mention of "ESRD"
                {
                    "agencyId": "CMS",
                    "comment": None,
                    "docketId": "CMS-2025-0240",
                    "documentId": "CMS-2025-0240-0214",
                    "documentType": "Rule",
                    "modifyDate": "2025-11-24 21:44:12+00:00",
                    "postedDate": "2025-11-24 05:00:00+00:00",
                    "title": "Medicare Program: End-Stage Renal Disease Prospective Payment System"
                },
                {
                    "agencyId": "CMS",
                    "comment": None,
                    "docketId": "CMS-2025-0240",
                    "documentId": "CMS-2025-0240-0002",
                    "documentType": "Proposed Rule",
                    "modifyDate": "2025-08-31 09:00:09+00:00",
                    "postedDate": "2025-07-02 04:00:00+00:00",
                    "title": "Medicare Program: End-Stage Renal Disease Prospective Payment System"
                },
                {
                    "agencyId": "CMS",
                    "comment": None,
                    "docketId": "CMS-2025-0240",
                    "documentId": "CMS-2025-0240-0001",
                    "documentType": "Proposed Rule",
                    "modifyDate": "2025-07-02 16:46:00+00:00",
                    "postedDate": "2025-06-30 04:00:00+00:00",
                    "title": "CY 2026 Changes to the End-Stage Renal Disease (ESRD) Prospective Payment System"
                },
                # CMS-2019-0100 - 2 documents about Home Health
                {
                    "agencyId": "CMS",
                    "comment": None,
                    "docketId": "CMS-2019-0100",
                    "documentId": "CMS-2019-0100-0001",
                    "documentType": "Proposed Rule",
                    "modifyDate": "2019-08-28 01:06:05+00:00",
                    "postedDate": "2019-07-11 04:00:00+00:00",
                    "title": "CY 2020 Home Health Prospective Payment System Rate Update"
                },
                {
                    "agencyId": "CMS",
                    "comment": None,
                    "docketId": "CMS-2019-0100",
                    "documentId": "CMS-2019-0100-0559",
                    "documentType": "Rule",
                    "modifyDate": "2019-11-08 17:42:19+00:00",
                    "postedDate": "2019-10-31 04:00:00+00:00",
                    "title": "CY 2020 Home Health Prospective Payment System Rate Update"
                },
            ],
            "comments": [
                # DEA-2024-0059 - 1 comment about marijuana
                {
                    "commentId": "DEA-2024-0059-16885",
                    "commentText": "I am writing to express my support for the proposal to reclassify marijuana from a Schedule I to a Schedule III substance",
                    "docketId": "DEA-2024-0059"
                },
                # CMS-2025-0240 - 2 comments about Medicare/phosphate
                {
                    "commentId": "CMS-2025-0240-0014",
                    "commentText": "On behalf of kidney disease patients throughout the Midwest, I ask you to please reconsider the decision to move phosphate lowering treatments (PLT) out of Medicare Part D",
                    "docketId": "CMS-2025-0240"
                },
                {
                    "commentId": "CMS-2025-0240-0030",
                    "commentText": "What if you were told the medication your patient depends on to stay alive is now harder to get. That's the reality for thousands of kidney patients since Medicare shifted Phosphate Lowering Therapies (PLTs)",
                    "docketId": "CMS-2025-0240"
                },
                # CMS-2019-0100 - 7 comments about Home Health
                {
                    "commentId": "CMS-2019-0100-0402",
                    "commentText": "Please see the attached document. Thank you",
                    "docketId": "CMS-2019-0100"
                },
                {
                    "commentId": "CMS-2019-0100-0415",
                    "commentText": "Attached please find comments from BayCare HomeCare in Largo, Florida.",
                    "docketId": "CMS-2019-0100"
                },
                {
                    "commentId": "CMS-2019-0100-0420",
                    "commentText": "The home health agency that I work for is located in Florida and serves approximately 250 unique Medicare beneficiaries each month",
                    "docketId": "CMS-2019-0100"
                },
                {
                    "commentId": "CMS-2019-0100-0423",
                    "commentText": "Another behavioral assumption related to diagnostic coding is also very concerning to me",
                    "docketId": "CMS-2019-0100"
                },
                {
                    "commentId": "CMS-2019-0100-0438",
                    "commentText": "On behalf of the Academy of Geriatric Physical Therapy I am writing to submit comments on the Medicare Home Health Prospective Payment System",
                    "docketId": "CMS-2019-0100"
                },
                {
                    "commentId": "CMS-2019-0100-0424",
                    "commentText": "I am writing in response to the request for comments on the Centers for Medicare and Medicaid Services Home Health Prospective Payment System",
                    "docketId": "CMS-2019-0100"
                },
                {
                    "commentId": "CMS-2019-0100-0431",
                    "commentText": "Seema Verma, Administrator Centers for Medicare and Medicaid Services",
                    "docketId": "CMS-2019-0100"
                },
            ],
            "comments_extracted_text": [
                # DEA-2024-0059 - 1 extracted text about cannabis
                {
                    "attachmentId": "DEA-2024-0059-24062-1",
                    "commentId": "DEA-2024-0059-24062",
                    "docketId": "DEA-2024-0059",
                    "extractedMethod": "pdfminer",
                    "extractedText": "I am providing comments in "
                    "support of the rescheduling of botanical cannabis. The Department of Health and Human Services appropriately concluded that cannabis has a currently accepted medical use"
                },
                # CMS-2025-0240 - 4 extracted texts about Medicare/ESRD
                {
                    "attachmentId": "CMS-2025-0240-0203-1",
                    "commentId": "CMS-2025-0240-0203",
                    "docketId": "CMS-2025-0240",
                    "extractedMethod": "pypdf",
                    "extractedText": "RE: Medicare End-Stage Renal Disease Prospective Payment System for CY 2026"
                },
                {
                    "attachmentId": "CMS-2025-0240-0156-1",
                    "commentId": "CMS-2025-0240-0156",
                    "docketId": "CMS-2025-0240",
                    "extractedMethod": "pypdf",
                    "extractedText": "Dialysis Patient Citizens August 27, 2025 Medicare ESRD Prospective Payment System"
                },
                {
                    "attachmentId": "CMS-2025-0240-0168-1",
                    "commentId": "CMS-2025-0240-0168",
                    "docketId": "CMS-2025-0240",
                    "extractedMethod": "pypdf",
                    "extractedText": "RE: Medicare Program End-Stage Renal Disease Prospective Payment System"
                },
                {
                    "attachmentId": "CMS-2025-0240-0165-1",
                    "commentId": "CMS-2025-0240-0165",
                    "docketId": "CMS-2025-0240",
                    "extractedMethod": "pypdf",
                    "extractedText": "The Honorable Mehmet Oz Administrator Centers for Medicare and Medicaid Services"
                },
                # CMS-2019-0100 - 3 extracted texts about Home Health/Medicare
                {
                    "attachmentId": "CMS-2019-0100-0177-1",
                    "commentId": "CMS-2019-0100-0177",
                    "docketId": "CMS-2019-0100",
                    "extractedMethod": "pdfminer",
                    "extractedText": "RE: Medicare and Medicaid Programs CY 2020 Home Health Prospective Payment System"
                },
                {
                    "attachmentId": "CMS-2019-0100-0519-1",
                    "commentId": "CMS-2019-0100-0519",
                    "docketId": "CMS-2019-0100",
                    "extractedMethod": "pdfminer",
                    "extractedText": "Seema Verma, Administrator Centers for Medicare & Medicaid Services"
                },
                {
                    "attachmentId": "CMS-2019-0100-0205-1",
                    "commentId": "CMS-2019-0100-0205",
                    "docketId": "CMS-2019-0100",
                    "extractedMethod": "pdfminer",
                    "extractedText": "CMS-1711-P proposes several significant changes for Home Health Provider community"
                },
            ]
        }

    def text_match_terms(self, terms: List[str]) -> List[Dict[str, Any]]:  # pylint: disable=too-many-locals
        """
        Mock version that mirrors OpenSearch behavior:
        - documents: searches title + comment
        - comments: searches commentText  
        - comments_extracted_text: searches extractedText (counts as comments)
        """
        data = self._opensearch_items()

        def matches_phrase(text: str, term: str) -> bool:
            """Simple phrase matching - term appears in text"""
            if text is None:
                return False
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

        # Match comments (commentText)
        matching_comments = [
            comment for comment in data["comments"]
            if any(
                matches_phrase(comment["commentText"], term)
                for term in terms
            )
        ]

        # Match extracted text (extractedText) - counts as comments
        matching_extracted = [
            extracted for extracted in data["comments_extracted_text"]
            if any(
                matches_phrase(extracted["extractedText"], term)
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

        for extracted in matching_extracted:
            docket_id = extracted["docketId"]
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
