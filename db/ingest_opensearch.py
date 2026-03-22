"""
Ingest real production-like data into local OpenSearch matching the actual structure.
Uses separate indices for documents, comments, and comments_extracted_text.
"""

from opensearchpy import OpenSearch


def ingest_opensearch():
    """Insert production-like documents, comments, and extracted text into local OpenSearch."""
    try:
        client = OpenSearch(
            hosts=[{"host": "localhost", "port": 9200}],
            use_ssl=False,
            verify_certs=False,
        )
        # Force a request early so we can exit gracefully if OpenSearch is down.
        client.info()
    except Exception as e:  # pylint: disable=broad-exception-caught
        print(f"OpenSearch is not running (skipping ingest): {e}")
        return

    # Delete existing indexes for a clean ingest.
    for index in ["documents", "comments", "comments_extracted_text"]:
        if client.indices.exists(index=index):
            client.indices.delete(index=index)
    
    client.indices.create(
        index="documents",
        body={
            "mappings": {
                "properties": {
                    "agencyId": {
                        "type": "text",
                        "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}
                    },
                    "comment": {
                        "type": "text",
                        "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}
                    },
                    "docketId": {
                        "type": "text",
                        "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}
                    },
                    "documentId": {
                        "type": "text",
                        "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}
                    },
                    "documentType": {
                        "type": "text",
                        "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}
                    },
                    "modifyDate": {
                        "type": "text",
                        "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}
                    },
                    "postedDate": {
                        "type": "text",
                        "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}
                    },
                    "title": {
                        "type": "text",
                        "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}
                    }
                }
            }
        }
    )
    
    # Delete and recreate comments index
    if client.indices.exists(index="comments"):
        client.indices.delete(index="comments")
    
    client.indices.create(
        index="comments",
        body={
            "mappings": {
                "properties": {
                    "commentId": {
                        "type": "text",
                        "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}
                    },
                    "commentText": {
                        "type": "text",
                        "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}
                    },
                    "docketId": {
                        "type": "text",
                        "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}
                    }
                }
            }
        }
    )
    
    # Delete and recreate comments_extracted_text index
    if client.indices.exists(index="comments_extracted_text"):
        client.indices.delete(index="comments_extracted_text")
    
    client.indices.create(
        index="comments_extracted_text",
        body={
            "mappings": {
                "properties": {
                    "attachmentId": {
                        "type": "text",
                        "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}
                    },
                    "commentId": {
                        "type": "text",
                        "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}
                    },
                    "docketId": {
                        "type": "text",
                        "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}
                    },
                    "extractedMethod": {
                        "type": "text",
                        "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}
                    },
                    "extractedText": {
                        "type": "text",
                        "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}
                    }
                }
            }
        }
    )
    
    # Insert documents
    documents = [
        {
            "agencyId": "DEA",
            "comment": None,
            "docketId": "DEA-2024-0059",
            "documentId": "DEA-2024-0059-0004",
            "documentType": "Supporting & Related Material",
            "modifyDate": "2024-05-21 13:46:37+00:00",
            "postedDate": "2024-05-21 04:00:00+00:00",
            "title": "2024-04-11 - AAG Fonzone - Marijuana Rescheduling"
        },
        {
            "agencyId": "DEA",
            "comment": None,
            "docketId": "DEA-2024-0059",
            "documentId": "DEA-2024-0059-0005",
            "documentType": "Supporting & Related Material",
            "modifyDate": "2024-05-21 13:55:07+00:00",
            "postedDate": "2024-05-21 04:00:00+00:00",
            "title": "2016-17960-DEA-427"
        },
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
            "documentId": "DEA-2024-0059-0007",
            "documentType": "Supporting & Related Material",
            "modifyDate": "2024-05-21 13:55:44+00:00",
            "postedDate": "2024-05-21 04:00:00+00:00",
            "title": "2016-17954-DEA-426"
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
        {
            "agencyId": "CMS",
            "comment": None,
            "docketId": "CMS-2025-0240",
            "documentId": "CMS-2025-0240-0214",
            "documentType": "Rule",
            "modifyDate": "2025-11-24 21:44:12+00:00",
            "postedDate": "2025-11-24 05:00:00+00:00",
            "title": "Medicare Program: End-Stage Renal Disease Prospective Payment System, Payment for Renal Dialysis Services Furnished to Individuals with Acute Kidney Injury, End-Stage Renal Disease Quality Incentive Program, and End-Stage Renal Disease Treatment Choices Model"
        },
        {
            "agencyId": "CMS",
            "comment": None,
            "docketId": "CMS-2025-0240",
            "documentId": "CMS-2025-0240-0002",
            "documentType": "Proposed Rule",
            "modifyDate": "2025-08-31 09:00:09+00:00",
            "postedDate": "2025-07-02 04:00:00+00:00",
            "title": "Medicare Program: End-Stage Renal Disease Prospective Payment System, Payment for Renal Dialysis Services Furnished to Individuals with Acute Kidney Injury, End-Stage Renal Disease Quality Incentive Program, and End-Stage Renal Disease Treatment Choices Model"
        },
        {
            "agencyId": "CMS",
            "comment": None,
            "docketId": "CMS-2025-0240",
            "documentId": "CMS-2025-0240-0001",
            "documentType": "Proposed Rule",
            "modifyDate": "2025-07-02 16:46:00+00:00",
            "postedDate": "2025-06-30 04:00:00+00:00",
            "title": "CY 2026 Changes to the End-Stage Renal Disease (ESRD) Prospective Payment System and Quality Incentive Program. CMS1830-P Display"
        },
        {
            "agencyId": "CMS",
            "comment": None,
            "docketId": "CMS-2019-0100",
            "documentId": "CMS-2019-0100-0001",
            "documentType": "Proposed Rule",
            "modifyDate": "2019-08-28 01:06:05+00:00",
            "postedDate": "2019-07-11 04:00:00+00:00",
            "title": "CY 2020 Home Health Prospective Payment System Rate Update; Value-Based Purchasing Model; Quality Reporting Requirements CMS-1711-P"
        },
        {
            "agencyId": "CMS",
            "comment": "",
            "docketId": "CMS-2025-0304",
            "documentId": "CMS-2025-0304-0001",
            "documentType": "Proposed Rule",
            "modifyDate": "2025-03-20",
            "postedDate": "2025-03-15",
            "title": "Medicare Part B payment policies and coverage changes for calendar year 2026"
        },
        {
            "agencyId": "CMS",
            "comment": "Medicare shared savings program requirements and Medicare Prescription Drug Inflation Rebate Program updates",
            "docketId": "CMS-2025-0304",
            "documentId": "CMS-2025-0304-0002",
            "documentType": "Rule",
            "modifyDate": "2025-04-10",
            "postedDate": "2025-04-05",
            "title": "Updates to Medicare Promoting Interoperability Program requirements"
        },
        {
            "agencyId": "CMS",
            "comment": "",
            "docketId": "CMS-2025-0304",
            "documentId": "CMS-2025-0304-0003",
            "documentType": "Rule",
            "modifyDate": "2025-05-10",
            "postedDate": "2025-05-05",
            "title": "Updates to ESRD Treatment Choices Model for calendar year 2026"
        },
        {
            "agencyId": "CMS",
            "comment": "Medicare rural payment support for underserved communities",
            "docketId": "CMS-2026-0001",
            "documentId": "CMS-2026-0001-0001",
            "documentType": "Rule",
            "modifyDate": "2025-12-01",
            "postedDate": "2025-12-01",
            "title": "Rural dialysis access and payment operations update"
        },
        {
            "agencyId": "CMS",
            "comment": None,
            "docketId": "CMS-2019-0100",
            "documentId": "CMS-2019-0100-0559",
            "documentType": "Rule",
            "modifyDate": "2019-11-08 17:42:19+00:00",
            "postedDate": "2019-10-31 04:00:00+00:00",
            "title": "CY 2020 Home Health Prospective Payment System Rate Update; Value-Based Purchasing Model; Quality Reporting Requirements CMS-1711-FC"
        }
    ]
    
    for i, doc in enumerate(documents):
        client.index(index="documents", id=i, body=doc)
    
    # Insert comments
    comments = [
        {
            "commentId": "CMS-2025-0240-0014",
            "commentText": "I am contacting you in my role as the Founder and CEO of Peer Plus Education and Training Advocates, a Chicago-based 501(c)3 that identifies underserved populations in the Midwest area. Peer Plus provides culturally sensitive programs that address the multifaceted issues of people in need of essential health, educational, and psychosocial services.<br/><br/>On behalf of kidney disease patients throughout the Midwest, I ask you to please reconsider the decision to move phosphate lowering treatments (PLT) out of Medicare Part D, which allowed patients to access their treatment from their community pharmacist.",
            "docketId": "CMS-2025-0240"
        },
        {
            "commentId": "CMS-2025-0240-0030",
            "commentText": "Dear Dr. Mehmet Oz,<br/><br/>What if you were told the medication your patient depends on to stay alive is now harder to get &ndash; all because of a decision in Washington D.C.? That&rsquo;s the reality for thousands of kidney patients since Medicare shifted Phosphate Lowering Therapies (PLTs) into a budget-capped payment system on January 1, 2025.",
            "docketId": "CMS-2025-0240"
        },
        {
            "commentId": "CMS-2019-0100-0402",
            "commentText": "Please see the attached document.<br/><br/>Thank you",
            "docketId": "CMS-2019-0100"
        },
        {
            "commentId": "CMS-2019-0100-0415",
            "commentText": "Attached please find comments from BayCare HomeCare in Largo, Florida.",
            "docketId": "CMS-2019-0100"
        },
        {
            "commentId": "CMS-2019-0100-0420",
            "commentText": "The home health agency that I work for is located in Florida and serves approximately 250 unique Medicare beneficiaries each month. Our agency has been subjected to Targeted Probe and Educate, the Value Based Purchasing demonstration project, a multi-year moratorium on new licenses that prevented our agency from achieving organic growth in the geographical footprint that we were able to serve.",
            "docketId": "CMS-2019-0100"
        },
        {
            "commentId": "CMS-2019-0100-0423",
            "commentText": "In addition to my previous comments - Another behavioral assumption related to diagnostic coding is also very concerning to me. In the Conditions of Participation at 484.60(a)(2) requires that all pertinent diagnoses be included in the POC.",
            "docketId": "CMS-2019-0100"
        },
        {
            "commentId": "CMS-2019-0100-0438",
            "commentText": "Dear Administrator Verma: <br/><br/>On behalf of the Academy of Geriatric Physical Therapy of the American Physical Therapy Association (Academy of Geriatric Physical Therapy), I am writing to submit comments in response to the Centers for Medicare and Medicaid Services (CMS) Calendar Year (CY) 2020 Home Health Prospective Payment System (HH PPS) Rate Update.",
            "docketId": "CMS-2019-0100"
        },
        {
            "commentId": "CMS-2019-0100-0424",
            "commentText": "Dear Administrator Verma: <br/><br/>I am writing in response to the request for comments on the Centers for Medicare and Medicaid Services (CMS) Calendar Year (CY) 2020 Home Health Prospective Payment System (PPS) Rate Update proposed rule.<br/>",
            "docketId": "CMS-2019-0100"
        },
        {
            "commentId": "CMS-2019-0100-0431",
            "commentText": "September 8, 2019<br/> <br/>Seema Verma, MPH<br/>Administrator<br/>Centers for Medicare and Medicaid Services<br/>Department of Health and Human Services<br/>Room 445-G<br/>Attn: CMS-1711-P<br/>Hubert Humphrey Building<br/>200 Independence Ave, SW<br/>Washington, DC 20201<br/> <br/>Submitted electronically",
            "docketId": "CMS-2019-0100"
        },
        {
            "commentId": "CMS-2025-0304-0001",
            "commentText": "Medicare Shared Savings Program requirements for calendar year 2026",
            "docketId": "CMS-2025-0304"
        },
        {
            "commentId": "CMS-2025-0304-0002",
            "commentText": "Medicare Prescription Drug Inflation Rebate Program updates and Part B coverage policy changes",
            "docketId": "CMS-2025-0304"
        },
        {
            "commentId": "DEA-2024-0059-16885",
            "commentText": "Official Comment Drug Enforcement Agency ,<br/><br/>I am writing to express my support for the proposal to reclassify marijuana from a Schedule I to a Schedule III substance under the Controlled Substances Act. <br/><br/>This is an important step toward a change in marijuana policy that addresses the decades-long discrimination against Black and Brown communities.",
            "docketId": "DEA-2024-0059"
        }
    ]

    for i, comment in enumerate(comments):
        client.index(index="comments", id=i, body=comment)
    
    # Insert comments_extracted_text
    extracted_texts = [
        {
            "attachmentId": "CMS-2025-0240-0203-1",
            "commentId": "CMS-2025-0240-0203",
            "docketId": "CMS-2025-0240",
            "extractedMethod": "pypdf",
            "extractedText": "FIRM:68395945v1 August 29, 2025 VIA REGULATIONS.GOV FILING Centers for Medicare & Medicaid Services U.S. Department of Health and Human Services Attention: CMS-1830-P P.O. Box 8010 Baltimore, MD 21244-8010 RE: Medicare End-Stage Renal Disease Prospective Payment System for CY 2026"
        },
        {
            "attachmentId": "CMS-2025-0240-0156-1",
            "commentId": "CMS-2025-0240-0156",
            "docketId": "CMS-2025-0240",
            "extractedMethod": "pypdf",
            "extractedText": "1300 17th St. N, Suite 580 • Arlington, VA 22209 • Toll Free Number 1.866.877.4242 www.dialysispatients.org • Email: dpc@dialysispatients.org • Fax 1.888.423.5002 DPC is a 501(c)(4) non-profit organization governed by dialysis patients. Improving Life Through Empowerment August 27, 2025"
        },
        {
            "attachmentId": "CMS-2025-0240-0168-1",
            "commentId": "CMS-2025-0240-0168",
            "docketId": "CMS-2025-0240",
            "extractedMethod": "pypdf",
            "extractedText": "August 18, 2025 Mehmet Oz, MD, MBA Administrator Centers for Medicare & Medicaid Services Department of Health and Human Services 200 Independence Avenue, SW Washington, DC 20201 Re: CMS–1830-P — Medicare Program; End-Stage Renal Disease Prospective Payment System"
        },
        {
            "attachmentId": "CMS-2025-0240-0165-1",
            "commentId": "CMS-2025-0240-0165",
            "docketId": "CMS-2025-0240",
            "extractedMethod": "pypdf",
            "extractedText": "11921 Rockville Pike | Suite 300 | Rockville, MD 20852 301.881.3052 voice | 301.881.0898 fax | 800.638.8299 toll-free | 866.300.2900 Español Member: CFC 11404 | KidneyFund.org August 28, 2025 The Honorable Mehmet Oz Administrator Centers for Medicare and Medicaid Services"
        },
        {
            "attachmentId": "CMS-2019-0100-0177-1",
            "commentId": "CMS-2019-0100-0177",
            "docketId": "CMS-2019-0100",
            "extractedMethod": "pdfminer",
            "extractedText": "August 23, 2019 Seema Verma, MPH Administrator Centers for Medicare and Medicaid Services Department of Health and Human Services Room 445-G Attn: CMS-1711-P Hubert Humphrey Building 200 Independence Ave, SW Washington, DC 20201"
        },
        {
            "attachmentId": "CMS-2019-0100-0519-1",
            "commentId": "CMS-2019-0100-0519",
            "docketId": "CMS-2019-0100",
            "extractedMethod": "pdfminer",
            "extractedText": "Humana Inc. 500 W. Main St. Louisville, KY 40202-2946 www.humana.com September 9, 2019 Seema Verma, Administrator Centers for Medicare & Medicaid Services (CMS) 7500 Security Boulevard, Attention: CMS-1689-P Baltimore, MD 21244-1850"
        },
        {
            "attachmentId": "CMS-2019-0100-0205-1",
            "commentId": "CMS-2019-0100-0205",
            "docketId": "CMS-2019-0100",
            "extractedMethod": "pdfminer",
            "extractedText": "CMS-1711-P (July 18, 2019) proposes several significant changes for Home Health Provider community. A couple of these changes will have a negative impact on our industry's ability to provide care to the Medicare beneficiaries that are served across our nation."
        },
        {
            "attachmentId": "DEA-2024-0059-24062-1",
            "commentId": "DEA-2024-0059-24062",
            "docketId": "DEA-2024-0059",
            "extractedMethod": "pdfminer",
            "extractedText": "I am providing comments in support of the rescheduling of botanical cannabis (Docket No. DEA–1362). The Department of Health and Human Services appropriately concluded that cannabis has a currently accepted medical use and that its abuse potential does not warrant its placement as either a Schedule I or Schedule II controlled substance."
        }
    ]
    
    for i, extracted in enumerate(extracted_texts):
        client.index(index="comments_extracted_text", id=i, body=extracted)
    
    # Refresh indices
    client.indices.refresh(index="documents")
    client.indices.refresh(index="comments")
    client.indices.refresh(index="comments_extracted_text")

    print(f"✓ Ingested {len(documents)} documents")
    print(f"✓ Ingested {len(comments)} comments")
    print(f"✓ Ingested {len(extracted_texts)} comments (extracted text from attachments)")
    print("\nDockets included:")
    print("  DEA-2024-0059")
    print("  CMS-2025-0240")
    print("  CMS-2019-0100")
    print("  CMS-2025-0304")
    print("  CMS-2026-0001")


if __name__ == "__main__":
    ingest_opensearch()