# Missing FR fields (vs `federal_register_documents` in the Mirrulations schema)

From the Federal Register document JSON (example: `data/runs/CMS_2026/federal_register/2025-21121.json`), the following fields are **not represented** `federal_register_documents` schema.

### Missing fields with descriptions (2 sentences each)

- `action`: The Federal Register “Action” line (e.g., Notice, Proposed rule, Final rule) describing what kind of publication this entry is. It’s often similar to, but not always identical with, your `document_type`.

- `agencies[]`: Structured agency objects associated with the FR document (not just names), including IDs, slugs, and canonical URLs. This is useful if you want stable agency identifiers or agency hierarchy beyond a single `agency_id` string.

- `body_html_url`: A direct link to the “full text” HTML rendition hosted by FederalRegister.gov. It’s distinct from `html_url`, which is the human-facing landing page URL.

- `citation`: The official Federal Register citation string (e.g., “90 FR 54327”). It’s useful for legal citation and for quickly linking page numbers to the correct volume.

- `comment_url`: A FederalRegister.gov URL for submitting/viewing comments (when FR supports that for the document). This is separate from regulations.gov comment URLs and can differ or be null.

- `comments_close_on`: The FR-provided close date for comments (YYYY-MM-DD) when applicable. This can be more reliable for FR-centric displays than deriving windows from regulations.gov fields.

- `correction_of`: When present, points to the document number of the FR document that this entry corrects. It enables tracing correction chains without re-parsing free text.

- `corrections[]`: A list of FR documents that correct this document (the inverse relationship of `correction_of`). It’s useful for alerting users that the rule/notice has been amended or fixed.

- `dates`: The free-text “DATES:” section as published in the FR notice/rule. It often contains nuance (multiple deadlines, exceptions) that isn’t captured in a single date column.

- `disposition_notes`: Editorial notes about the disposition/status of the document in the FR system. This can explain withdrawals, special handling, or publication anomalies.

- `dockets[]`: A structured crosswalk block that FR provides to connect this FR entry to regulations.gov dockets/documents and include comment stats. It is richer than storing only `docket_ids[]` strings.

- `executive_order_notes`: Free-text notes related to executive order review or applicability. It provides context beyond a numeric EO identifier.

- `executive_order_number`: The executive order number associated with the document (when applicable). This enables filtering/grouping by EO without text parsing.

- `explanation`: Additional FR-provided explanation text (often used for corrections or special publication context). It can be important for transparency but is not consistently present.

- `full_text_xml_url`: A link to the FR-provided XML version of the full text. It’s useful if you want structured ingestion/paragraph-level parsing of the content.

- `images`: A mapping of image references included in the FR document content. This matters if you want to render figures/tables embedded as images.

- `images_metadata`: Metadata describing the images in `images` (e.g., sizes or identifiers). It helps with correct rendering and asset management.

- `mods_url`: A link to MODS (Metadata Object Description Schema) XML, usually hosted by GovInfo. This is a standardized metadata format that can be useful for library-style integrations.

- `not_received_for_publication`: A flag/field indicating the document was not received for publication (or similar FR status). It’s helpful for distinguishing drafts/inspection items from officially published entries.

- `page_length`: The number of Federal Register pages the document spans. This is different from (and often more authoritative than) computing `end_page - start_page + 1` yourself.

- `page_views`: A small analytics object containing view counts and last-updated timestamps for FR.gov. It’s useful for popularity/usage analytics but not required for core data modeling.

- `presidential_document_number`: An identifier used for certain presidential documents (when applicable). It supports categorizing and retrieving presidential materials distinct from standard agency documents.

- `proclamation_number`: The proclamation number for presidential proclamations. This enables precise identification and cross-referencing with other presidential document sources.

- `public_inspection_pdf_url`: A link to the Public Inspection PDF when available (pre-publication or inspection copy). This can be important when users need the inspection version before the final FR issue PDF is posted.

- `raw_text_url`: A link to a plain-text rendition of the full FR document text. It’s useful for quick text search and lightweight ingestion when XML/HTML parsing is unnecessary.

- `regulation_id_number_info`: A structured object with details about regulation identifiers beyond the simple `regulation_id_numbers[]` list. It can include additional metadata that helps connect FR entries to regulatory tracking systems.

- `regulations_dot_gov_info`: A structured FR ↔ regulations.gov crosswalk object with fields like docket/document IDs, comment counts, and the timestamp FR checked regs.gov. It is often the most convenient way to build reliable deep links into regulations.gov from an FR record.

- `regulations_dot_gov_url`: A FederalRegister.gov-provided URL pointing to the corresponding regulations.gov location (when available). It’s a convenience field that can differ from what you’d construct yourself.

- `signing_date`: The date the document was signed (when applicable), which can differ from publication or effective dates. It’s useful for timelines that track the lifecycle of a rule.

- `subtype`: A more specific FR subtype classification (often null), separate from the main `type`. It can be used for finer-grained filtering when populated.

- `toc_doc`: The “Table of Contents” document category label used by FR. It’s useful for grouping and navigation in a UI (e.g., within an issue section).

- `toc_subject`: The “Table of Contents” subject label used by FR (often null). It provides an additional level of grouping within `toc_doc`.

- `type`: The FR’s high-level type label (e.g., Notice, Rule, Proposed Rule) as FederalRegister.gov classifies it. It can differ from regulations.gov `documentType` and is helpful when you want FR-native classification.

- `volume`: The Federal Register volume number (e.g., 90). It’s commonly used alongside `citation` and page numbers for formal references.

### Nested fields (inside missing objects)

These are the notable **nested keys** you lose if you don’t persist the parent objects (like `agencies[]`, `dockets[]`, `page_views`, and `regulations_dot_gov_info`). All examples below are from `2025-21121.json`.

- `agencies[].raw_name`: The agency name exactly as it appears in the FR entry header (often uppercased department lines). It can be useful for display or for diagnosing mapping differences versus `agencies[].name`.

- `agencies[].id`: A stable FederalRegister.gov numeric agency identifier. It’s useful if you ever want to fetch agency metadata directly from the FR API or de-duplicate agencies reliably.

- `agencies[].url`: The public FederalRegister.gov agency page URL. It’s helpful for UI linking without reconstructing routes.

- `agencies[].json_url`: The FR API endpoint URL for that agency object. It provides a canonical API link for follow-on ingestion.

- `agencies[].parent_id`: The numeric parent agency id (when applicable). It allows you to model agency hierarchy (department → sub-agency) without heuristics.

- `agencies[].slug`: The human-readable slug used by FederalRegister.gov for the agency page. It’s convenient for stable URLs and debugging.

- `dockets[].id`: The regulations.gov docket id that FR believes corresponds to this FR document. It helps connect an FR entry to regs.gov even when the FR `docket_ids[]` strings are not directly usable.

- `dockets[].title`: The title of the docket as returned in the FR crosswalk. It can differ from the regs.gov docket title and is useful for display/validation.

- `dockets[].agency_name`: A short agency label used in the crosswalk (e.g., “CMS”). It’s useful when the FR entry includes multiple agencies and you want to know which agency the docket mapping refers to.

- `dockets[].supporting_documents_count`: Count of supporting documents FR associates with the docket mapping. It’s a quick signal of docket “size” without calling regs.gov.

- `dockets[].documents[].id`: A regulations.gov document id (often the main rule/notice document) linked from the FR record. It’s the most direct way to create a regs.gov “comment on document” link.

- `dockets[].documents[].comment_count`: The number of comments FR observed for the linked regs.gov document. It’s useful for ranking dockets/documents by engagement.

- `dockets[].documents[].comment_start_date`: The comment window start date FR observed for the linked regs.gov document. It can be useful if regs.gov timestamps are missing/shifted in your stored payloads.

- `dockets[].documents[].comment_end_date`: The comment window end date FR observed for the linked regs.gov document. It’s useful for UI display of comment deadlines without additional API calls.

- `dockets[].documents[].comment_url`: A direct regs.gov URL to submit a comment on the linked document. It’s a convenience link that saves you from needing to construct the URL pattern.

- `dockets[].documents[].allow_late_comments`: Whether late comments were allowed according to FR’s last check. This is a policy flag you can surface without loading the full regs.gov document payload.

- `dockets[].documents[].updated_at`: Timestamp when FR last updated this crosswalk entry. It helps you reason about staleness of FR↔regs.gov linkage data.

- `dockets[].documents[].regulations_dot_gov_open_for_comment`: Whether regs.gov was open for comment at the time FR checked. It’s useful for quickly filtering “currently open” items.

- `page_views.count`: FederalRegister.gov page view count for the FR landing page. It’s a popularity metric that can help ranking/search but isn’t required for compliance.

- `page_views.last_updated`: When the page view metrics were last updated. It helps you understand freshness of `page_views.count`.

- `regulations_dot_gov_info.comments_count`: FR’s recorded number of comments for the linked regs.gov item. It can diverge from your own harvested counts depending on timing and filters.

- `regulations_dot_gov_info.comments_url`: A regs.gov URL to the docket browser/comments listing FR associates with this FR document. It’s useful for navigation without constructing query parameters.

- `regulations_dot_gov_info.supporting_documents_count`: Count of supporting documents FR observed on regs.gov. It’s another quick “docket size” metric.

- `regulations_dot_gov_info.checked_regulationsdotgov_at`: Timestamp when FR checked regulations.gov for this linkage. It helps you detect stale crosswalk data and decide when to refresh.

# Regulations.gov fields missing from the Mirrulations schema

From regulations.gov JSON payloads (examples: `FAA-2010-1175` docket, `FAA-2010-1175-0001` document, `FAA-2010-1175-0002` comment), the following fields are **not represented** on the Mirrulations SQL schema.

## Missing fields with descriptions (2 sentences each)

- `links.self`: The canonical API URL for the specific record returned by regulations.gov. You can reconstruct it from the ID, but storing it is useful for traceability and debugging.

- `attributes.displayProperties[]`: A list of UI/display metadata objects (label/tooltip/name) that describe how some fields should be presented. It’s not needed for core search, but it can help you render user-friendly labels without hardcoding.

- `attributes.objectId`: A regulations.gov internal object identifier (often used as `commentOn` targets). It’s important when you need to relate a comment to the specific “object” being commented on, not just the document ID string.

- `attributes.legacyId`: A legacy identifier sometimes present for backward compatibility with older systems. If you don’t store it you usually won’t miss data, but it can help with reconciliation against legacy exports.

### Docket (`type: dockets`) missing fields

- `attributes.dkAbstract`: Docket abstract/summary text as provided at the docket level. Your schema stores `docket_abstract`, but note the naming mismatch (`dkAbstract` vs `docket_abstract`) and that it may require explicit mapping.

- `attributes.keywords`: Keywords associated with a docket when the agency provides them. This can improve search relevance but is often null.

- `attributes.generic`: A generic/freeform docket field some agencies populate. It can carry agency-specific meaning, so storing it requires either a flexible column or a JSON blob.

- `attributes.objectId`: Docket-level object identifier used internally by regulations.gov. It’s separate from the public `docket_id` and can matter for some cross-object references.

### Document (`type: documents`) missing fields

- `relationships.attachments.links.self` / `relationships.attachments.links.related`: URLs that point to the attachments relationship endpoint and the actual attachments collection. Without these, you can still fetch attachments by constructing URLs, but you lose the discoverable linkage the API provides.

- `attributes.fileFormats[]`: A list describing available content renditions for the document (e.g., PDF/HTML/HTM) with URLs and sizes. Your schema does not have a normalized table for these, so you can’t query “has HTML/HTM” without re-reading raw JSON.

- `attributes.fileFormats[].fileUrl`: The download URL for a particular file rendition. This is essential if you plan to fetch full text or provide direct download links.

- `attributes.fileFormats[].format`: The format label for the rendition (e.g., `pdf`, `html`, `htm`). It’s useful for filtering and choosing preferred renditions.

- `attributes.fileFormats[].size`: The byte size of the rendition. It’s useful for estimating download cost and for detecting incomplete or anomalously small files.

- `attributes.displayProperties[]`: Document-level display metadata describing fields like `pageCount`. It helps render consistent labels/tooltips but is not required for the data pipeline.

### Comment (`type: comments`) missing fields

- `attributes.commentOn`: The regulations.gov `objectId` of the item being commented on. This is a stronger linkage than free-text and can be required to correctly associate comments with the exact object.

- `attributes.commentOnDocumentId`: The specific regulations.gov document ID that the comment is on (when available). Your schema links comments to `document_id`, but this field is the authoritative source and should be mapped explicitly.

- `attributes.duplicateComments`: The API’s count of detected duplicate comments for this comment record. Your schema has `duplicate_comment_count`, but note the naming mismatch (`duplicateComments`) and that it needs mapping.

- `relationships.attachments.links.self` / `relationships.attachments.links.related`: Endpoints for comment attachments (relationship and collection). If you don’t store/fetch them, you will miss attached files submitted with comments.

## Nested fields (inside missing objects)

These are the notable nested keys you lose if you don’t persist the parent objects (`displayProperties[]`, `fileFormats[]`, `relationships.attachments`).

- `displayProperties[].name`: The field name the display metadata applies to. It’s used to connect the label/tooltip back to a specific attribute.

- `displayProperties[].label`: A human-readable label for UI display. It’s useful if you want to avoid hardcoded labels in the frontend.

- `displayProperties[].tooltip`: Help text explaining the meaning of a field. It improves UX but is not required for querying.

