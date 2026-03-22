## Database Overview

Search aggregations use OpenSearch `terms` buckets, which **require an explicit `size`** (there is no unbounded “return all” mode). Defaults in code aim for typical cluster limits (e.g. `max_terms_count` ~65535 for comment IDs per docket). Tune for very large dockets with:

- `OPENSEARCH_COMMENT_ID_TERMS_SIZE` — distinct `commentId` buckets per docket (must align with cluster/index `max_terms_count` if raised).
- `OPENSEARCH_MATCH_DOCKET_BUCKET_SIZE` — how many docket buckets to return for corpus-wide text match queries (trade memory/latency vs completeness).

If a single docket can exceed those limits, counts may be approximate unless you move to a **composite aggregation** (paged) or a different counting strategy.

There are three OpenSearch indices:

- `comments`
- `comments_extracted_text`
- `documents`
- `extracted_text_test` (testing only)

---

## `comments` Index

```json
{
  "comments": {
    "mappings": {
      "properties": {
        "commentId": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 256
            }
          }
        },
        "commentText": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 256
            }
          }
        },
        "docketId": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 256
            }
          }
        }
      }
    }
  }
}
```
Has around 25 million json files

## `comments_extracted_text`
```json
{
  "comments_extracted_text" : {
    "mappings" : {
      "properties" : {
        "attachmentId" : {
          "type" : "text",
          "fields" : {
            "keyword" : {
              "type" : "keyword",
              "ignore_above" : 256
            }
          }
        },
        "commentId" : {
          "type" : "text",
          "fields" : {
            "keyword" : {
              "type" : "keyword",
              "ignore_above" : 256
            }
          }
        },
        "docketId" : {
          "type" : "text",
          "fields" : {
            "keyword" : {
              "type" : "keyword",
              "ignore_above" : 256
            }
          }
        },
        "extractedMethod" : {
          "type" : "text",
          "fields" : {
            "keyword" : {
              "type" : "keyword",
              "ignore_above" : 256
            }
          }
        },
        "extractedText" : {
          "type" : "text",
          "fields" : {
            "keyword" : {
              "type" : "keyword",
              "ignore_above" : 256
            }
          }
        }
      }
    }
  }
}
```
Has around 2.5 million json files with text extracted from PDF attachments on comments. Connects to comments via commentId and docketId

## `documents`
```json
{
  "documents" : {
    "mappings" : {
      "properties" : {
        "agencyId" : {
          "type" : "text",
          "fields" : {
            "keyword" : {
              "type" : "keyword",
              "ignore_above" : 256
            }
          }
        },
        "comment" : {
          "type" : "text",
          "fields" : {
            "keyword" : {
              "type" : "keyword",
              "ignore_above" : 256
            }
          }
        },
        "docketId" : {
          "type" : "text",
          "fields" : {
            "keyword" : {
              "type" : "keyword",
              "ignore_above" : 256
            }
          }
        },
        "documentId" : {
          "type" : "text",
          "fields" : {
            "keyword" : {
              "type" : "keyword",
              "ignore_above" : 256
            }
          }
        },
        "documentType" : {
          "type" : "text",
          "fields" : {
            "keyword" : {
              "type" : "keyword",
              "ignore_above" : 256
            }
          }
        },
        "modifyDate" : {
          "type" : "text",
          "fields" : {
            "keyword" : {
              "type" : "keyword",
              "ignore_above" : 256
            }
          }
        },
        "postedDate" : {
          "type" : "text",
          "fields" : {
            "keyword" : {
              "type" : "keyword",
              "ignore_above" : 256
            }
          }
        },
        "title" : {
          "type" : "text",
          "fields" : {
            "keyword" : {
              "type" : "keyword",
              "ignore_above" : 256
            }
          }
        }
      }
    }
  }
}
```
Around 2 million documents that can connect with documentId and docketId

## `extracted_text_test`
Same schema as comments_extracted_text. Used to see if ingesting a few comments worked. Can be ignored for now.
