# OpenSearch Installation and Launch Guide

This doc shows how to run OpenSearch locally:

- Homebrew (macOS)

## Homebrew (macOS)

Install and run OpenSearch:

Install OpenSearch via Homebrew:

```bash
brew install opensearch
```

Start the OpenSearch service:

```bash
brew services start opensearch
```

Verify OpenSearch is running(Should return json output if working correctly):

```bash
curl -X GET "http://localhost:9200/"
```

Stop the OpenSearch service when finished:

```bash
brew services stop opensearch
```
