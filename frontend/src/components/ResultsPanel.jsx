export default function ResultsPanel({ results }) {
  if (!results || results.length === 0) {
    return (
      <div className="results">
        <p>No results found.</p>
      </div>
    );
  }

  return (
    <div className="results">
      {results.map((item, index) => (
        <div key={item.docket_id || index} className="result-card">
          
          <h3 className="result-title">
            {item.title}
          </h3>

          <div className="result-meta">
            <p><strong>Agency:</strong> {item.agency_id}</p>
            <p><strong>Document Type:</strong> {item.document_type}</p>
            <p><strong>CFR:</strong> {item.cfrPart}</p>
            <p><strong>Publication Date:</strong> {item.publication_date}</p>
          </div>

          {item.summary && (
            <p className="result-summary">
              {item.summary}
            </p>
          )}

        </div>
      ))}
    </div>
  );
}