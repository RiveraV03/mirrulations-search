import {ColorRing} from 'react-loader-spinner'
const CFR_BASE_URL = "https://www.ecfr.gov/search#query";

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
          <h3 className="result-title">{item.docket_title}
          </h3>

          <div className="result-meta">
            <p><strong>Agency:</strong> {item.agency_id}</p>
            <p><strong>Docket-ID:</strong> {item.docket_id}</p>
            <p><strong>Docket type:</strong> {item.docket_type}</p>
            <a href={CFR_BASE_URL}>
            <p>
              <strong>CFR:</strong>{" "}
              {item.cfrPart.map(p => p.part).join(", ")}
            </p>
            </a>
            <p><strong>Last modified date:</strong> {item.modify_date}</p>
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