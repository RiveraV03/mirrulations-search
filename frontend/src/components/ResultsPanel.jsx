import {ColorRing} from 'react-loader-spinner'
const CFR_BASE_URL = "https://www.ecfr.gov/search#query";

export default function ResultsPanel({ results, loading }) {

  if (loading) {
    return (
      <div className="results">
        <ColorRing
          visible={true}
          height="80"
          width="80"
          ariaLabel="color-ring-loading"
          colors={["#3b82f6", "#2563eb", "#1d4ed8", "#1e40af", "#1e3a8a"]}
        />
      </div>
    );
  }



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
           <p>
            <strong>CFR:</strong>{" "}
               {item.cfrPart.map((p, index) => (
                 <span key={index}>
             <a href={p.link} target="_blank" rel="noopener noreferrer">
             {p.part}
           </a>
             {index < item.cfrPart.length - 1 && ", "}
            </span>
             ))}
            </p>
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