import "../styles/collections.css";

// Placeholder — shows pending status for all jobs until the DB team
// adds the download jobs table and GET /download/jobs endpoint.
// When that's ready, replace the MOCK_JOBS array with a real fetch call.

const MOCK_JOBS = [
  {
    job_id: "pending-placeholder",
    status: "pending",
    format: "raw",
    docket_ids: [],
    created_at: new Date().toISOString(),
  },
];

const STATUS_STYLES = {
  pending: { color: "#856404", background: "#fff8e1", border: "1px solid #ffe082" },
  ready:   { color: "#2e7d32", background: "#e8f5e9", border: "1px solid #a5d6a7" },
  failed:  { color: "#c0392b", background: "#fdecea", border: "1px solid #f5c6cb" },
};

export default function DownloadStatusModal({ onClose }) {
  const jobs = MOCK_JOBS;

  return (
    <div className="modal-backdrop" onClick={onClose}>
      <div className="modal" onClick={(e) => e.stopPropagation()}>
        <h2 className="modal-title">Your Downloads</h2>

        {jobs.length === 0 ? (
          <p className="modal-loading">No downloads yet.</p>
        ) : (
          <div className="modal-collection-list">
            {jobs.map((job) => {
              const style = STATUS_STYLES[job.status] || STATUS_STYLES.pending;
              return (
                <div key={job.job_id} className="modal-collection-row" style={{ flexDirection: "column", alignItems: "flex-start", gap: 6 }}>
                  <div style={{ display: "flex", justifyContent: "space-between", width: "100%", alignItems: "center" }}>
                    <span style={{ fontWeight: 600, fontSize: 14, color: "#1a1a1a" }}>
                      {job.docket_ids.length > 0
                        ? `${job.docket_ids.length} docket${job.docket_ids.length !== 1 ? "s" : ""}`
                        : "All dockets"}
                    </span>
                    <span style={{ fontSize: 12, padding: "3px 10px", borderRadius: 99, fontWeight: 600, ...style }}>
                      {job.status.charAt(0).toUpperCase() + job.status.slice(1)}
                    </span>
                  </div>
                  <div style={{ fontSize: 12, color: "#888" }}>
                    Format: {job.format.toUpperCase()} · Requested: {new Date(job.created_at).toLocaleString()}
                  </div>
                  {job.status === "ready" && (
                    <button
                      className="modal-btn-add"
                      style={{ marginTop: 4 }}
                      onClick={() => { window.location.href = `/download/${job.job_id}`; }}
                    >
                      Download
                    </button>
                  )}
                </div>
              );
            })}
          </div>
        )}

        <div className="modal-actions">
          <button className="modal-btn-back" onClick={onClose}>Close</button>
        </div>
      </div>
    </div>
  );
}