import { useState } from "react";
import "../styles/collections.css";

const PACKAGE_OPTIONS = [
  {
    id: "metadata",
    label: "Metadata",
    description: "Docket titles, agency info, dates, document types, status",
  },
  {
    id: "documents",
    label: "Documents",
    description: "Federal Register notices, proposed rules, final rules (no comments)",
  },
  {
    id: "comments",
    label: "Comments",
    description: "Public comment text submitted on dockets",
  },
  {
    id: "attachments",
    label: "Attachments & binary files",
    description: "PDFs, Word docs, images uploaded with comments or documents",
  },
  {
    id: "extracted_text",
    label: "Extracted text",
    description: "Plain-text extraction from binary files (where available)",
  },
];

const FORMAT_OPTIONS = [
  { id: "Raw", label: "RAW" },
  { id: "csv", label: "CSV" },
];

export default function DownloadModal({ collectionName, docketIds, onClose }) {
  const [selected, setSelected] = useState(new Set(["metadata"]));
  const [format, setFormat] = useState("json");
  const [error, setError] = useState(null);

  const isAll = !docketIds || docketIds.length === 0;

  const toggleSelected = (id) => {
    setSelected((prev) => {
      const next = new Set(prev);
      next.has(id) ? next.delete(id) : next.add(id);
      return next;
    });
  };

  const handleDownload = () => {
    if (selected.size === 0) return;
    setError(null);
    try {
      const safeName = (collectionName || "collection")
        .replace(/\s+/g, "-")
        .toLowerCase();
      const lines = [
        `Collection: ${collectionName || ""}`,
        `Include: ${Array.from(selected).join(", ")}`,
        `Format: ${format}`,
        `Dockets (${isAll ? "all" : docketIds.length}):`,
        "",
        ...(isAll ? ["(all dockets in collection)"] : docketIds),
      ];
      const blob = new Blob([lines.join("\n")], { type: "text/plain;charset=utf-8" });
      const url = URL.createObjectURL(blob);
      const link = document.createElement("a");
      link.href = url;
      link.download = `${safeName}-dockets.txt`;
      link.click();
      URL.revokeObjectURL(url);
      onClose();
    } catch (err) {
      setError("Download failed. Please try again.");
      console.error(err);
    }
  };

 
  const Checkbox = ({ checked, onChange }) => (
    <div
      onClick={onChange}
      style={{
        width: 18,
        height: 18,
        borderRadius: 4,
        border: `2px solid ${checked ? "#6b63d4" : "#ccc"}`,
        background: checked ? "#6b63d4" : "white",
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        flexShrink: 0,
        cursor: "pointer",
        transition: "all 0.15s",
        marginTop: 2,
      }}
    >
      {checked && (
        <svg width="10" height="8" viewBox="0 0 10 8" fill="none">
          <path d="M1 4L3.5 6.5L9 1" stroke="white" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" />
        </svg>
      )}
    </div>
  );

  return (
    <div className="modal-backdrop" onClick={onClose}>
      <div className="modal" onClick={(e) => e.stopPropagation()}>

        <h2 className="modal-title">
          {isAll
            ? "Download all dockets"
            : `Download ${docketIds.length} selected docket${docketIds.length !== 1 ? "s" : ""}`}
        </h2>

        {error && <p className="modal-error">{error}</p>}

        <p style={{ fontSize: 11, fontWeight: 700, letterSpacing: "0.07em", textTransform: "uppercase", color: "#aaa", margin: "4px 0 10px" }}>
          What to include
        </p>
        <div className="modal-collection-list">
          {PACKAGE_OPTIONS.map((opt) => (
            <div
              key={opt.id}
              className="modal-collection-row"
              style={{ alignItems: "flex-start", gap: 12, cursor: "pointer" }}
              onClick={() => toggleSelected(opt.id)}
            >
              <div style={{ flex: 1 }}>
                <div style={{ fontWeight: 600, fontSize: 14, color: "#1a1a1a" }}>
                  {opt.label}
                </div>
                <div style={{ fontSize: 12, color: "#888", marginTop: 2, lineHeight: 1.4 }}>
                  {opt.description}
                </div>
              </div>
              <Checkbox
                checked={selected.has(opt.id)}
                onChange={() => toggleSelected(opt.id)}
              />
            </div>
          ))}
        </div>

        <p style={{ fontSize: 11, fontWeight: 700, letterSpacing: "0.07em", textTransform: "uppercase", color: "#aaa", margin: "16px 0 10px" }}>
          Output format
        </p>
        <div style={{ display: "flex", gap: 8, flexWrap: "wrap", marginBottom: 8 }}>
          {FORMAT_OPTIONS.map((f) => (
            <button
              key={f.id}
              type="button"
              onClick={() => setFormat(f.id)}
              style={{
                padding: "6px 18px",
                borderRadius: 99,
                border: `1.5px solid ${format === f.id ? "#6b63d4" : "#ddd"}`,
                background: format === f.id ? "#eeedf8" : "white",
                color: format === f.id ? "#4c45a0" : "#666",
                fontWeight: format === f.id ? 600 : 400,
                fontSize: 13,
                cursor: "pointer",
                transition: "all 0.15s",
              }}
            >
              {f.label}
            </button>
          ))}
        </div>

        <div className="modal-actions">
          <button className="modal-btn-back" onClick={onClose}>Cancel</button>
          <button
            className="modal-btn-add"
            disabled={selected.size === 0}
            onClick={handleDownload}
          >
            Download
          </button>
        </div>
      </div>
    </div>
  );
}