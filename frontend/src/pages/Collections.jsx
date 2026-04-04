import { useEffect, useState, useMemo, useRef } from "react";
import {
  getCollections,
  createCollection,
  deleteCollection,
  removeDocketFromCollection,
  getDocketsByIds,
} from "../api/collectionsApi";
import "../styles/collections.css";

const ECFR_URL = "https://www.ecfr.gov";
const EMPTY_DOCKET_IDS = [];

/** Stable archive: last modified (newest first) or title A–Z. */
const SORT_MODIFIED = "modified";
const SORT_ALPHABETICAL = "alphabetical";

export default function Collections() {
  const [collections, setCollections] = useState([]);
  const [loading, setLoading] = useState(true);
  const [submitting, setSubmitting] = useState(false);
  const [newCollectionName, setNewCollectionName] = useState("");
  const [showCreateForm, setShowCreateForm] = useState(false);
  const [selectedCollectionId, setSelectedCollectionId] = useState(null);
  const [editMode, setEditMode] = useState(false);
  const [error, setError] = useState("");
  const [unauthorized, setUnauthorized] = useState(false);
  const [docketDetails, setDocketDetails] = useState({});
  const [sortMode, setSortMode] = useState(SORT_MODIFIED);
  const [sortMenuOpen, setSortMenuOpen] = useState(false);
  const sortMenuRef = useRef(null);

  const selectedCollection = collections.find(
    (c) => c.collection_id === selectedCollectionId
  );
  const selectedDocketIds = selectedCollection?.docket_ids ?? EMPTY_DOCKET_IDS;

  useEffect(() => {
    function handlePointerDown(e) {
      if (!sortMenuRef.current?.contains(e.target)) {
        setSortMenuOpen(false);
      }
    }
    document.addEventListener("mousedown", handlePointerDown);
    document.addEventListener("touchstart", handlePointerDown);
    return () => {
      document.removeEventListener("mousedown", handlePointerDown);
      document.removeEventListener("touchstart", handlePointerDown);
    };
  }, []);

  const loadCollections = async () => {
    setLoading(true);
    setError("");
    try {
      const data = await getCollections();
      setCollections(Array.isArray(data) ? data : []);
    } catch (err) {
      if (err.message === "UNAUTHORIZED") {
        setUnauthorized(true);
      } else {
        setError("Failed to load collections.");
      }
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    loadCollections();
  }, []);

  useEffect(() => {
    if (collections.length === 0) {
      setSelectedCollectionId(null);
      return;
    }
    const hasSelected = collections.some(
      (collection) => collection.collection_id === selectedCollectionId
    );
    if (!hasSelected) {
      setSelectedCollectionId(collections[0].collection_id);
    }
  }, [collections, selectedCollectionId]);

  useEffect(() => {
    if (!selectedDocketIds.length) return;
    getDocketsByIds(selectedDocketIds).then((results) => {
      setDocketDetails((prev) => {
        const next = { ...prev };
        results.forEach((d) => {
          next[d.docket_id] = d;
        });
        return next;
      });
    });
  }, [selectedCollectionId, selectedDocketIds]);

  const handleCreate = async (e) => {
    e.preventDefault();
    const trimmedName = newCollectionName.trim();
    if (!trimmedName) return;

    setSubmitting(true);
    setError("");
    try {
      const created = await createCollection(trimmedName);
      const newCollection = {
        collection_id: created.collection_id,
        name: trimmedName,
        docket_ids: [],
      };
      setCollections((prev) => [...prev, newCollection]);
      setSelectedCollectionId(created.collection_id);
      setShowCreateForm(false);
      setNewCollectionName("");
    } catch (err) {
      if (err.message === "UNAUTHORIZED") {
        setUnauthorized(true);
      } else {
        setError("Failed to create collection.");
      }
    } finally {
      setSubmitting(false);
    }
  };

  const handleDeleteCollection = async (collectionId) => {
    setError("");
    try {
      await deleteCollection(collectionId);
      setCollections((prev) =>
        prev.filter((col) => col.collection_id !== collectionId)
      );
      setEditMode(false);
    } catch (err) {
      if (err.message === "UNAUTHORIZED") {
        setUnauthorized(true);
      } else {
        setError("Failed to delete collection.");
      }
    }
  };

  const handleRemoveDocket = async (collectionId, docketId) => {
    setError("");
    try {
      await removeDocketFromCollection(collectionId, docketId);
      setCollections((prev) =>
        prev.map((col) =>
          col.collection_id === collectionId
            ? {
                ...col,
                docket_ids: (col.docket_ids || []).filter((id) => id !== docketId),
              }
            : col
        )
      );
    } catch (err) {
      if (err.message === "UNAUTHORIZED") {
        setUnauthorized(true);
      } else {
        setError("Failed to remove docket from collection.");
      }
    }
  };

  const sortedDocketIds = useMemo(() => {
    const ids = [...selectedDocketIds];
    const getTime = (docketId) => {
      const raw = docketDetails[docketId]?.modify_date;
      if (raw == null || raw === "") return null;
      const t = Date.parse(String(raw));
      return Number.isNaN(t) ? null : t;
    };
    const getTitle = (docketId) => {
      const t = docketDetails[docketId]?.docket_title;
      return (t != null ? String(t) : "").trim();
    };
    ids.sort((a, b) => {
      if (sortMode === SORT_ALPHABETICAL) {
        const cmp = getTitle(a).localeCompare(getTitle(b), undefined, {
          sensitivity: "base",
        });
        if (cmp !== 0) return cmp;
        return String(a).localeCompare(String(b));
      }
      const ta = getTime(a);
      const tb = getTime(b);
      if (ta == null && tb == null) return 0;
      if (ta == null) return 1;
      if (tb == null) return -1;
      return tb - ta;
    });
    return ids;
  }, [selectedDocketIds, docketDetails, sortMode]);

  const sortLabel =
    sortMode === SORT_ALPHABETICAL ? "Alphabetical" : "Last modified";

  if (unauthorized) {
    return (
      <section className="collections-page">
        <h1 className="collections-title">My Collections</h1>
        <p>Please <a href="/login">log in</a> to view collections.</p>
      </section>
    );
  }

  const handleDownloadAll = () => {
    if (!selectedCollection) return;
    const lines = [
      `Collection: ${selectedCollection.name}`,
      "",
      ...sortedDocketIds.map((docketId) => docketId),
    ];
    const blob = new Blob([lines.join("\n")], { type: "text/plain;charset=utf-8" });
    const url = URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = url;
    link.download = `${selectedCollection.name.replace(/\s+/g, "-").toLowerCase()}-dockets.txt`;
    link.click();
    URL.revokeObjectURL(url);
  };

  return (
    <section className="collections-page collections-layout">
      <aside className="collections-sidebar">
        <div className="collections-sidebar-header">
          <div>
            <h2>My Collections</h2>
            <p>Save and revisit dockets—stable reference, not live search data.</p>
          </div>
          <button
            type="button"
            className="collections-plus"
            onClick={() => setShowCreateForm((prev) => !prev)}
          >
            +
          </button>
        </div>

        {showCreateForm && (
          <form className="collections-create-inline" onSubmit={handleCreate}>
            <input
              type="text"
              placeholder="New collection name"
              value={newCollectionName}
              onChange={(e) => setNewCollectionName(e.target.value)}
            />
            <button
              type="submit"
              className="btn btn-primary"
              disabled={submitting || !newCollectionName.trim()}
            >
              {submitting ? "Creating..." : "Create"}
            </button>
          </form>
        )}

        {loading ? (
          <p className="collections-muted">Loading collections...</p>
        ) : collections.length === 0 ? (
          <p className="collections-muted">No collections yet.</p>
        ) : (
          <div className="collections-nav-list">
            {collections.map((collection) => (
              <button
                key={collection.collection_id}
                type="button"
                className={`collections-nav-item ${
                  selectedCollectionId === collection.collection_id ? "is-active" : ""
                }`}
                onClick={() => {
                  setSelectedCollectionId(collection.collection_id);
                  setEditMode(false);
                }}
              >
                {collection.name}
              </button>
            ))}
          </div>
        )}
      </aside>

      <div className="collections-content">
        {error && <p className="collections-error">{error}</p>}

        {!selectedCollection ? (
          <p className="collections-muted">Select or create a collection to continue.</p>
        ) : (
          <>
            <h1 className="collections-title">{selectedCollection.name}</h1>
            <div className="collections-toolbar">
              <p className="collections-summary">
                Saved dockets in &quot;{selectedCollection.name}&quot; •{" "}
                {selectedDocketIds.length} docket
                {selectedDocketIds.length === 1 ? "" : "s"}
              </p>
              <div className="collections-toolbar-right">
                <div className="collections-sort-wrap" ref={sortMenuRef}>
                  <button
                    type="button"
                    className="collections-sort-trigger"
                    aria-expanded={sortMenuOpen}
                    aria-haspopup="listbox"
                    aria-label="Sort saved dockets"
                    onClick={() => setSortMenuOpen((o) => !o)}
                  >
                    Sort
                    <span className="collections-sort-trigger-value" aria-hidden>
                      {sortLabel}
                    </span>
                  </button>
                  {sortMenuOpen && (
                    <ul className="collections-sort-menu" role="listbox">
                      <li role="none">
                        <button
                          type="button"
                          role="option"
                          aria-selected={sortMode === SORT_MODIFIED}
                          className={
                            sortMode === SORT_MODIFIED
                              ? "collections-sort-menu-item is-active"
                              : "collections-sort-menu-item"
                          }
                          onClick={() => {
                            setSortMode(SORT_MODIFIED);
                            setSortMenuOpen(false);
                          }}
                        >
                          Last modified
                        </button>
                      </li>
                      <li role="none">
                        <button
                          type="button"
                          role="option"
                          aria-selected={sortMode === SORT_ALPHABETICAL}
                          className={
                            sortMode === SORT_ALPHABETICAL
                              ? "collections-sort-menu-item is-active"
                              : "collections-sort-menu-item"
                          }
                          onClick={() => {
                            setSortMode(SORT_ALPHABETICAL);
                            setSortMenuOpen(false);
                          }}
                        >
                          Alphabetical
                        </button>
                      </li>
                    </ul>
                  )}
                </div>
                <div className="collections-actions">
                  <button
                    type="button"
                    className="collections-action-btn collections-action-btn-secondary"
                    onClick={() => setEditMode((prev) => !prev)}
                  >
                    {editMode ? "Done" : "Edit"}
                  </button>
                  <button
                    type="button"
                    className="collections-action-btn"
                    onClick={handleDownloadAll}
                    disabled={selectedDocketIds.length === 0}
                  >
                    Download All
                  </button>
                  {editMode && (
                    <button
                      type="button"
                      className="collection-delete"
                      onClick={() =>
                        handleDeleteCollection(selectedCollection.collection_id)
                      }
                    >
                      Delete Collection
                    </button>
                  )}
                </div>
              </div>
            </div>

            {selectedDocketIds.length === 0 ? (
              <p className="collections-muted">No dockets in this collection.</p>
            ) : (
              <div className="collection-results">
                {sortedDocketIds.map((docketId) => {
                  const item = docketDetails[docketId];
                  if (!item) {
                    return (
                      <div key={docketId} className="result-card">
                        <p>Loading...</p>
                      </div>
                    );
                  }
                  return (
                    <article key={docketId} className="result-card">
                      <h3 className="result-title">{item.docket_title}</h3>
                      <div className="result-meta">
                        <p>
                          <strong>Agency:</strong> {item.agency_id}
                        </p>
                        <p>
                          <strong>Docket-ID:</strong> {item.docket_id}
                        </p>
                        <p>
                          <strong>Docket type:</strong> {item.docket_type}
                        </p>
                        <p>
                          <strong>CFR:</strong>{" "}
                          {item.cfrPart && item.cfrPart.length > 0 ? (
                            item.cfrPart.map((p, idx) => (
                              <span key={idx}>
                                <a
                                  href={p.link}
                                  target="_blank"
                                  rel="noopener noreferrer"
                                >
                                  {p.title != null
                                    ? `${p.title} Part ${p.part}`
                                    : p.part}
                                </a>
                                {idx < item.cfrPart.length - 1 && ", "}
                              </span>
                            ))
                          ) : (
                            <a
                              href={ECFR_URL}
                              target="_blank"
                              rel="noopener noreferrer"
                            >
                              None
                            </a>
                          )}
                        </p>
                        <p>
                          <strong>Last modified date:</strong> {item.modify_date}
                        </p>
                      </div>
                      {editMode && (
                        <button
                          className="collection-remove-docket"
                          onClick={() =>
                            handleRemoveDocket(
                              selectedCollection.collection_id,
                              docketId
                            )
                          }
                        >
                          Remove from Collection
                        </button>
                      )}
                    </article>
                  );
                })}
              </div>
            )}
          </>
        )}
      </div>
    </section>
  );
}
