import { useEffect, useState } from "react";
import {
  getCollections,
  createCollection,
  deleteCollection,
  removeDocketFromCollection,
} from "../api/collectionsApi";
import DownloadModal from "./DownloadModal";
import "../styles/collections.css";

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
  const [showDownloadModal, setShowDownloadModal] = useState(false);
  const [checkedDockets, setCheckedDockets] = useState(new Set());

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
    setCheckedDockets(new Set());
  }, [selectedCollectionId, editMode]);

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

  const toggleCheckedDocket = (docketId) => {
    setCheckedDockets((prev) => {
      const next = new Set(prev);
      next.has(docketId) ? next.delete(docketId) : next.add(docketId);
      return next;
    });
  };

  if (unauthorized) {
    return (
      <section className="collections-page">
        <h1 className="collections-title">My Collections</h1>
        <p>Please <a href="/login">log in</a> to view collections.</p>
      </section>
    );
  }

  const selectedCollection = collections.find(
    (collection) => collection.collection_id === selectedCollectionId
  );
  const selectedDocketIds = selectedCollection?.docket_ids || [];

  const docketsForModal =
    checkedDockets.size > 0 ? Array.from(checkedDockets) : selectedDocketIds;

  const handleDownloadAll = () => {
    if (!selectedCollection) return;
    setShowDownloadModal(true);
  };

  const DocketCheckbox = ({ docketId }) => {
    const checked = checkedDockets.has(docketId);
    return (
      <div
        onClick={(e) => { e.stopPropagation(); toggleCheckedDocket(docketId); }}
        style={{
          width: 20,
          height: 20,
          borderRadius: 4,
          border: `2px solid ${checked ? "#6b63d4" : "#ccc"}`,
          background: checked ? "#6b63d4" : "white",
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
          flexShrink: 0,
          cursor: "pointer",
          transition: "all 0.15s",
          alignSelf: "flex-start",
          marginTop: 2,
        }}
      >
        {checked && (
          <svg width="11" height="9" viewBox="0 0 10 8" fill="none">
            <path d="M1 4L3.5 6.5L9 1" stroke="white" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" />
          </svg>
        )}
      </div>
    );
  };

  const checkedCount = checkedDockets.size;

  return (
    <section className="collections-page collections-layout">
      <aside className="collections-sidebar">
        <div className="collections-sidebar-header">
          <div>
            <h2>My Collections</h2>
            <p>All your saved dockets in one place!</p>
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
                Showing dockets in "{selectedCollection.name}" •{" "}
                {selectedDocketIds.length} docket{selectedDocketIds.length === 1 ? "" : "s"} found
                {checkedCount > 0 && (
                  <span style={{ color: "#6b63d4", marginLeft: 8, fontWeight: 600 }}>
                    · {checkedCount} selected
                  </span>
                )}
              </p>
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
                  {checkedCount > 0 ? `Download (${checkedCount})` : "Download All"}
                </button>
                {editMode && (
                  <button
                    type="button"
                    className="collection-delete"
                    onClick={() => handleDeleteCollection(selectedCollection.collection_id)}
                  >
                    Delete Collection
                  </button>
                )}
              </div>
            </div>

            {selectedDocketIds.length === 0 ? (
              <p className="collections-muted">No dockets in this collection.</p>
            ) : (
              <div className="collection-results">
                {selectedDocketIds.map((docketId) => (
                  <article
                    key={docketId}
                    className="collection-result-card"
                    style={{ display: "flex", alignItems: "flex-start", gap: 12 }}
                  >
                    <div style={{ flex: 1 }}>
                      <h3>{docketId}</h3>
                      <p><strong>Agency:</strong> CMS</p>
                      <p><strong>Docket-ID:</strong> {docketId}</p>
                      <p><strong>Docket type:</strong> Rulemaking</p>
                      {editMode && (
                        <button
                          type="button"
                          className="collection-remove-docket"
                          onClick={() =>
                            handleRemoveDocket(selectedCollection.collection_id, docketId)
                          }
                        >
                          Remove from Collection
                        </button>
                      )}
                    </div>
                    <DocketCheckbox docketId={docketId} />
                  </article>
                ))}
              </div>
            )}
          </>
        )}
      </div>

      {showDownloadModal && (
        <DownloadModal
          collectionName={selectedCollection?.name}
          docketIds={docketsForModal}
          onClose={() => setShowDownloadModal(false)}
        />
      )}
    </section>
  );
}