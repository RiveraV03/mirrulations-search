import { useEffect, useState } from "react";
import {
  getCollections,
  createCollection,
  deleteCollection,
  removeDocketFromCollection,
  getCollectionDockets,
} from "../api/collectionsApi";
import DownloadModal from "./DownloadModal";
import "../styles/collections.css";
import { ArrowLeftIcon, ArrowRightIcon } from "@phosphor-icons/react";
const ECFR_URL = "https://www.ecfr.gov";
const MAX_DOCKETS = 10;

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
  const [dockets, setDockets] = useState([]);
  const [pagination, setPagination] = useState(null);
  const [page, setPage] = useState(1);
  const [docketsLoading, setDocketsLoading] = useState(false);
  const [showDownloadModal, setShowDownloadModal] = useState(false);

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

  const loadDockets = async (collectionId, pageNum) => {
    setDocketsLoading(true);
    try {
        const { results, pagination: p } = await getCollectionDockets(collectionId, pageNum);
        setDockets(results);
        setPagination(p);
    } catch (err) {
        if (err.message === "UNAUTHORIZED") setUnauthorized(true);
        else setError("Failed to load dockets.");
    } finally {
        setDocketsLoading(false);
    }
};

  useEffect(() => {
      if (!selectedCollectionId) return;
      setPage(1);
      loadDockets(selectedCollectionId, 1);
  }, [selectedCollectionId]);

  useEffect(() => {
      if (!selectedCollectionId) return;
      loadDockets(selectedCollectionId, page);
  }, [page]);

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
      loadDockets(collectionId, page);

    } catch (err) {
      if (err.message === "UNAUTHORIZED") {
        setUnauthorized(true);
      } else {
        setError("Failed to remove docket from collection.");
      }
    }
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
  const overLimit = selectedDocketIds.length > MAX_DOCKETS;

  const handleDownloadAll = () => {
    if (!selectedCollection || overLimit) return;
    setShowDownloadModal(true);
  };

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
                Showing dockets in "{selectedCollection.name}" • {pagination?.totalResults ?? 0}{" "}
                docket{(pagination?.totalResults ?? 0) === 1 ? "" : "s"} found
                {overLimit && (
                  <span style={{ color: "#c0392b", marginLeft: 8, fontWeight: 600 }}>
                    · Limit of {MAX_DOCKETS} reached — remove dockets to download
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
                  disabled={!pagination?.totalResults || overLimit}
                  title={overLimit ? `Collections are limited to ${MAX_DOCKETS} dockets for download` : ""}
                >
                  Download All
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

            {docketsLoading ? (
                <p className="collections-muted">Loading dockets...</p>
            ) : dockets.length === 0 ? (
                <p className="collections-muted">No dockets in this collection.</p>
            ) : (
                <>
                <div className="collection-results">
                    {dockets.map((item) => {
                       return (
                          <article key={item.docket_id} className="result-card">
                            <h3 className="result-title">{item.docket_title}</h3>
                            <div className="result-meta">
                              <p><strong>Agency:</strong> {item.agency_id}</p>
                              <p><strong>Docket-ID:</strong> {item.docket_id}</p>
                              <p><strong>Docket type:</strong> {item.docket_type}</p>
                              <p>
                                <strong>CFR:</strong>{" "}
                                {item.cfrPart && item.cfrPart.length > 0 ? (
                                  item.cfrPart.map((p, idx) => (
                                    <span key={idx}>
                                      <a href={p.link} target="_blank" rel="noopener noreferrer">
                                        { p.title != null ? `${p.title} Part ${p.part}` : p.part}
                                      </a>
                                      {idx < item.cfrPart.length - 1 && ", "}
                                    </span>
                                  ))
                                ) : (
                                  <a href={ECFR_URL} target="_blank" rel="noopener noreferrer">None</a>
                                )}
                              </p>
                              <p><strong>Last modified date:</strong> {item.modify_date}</p>
                            </div>
                            {editMode && (
                              <button className="collection-remove-docket"
                                onClick={() => handleRemoveDocket(selectedCollection.collection_id, item.docket_id)}>
                                Remove from Collection
                              </button>
                            )}
                          </article>
                  );
                })}
                </div>
                <div className="pagination-div">
                    <button className="page-button" disabled={!pagination?.hasPrev}
                        onClick={() => setPage(p => p - 1)}>
                        <ArrowLeftIcon color="white" size={32} />
                    </button>
                    <span className="page-info">Page {pagination?.page} of {pagination?.totalPages}</span>
                    <button className="page-button" disabled={!pagination?.hasNext}
                        onClick={() => setPage(p => p + 1)}>
                        <ArrowRightIcon color="white" size={32} />
                    </button>
                </div>
                </>
            )}
          </>
        )}
      </div>

      {showDownloadModal && (
        <DownloadModal
          collectionName={selectedCollection?.name}
          docketIds={selectedDocketIds}
          onClose={() => setShowDownloadModal(false)}
        />
      )}
    </section>
  );
}