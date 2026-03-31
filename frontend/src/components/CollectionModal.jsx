import { useState, useEffect } from "react";
import { getCollections, createCollection, addDocketToCollection } from "../api/collectionsApi";
import "../styles/collections.css"
 
export default function CollectionModal({ docketId, onClose }) {
    const [collections, setCollections] = useState([]);
    const [selected, setSelected] = useState(new Set());
    const [view, setView] = useState("select");
    const [newName, setNewName] = useState("");
    const [loading, setLoading] = useState(true);
    const [submitting, setSubmitting] = useState(false);
    const [error, setError] = useState(null);
 
    useEffect(() => {
        getCollections()
            .then(setCollections)
            .catch(() => setError("Failed to load collections."))
            .finally(() => setLoading(false));
    }, []);
 
    const toggleSelected = (id) => {
        setSelected((prev) => {
            const next = new Set(prev);
            next.has(id) ? next.delete(id) : next.add(id);
            return next;
        });
    };
 
    const handleAdd = async () => {
        if (selected.size === 0) return; // Does nothing if nothing is selected
        setSubmitting(true); // disables the Add button while adding
        setError(null);
        try {
            await Promise.all( // Promise makes all of the api calls run together and waits for them to finish
                Array.from(selected).map((id) => addDocketToCollection(id, docketId))
            );
            onClose();
        } catch {
            setError("Failed to add to collection(s). Please try again.");
        } finally {
            setSubmitting(false); //re-enable add button
        }
    };
 
    const handleCreate = async () => {
        const trimmed = newName.trim(); // Don't allow creating collections with just whitespace
        if (!trimmed) return;
        setSubmitting(true);
        setError(null);
        try {
            const created = await createCollection(trimmed);
            await addDocketToCollection(created.collection_id, docketId);
            onClose();
        } catch {
            setError("Failed to create collection. Please try again.");
        } finally {
            setSubmitting(false);
        }
    };
 
    return (
        <div className="modal-backdrop" onClick={onClose}> {/* This makes it so clicking outside of the modal popup closes it */}
            <div className="modal" onClick={(e) => e.stopPropagation()}> {/* This makes it so clicking inside doesn't close it */}
                {view === "select" ? (
                    <>
                        <div className="modal-header">
                            <button className="modal-btn-create" onClick={() => { setView("create"); setError(null); }}>+</button>
                        </div>
                        <h2 className="modal-title">Which Collections would you like to add to?</h2>
                        {loading && <p className="modal-loading">Loading...</p>}
                        {error && <p className="modal-error">{error}</p>}
                        <div className="modal-collection-list">
                            {collections.map((col) => (
                                <label key={col.collection_id} className="modal-collection-row">
                                    <span className="modal-collection-name">{col.name}</span>
                                    <input
                                        type="checkbox"
                                        checked={selected.has(col.collection_id)}
                                        onChange={() => toggleSelected(col.collection_id)}/>
                                </label>
                            ))}
                        </div>
                        <div className="modal-actions">
                            <button
                                className="modal-btn-add"
                                disabled={selected.size === 0 || submitting}
                                onClick={handleAdd}>
                                {submitting ? "Adding..." : "Add"}
                            </button>
                        </div>
                    </>
                ) : (
                    <>
                        <h2 className="modal-title">Name your new collection</h2>
                        {error && <p className="modal-error">{error}</p>}
                        <input
                            className="modal-input"
                            type="text"
                            placeholder="Collection name"
                            value={newName}
                            onChange={(e) => setNewName(e.target.value)}
                            onKeyDown={(e) => e.key === "Enter" && handleCreate()}
                            autoFocus
                        />
                        <div className="modal-actions">
                            <button
                                className="modal-btn-back"
                                onClick={() => { setView("select"); setError(null); }}>Back</button>
                            <button
                                className="modal-btn-add"
                                disabled={!newName.trim() || submitting}
                                onClick={handleCreate}>
                                {submitting ? "Creating..." : "Create & Add"}
                            </button>
                        </div>
                    </>
                )}
            </div>
        </div>
    );
}
 