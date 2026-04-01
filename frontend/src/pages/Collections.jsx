import React, { useState, useEffect } from 'react';

export default function Collection() {
    const [collections, setCollections] = useState([]);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);

    useEffect(() => {
        // Fetch user's collections
        const fetchCollections = async () => {
            try {
                const response = await fetch('/api/collections');
                if (!response.ok) throw new Error('Failed to fetch collections');
                const data = await response.json();
                setCollections(data);
            } catch (err) {
                setError(err.message);
            } finally {
                setLoading(false);
            }
        };

        fetchCollections();
    }, []);

    if (loading) return <div>Loading...</div>;
    if (error) return <div>Error: {error}</div>;

    return (
        <div className="collections-container">
            <h1>My Collections</h1>
            {collections.length === 0 ? (
                <p>No collections yet.</p>
            ) : (
                <div className="collections-grid">
                    {collections.map((collection) => (
                        <div key={collection.id} className="collection-card">
                            <h2>{collection.name}</h2>
                            <p>{collection.dockets?.length || 0} dockets</p>
                            <ul>
                                {collection.dockets?.map((docket) => (
                                    <li key={docket.id}>{docket.title}</li>
                                ))}
                            </ul>
                        </div>
                    ))}
                </div>
            )}
        </div>
    );
}