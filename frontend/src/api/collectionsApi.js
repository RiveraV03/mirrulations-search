export async function getCollections() { // Gets all the current collections
    const response = await fetch("/collections");
    if (response.status === 401) throw new Error("UNAUTHORIZED");
    if (!response.ok) throw new Error(`Failed to fetch collections: ${response.status}`);
    return response.json();
}

export async function createCollection(name) { // Creates a new collection with the given name
    const response = await fetch("/collections", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ name }),
    });
    if (response.status === 401) throw new Error("UNAUTHORIZED");
    if (!response.ok) throw new Error(`Failed to create collection: ${response.status}`);
    return response.json();
}

export async function addDocketToCollection(collectionId, docketId) { // Adds a docket to a specific collection
    const response = await fetch(`/collections/${collectionId}/dockets`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ docket_id: docketId }),
    });
    if (response.status === 401) throw new Error("UNAUTHORIZED");
    if (!response.ok) throw new Error(`Failed to add docket to collection: ${response.status}`);
}