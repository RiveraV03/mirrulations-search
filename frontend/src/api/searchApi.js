export async function searchDockets(query, docket_type = '', agency = '', cfr_part = '') {
    // EncodeURIComponent allows for spaces, special chars, etc
	const response = await fetch(
        `/search/?str=${encodeURIComponent(query)}&docket_type=${encodeURIComponent(docket_type)}&agency=${encodeURIComponent(agency)}&cfr_part=${encodeURIComponent(cfr_part)}`
    )

	if (!response.ok) {
		throw new Error(`Search request failed: ${response.status}`)
	}

	return response.json()
}

