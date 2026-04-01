import json
from dataclasses import dataclass
from typing import List, Dict, Any, Tuple
import os
import psycopg2
from opensearchpy import OpenSearch

try:
    import boto3
except ImportError:
    boto3 = None

try:
    from dotenv import load_dotenv
except ImportError:
    LOAD_DOTENV = None
else:
    LOAD_DOTENV = load_dotenv


@dataclass(frozen=True)
class DBLayer:
    conn: Any = None

    def search(
            self,
            query: str,
            docket_type_param: str = None,
            agency: List[str] = None,
            cfr_part_param: List[str] = None,
            start_date: str = None,
            end_date: str = None) \
            -> List[Dict[str, Any]]:
        if self.conn is None:
            return []
        return self._search_dockets(query, docket_type_param, agency, cfr_part_param,
                                    start_date, end_date)

    def _get_cfr_docket_ids(self, cfr_part_param: List[Dict[str, str]]) -> set:
        clauses = " OR ".join(
            "(cfr_title ILIKE %s AND cfr_part ILIKE %s)"
            for _ in cfr_part_param
        )
        sql = f"SELECT DISTINCT docket_id FROM federal_register_documents WHERE ({clauses})"
        params = []
        for c in cfr_part_param:
            params.append(f"%{c['title']}%")
            params.append(f"%{c['part']}%")
        with self.conn.cursor() as cur:
            cur.execute(sql, params)
            return {row[0] for row in cur.fetchall()}

    def _search_dockets_postgres(  # pylint: disable=too-many-locals
            self, query: str, docket_type_param: str = None,
            agency: List[str] = None,
            cfr_part_param: List[str] = None,
            start_date: str = None,
            end_date: str = None) -> List[Dict[str, Any]]:
        sql = """
            SELECT DISTINCT
                d.docket_id,
                d.docket_title,
                d.agency_id,
                d.docket_type,
                d.modify_date,
                cp.title,
                cp.cfrpart,
                l.link
            FROM dockets d
            JOIN documents doc ON doc.docket_id = d.docket_id
            LEFT JOIN cfrparts cp ON cp.document_id = doc.document_id
            LEFT JOIN links l ON l.title = cp.title AND l.cfrpart = cp.cfrpart
            WHERE d.docket_title ILIKE %s
        """
        params = [f"%{(query or '').strip().lower()}%"]

        if docket_type_param:
            sql += " AND d.docket_type = %s"
            params.append(docket_type_param)

        if agency:
            clauses = " OR ".join("d.agency_id ILIKE %s" for _ in agency)
            sql += f" AND ({clauses})"
            params.extend(f"%{a}%" for a in agency)

        sql += " ORDER BY d.modify_date DESC, d.docket_id, cp.title, cp.cfrpart LIMIT 50"

        with self.conn.cursor() as cur:
            cur.execute(sql, params)
            dockets = {}
            for row in cur.fetchall():
                self._process_docket_row(dockets, row)
            results = [
                {**d, "cfr_refs": list(d["cfr_refs"].values())}
                for d in dockets.values()
            ]

        if cfr_part_param:
            cfr_docket_ids = self._get_cfr_docket_ids(cfr_part_param)
            results = [r for r in results if r["docket_id"] in cfr_docket_ids]

        return results

    def _search_dockets_by_title(self, query: str) -> set:
        """
        Compile a list of docket ids of the dockets
        whose title matches the search term. Returns a set of unique ids.
        """
        sql = "SELECT docket_id FROM dockets WHERE docket_title ILIKE %s"
        with self.conn.cursor() as cur:
            cur.execute(sql, [f"%{(query or '').strip().lower()}%"])
            return {row[0] for row in cur.fetchall()}

    def _search_dockets_by_cfr(self, cfr_part_param: List[Dict[str, str]]) -> set:
        """
        Compile a list of docket ids of the dockets whose
        cfr parts match the filter parameters. Returns a set of unique ids.
        """
        if not cfr_part_param:
            return set()
        clauses = " OR ".join(
            "(cfr_title ILIKE %s AND cfr_part ILIKE %s)"
            for _ in cfr_part_param
        )
        sql = f"SELECT DISTINCT docket_id FROM federal_register_documents WHERE ({clauses})"
        params = []
        for c in cfr_part_param:
            params.append(f"%{c['title']}%")
            params.append(f"%{c['part']}%")
        with self.conn.cursor() as cur:
            cur.execute(sql, params)
            return {row[0] for row in cur.fetchall()}

    def _search_dockets_by_document_title(self, query: str) -> set:
        """
        Compile a list of docket ids of the dockets that hold
        documents whose title matches the search term. (Docket title does not have
        to match). Return a set of unique ids.
        """
        sql = "SELECT DISTINCT docket_id FROM documents WHERE document_title ILIKE %s"
        with self.conn.cursor() as cur:
            cur.execute(sql, [f"%{(query or '').strip().lower()}%"])
            return {row[0] for row in cur.fetchall()}

    def _join_results(self, title_ids: set, cfr_ids: set, doc_title_ids: set) -> set:
        """
        Join the 3 sets together with the union operator so that
        there are no repeated docket ids listed.
        """
        return title_ids | cfr_ids | doc_title_ids

    def _apply_filters(self, sql: str, params: list, docket_type_param: str,
                       agency: List[str], start_date: str = None,
                       end_date: str = None) -> Tuple[str, list]:
        """Append optional WHERE clauses and return updated sql, params."""
        if docket_type_param:
            sql += " AND d.docket_type = %s"
            params.append(docket_type_param)
        if agency:
            clauses = " OR ".join("d.agency_id ILIKE %s" for _ in agency)
            sql += f" AND ({clauses})"
            params.extend(f"%{a}%" for a in agency)
        if start_date:
            sql += " AND d.modify_date::date >= %s::date"
            params.append(start_date)
        if end_date:
            sql += " AND d.modify_date::date <= %s::date"
            params.append(end_date)
        return sql, params

    def _fetch_dockets(self, docket_ids: set, docket_type_param: str,
                       agency: List[str], start_date: str = None,
                       end_date: str = None) -> List[Dict[str, Any]]:
        """Run the final JOIN query for the given docket_ids and return results."""
        sql = """
            SELECT DISTINCT
                d.docket_id, d.docket_title, d.agency_id, d.docket_type,
                d.modify_date, cp.title, cp.cfrpart, l.link
            FROM dockets d
            JOIN documents doc ON doc.docket_id = d.docket_id
            LEFT JOIN cfrparts cp ON cp.document_id = doc.document_id
            LEFT JOIN links l ON l.title = cp.title AND l.cfrpart = cp.cfrpart
            WHERE d.docket_id = ANY(%s)
        """
        params = [list(docket_ids)]
        sql, params = self._apply_filters(sql, params, docket_type_param, agency,
                                          start_date, end_date)
        sql += " ORDER BY d.modify_date DESC, d.docket_id, cp.title, cp.cfrpart LIMIT 50"

        with self.conn.cursor() as cur:
            cur.execute(sql, params)
            dockets = {}
            for row in cur.fetchall():
                self._process_docket_row(dockets, row)
            return [{**d, "cfr_refs": list(d["cfr_refs"].values())} for d in dockets.values()]

    def _search_dockets(self, query: str, docket_type_param: str = None,
                        agency: List[str] = None, cfr_part_param: List[str] = None,
                        start_date: str = None,
                        end_date: str = None) -> List[Dict[str, Any]]:
        title_ids = self._search_dockets_by_title(query)
        doc_title_ids = self._search_dockets_by_document_title(query)
        text_ids = title_ids | doc_title_ids

        if cfr_part_param:
            cfr_ids = self._search_dockets_by_cfr(cfr_part_param)
            docket_ids = text_ids & cfr_ids
        else:
            docket_ids = text_ids

        if not docket_ids:
            return []

        return self._fetch_dockets(docket_ids, docket_type_param, agency,
                                   start_date, end_date)

    @staticmethod
    def _process_docket_row(dockets, row):
        docket_id = row[0]
        if docket_id not in dockets:
            dockets[docket_id] = {
                "docket_id": row[0],
                "docket_title": row[1],
                "agency_id": row[2],
                "docket_type": row[3],
                "modify_date": row[4],
                "cfr_refs": {}
            }
        title, cfr_part, link = row[5], row[6], row[7]
        if title is not None and cfr_part is not None:
            if title not in dockets[docket_id]["cfr_refs"]:
                dockets[docket_id]["cfr_refs"][title] = {
                    "title": title,
                    "cfrParts": {}
                }
            dockets[docket_id]["cfr_refs"][title]["cfrParts"][cfr_part] = link

    @staticmethod
    def _build_docket_agg_query(agg_name: str, match_clauses: List[Dict]) -> Dict:
        """Build a docket-bucketed aggregation query with an inner filter."""
        return {
            "size": 0,
            "aggs": {
                "by_docket": {
                    "terms": {"field": "docketId.keyword", "size": 1000},
                    "aggs": {
                        agg_name: {
                            "filter": {
                                "bool": {
                                    "should": match_clauses,
                                    "minimum_should_match": 1
                                }
                            }
                        }
                    }
                }
            }
        }

    @staticmethod
    def _accumulate_counts(
            docket_counts: Dict, buckets: List, agg_name: str, count_key: str) -> None:
        """Add match counts from OpenSearch buckets into docket_counts in place."""
        for bucket in buckets:
            match_count = bucket[agg_name]["doc_count"]
            if match_count > 0:
                docket_id = bucket["key"]
                docket_counts.setdefault(
                    docket_id, {"document_match_count": 0, "comment_match_count": 0}
                )
                docket_counts[docket_id][count_key] += match_count

    def text_match_terms(
            self, terms: List[str], opensearch_client=None) -> List[Dict[str, Any]]:
        """
        Search OpenSearch for dockets containing the given terms.

        Searches:
        - documents index: title and comment fields
        - comments index: commentText field (phrase matching)
        - comments_extracted_text index: extractedText field (from PDF attachments)

        Returns list of {docket_id, document_match_count, comment_match_count}
        """
        if opensearch_client is None:
            opensearch_client = get_opensearch_connection()
        try:
            return self._run_text_match_queries(opensearch_client, terms)
        except (KeyError, AttributeError) as e:
            print(f"OpenSearch query failed: {e}")
            return []

    def _run_text_match_queries(
            self, opensearch_client, terms: List[str]) -> List[Dict[str, Any]]:
        """Execute all three OpenSearch queries and merge their results."""
        def buckets(resp):
            return resp["aggregations"]["by_docket"]["buckets"]

        docket_counts: Dict = {}
        doc_resp = opensearch_client.search(
            index="documents", body=self._build_docket_agg_query(
                "matching_docs",
                [{"multi_match": {"query": t, "fields": ["title", "comment"]}} for t in terms]
            ))
        comment_resp = opensearch_client.search(
            index="comments", body=self._build_docket_agg_query(
                "matching_comments",
                [{"match_phrase": {"commentText": t}} for t in terms]
            ))
        extracted_resp = opensearch_client.search(
            index="comments_extracted_text", body=self._build_docket_agg_query(
                "matching_extracted",
                [{"match_phrase": {"extractedText": t}} for t in terms]
            ))
        self._accumulate_counts(
            docket_counts, buckets(doc_resp), "matching_docs", "document_match_count"
        )
        self._accumulate_counts(
            docket_counts, buckets(comment_resp), "matching_comments", "comment_match_count"
        )
        self._accumulate_counts(
            docket_counts, buckets(extracted_resp), "matching_extracted", "comment_match_count"
        )
        return [{"docket_id": did, **counts} for did, counts in docket_counts.items()]


    def get_collections(self, user_email: str) -> List[Dict[str, Any]]:
        """Return all collections belonging to the given user."""
        if self.conn is None:
            return []
        sql = """
            SELECT c.collection_id, c.collection_name, c.user_email,
                   COALESCE(
                       json_agg(cd.docket_id) FILTER (WHERE cd.docket_id IS NOT NULL),
                       '[]'
                   ) AS docket_ids
            FROM collections c
            LEFT JOIN collection_dockets cd ON cd.collection_id = c.collection_id
            WHERE c.user_email = %s
            GROUP BY c.collection_id, c.collection_name, c.user_email
            ORDER BY c.collection_id
        """
        with self.conn.cursor() as cur:
            cur.execute(sql, (user_email,))
            return [
                {
                    "collection_id": row[0],
                    "name": row[1],
                    "user_email": row[2],
                    "docket_ids": row[3] if isinstance(row[3], list) else []
                }
                for row in cur.fetchall()
            ]

    def create_collection(self, user_email: str, name: str) -> int:
        """Create a new collection for the user and return its id."""
        if self.conn is None:
            return -1
        upsert_user_sql = """
            INSERT INTO users (email, name) VALUES (%s, %s)
            ON CONFLICT (email) DO NOTHING
        """
        with self.conn.cursor() as cur:
            cur.execute(upsert_user_sql, (user_email, user_email))
        insert_sql = """
            INSERT INTO collections (user_email, collection_name)
            VALUES (%s, %s)
            RETURNING collection_id
        """
        with self.conn.cursor() as cur:
            cur.execute(insert_sql, (user_email, name))
            collection_id = cur.fetchone()[0]
        self.conn.commit()
        return collection_id

    def delete_collection(self, collection_id: int, user_email: str) -> bool:
        """Delete a collection owned by the user. Returns True if deleted."""
        if self.conn is None:
            return False
        sql = """
            DELETE FROM collections
            WHERE collection_id = %s AND user_email = %s
        """
        with self.conn.cursor() as cur:
            cur.execute(sql, (collection_id, user_email))
            deleted = cur.rowcount > 0
        self.conn.commit()
        return deleted

    def add_docket_to_collection(
            self, collection_id: int, docket_id: str, user_email: str) -> bool:
        """Add a docket to a collection the user owns. Returns True if successful."""
        if self.conn is None:
            return False
        check_sql = """
            SELECT 1 FROM collections
            WHERE collection_id = %s AND user_email = %s
        """
        with self.conn.cursor() as cur:
            cur.execute(check_sql, (collection_id, user_email))
            if cur.fetchone() is None:
                return False
        insert_sql = """
            INSERT INTO collection_dockets (collection_id, docket_id)
            VALUES (%s, %s)
            ON CONFLICT DO NOTHING
        """
        with self.conn.cursor() as cur:
            cur.execute(insert_sql, (collection_id, docket_id))
        self.conn.commit()
        return True

    def remove_docket_from_collection(
            self, collection_id: int, docket_id: str, user_email: str) -> bool:
        """Remove a docket from a collection the user owns. Returns True if successful."""
        if self.conn is None:
            return False
        check_sql = """
            SELECT 1 FROM collections
            WHERE collection_id = %s AND user_email = %s
        """
        with self.conn.cursor() as cur:
            cur.execute(check_sql, (collection_id, user_email))
            if cur.fetchone() is None:
                return False
        delete_sql = """
            DELETE FROM collection_dockets
            WHERE collection_id = %s AND docket_id = %s
        """
        with self.conn.cursor() as cur:
            cur.execute(delete_sql, (collection_id, docket_id))
        self.conn.commit()
        return True


def _get_secrets_from_aws() -> Dict[str, str]:
    if boto3 is None:
        raise ImportError("boto3 is required to use AWS Secrets Manager.")

    client = boto3.client(
        "secretsmanager",
        region_name="YOUR_REGION"
    )
    response = client.get_secret_value(
        SecretId="YOUR_SECRET_NAME"
    )
    return json.loads(response["SecretString"])


def get_postgres_connection() -> DBLayer:
    use_aws_secrets = os.getenv("USE_AWS_SECRETS", "").lower() in {"1", "true", "yes", "on"}

    if use_aws_secrets:
        creds = _get_secrets_from_aws()
        conn = psycopg2.connect(
            host=creds["host"],
            port=creds["port"],
            database=creds["db"],
            user=creds["username"],
            password=creds["password"]
        )
    else:
        if LOAD_DOTENV is not None:
            LOAD_DOTENV()
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST", "localhost"),
            port=os.getenv("DB_PORT", "5432"),
            database=os.getenv("DB_NAME", "your_db"),
            user=os.getenv("DB_USER", "your_user"),
            password=os.getenv("DB_PASSWORD", "your_password")
        )

    return DBLayer(conn)


def get_db() -> DBLayer:
    if LOAD_DOTENV is not None:
        LOAD_DOTENV()
    try:
        return get_postgres_connection()
    except psycopg2.OperationalError:
        return DBLayer()


def get_opensearch_connection() -> OpenSearch:
    if LOAD_DOTENV is not None:
        LOAD_DOTENV()
    return OpenSearch(
        hosts=[{
            "host": os.getenv("OPENSEARCH_HOST", "localhost"),
            "port": int(os.getenv("OPENSEARCH_PORT", "9200")),
        }],
        use_ssl=False,
        verify_certs=False,
    )