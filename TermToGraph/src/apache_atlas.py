import requests

from typing import Optional, Dict, Tuple, Any, List
from dotenv import load_dotenv, find_dotenv
from requests.auth import HTTPBasicAuth
from os import getenv

from .logger import setup_logger
logger = setup_logger('atlas')

load_dotenv(find_dotenv(), override=True)

# Support a comma-separated list of Atlas URLs for fallback (e.g. two cluster nodes).
_raw_urls = getenv('ATLAS_URL', '')
ATLAS_URLS = [u.strip() for u in _raw_urls.split(',') if u.strip()]
USERNAME   = getenv('ATLAS_USERNAME')
PASSWORD   = getenv('ATLAS_PASSWORD')

class Atlas:

    def __init__(self) -> None:
        if not ATLAS_URLS:
            raise RuntimeError("ATLAS_URL is not set in the environment.")
        if not USERNAME or not PASSWORD:
            raise RuntimeError("ATLAS_USERNAME or ATLAS_PASSWORD is not set in the environment.")
        self.session = requests.Session()
        self.urls = ATLAS_URLS
        self.auth = HTTPBasicAuth(USERNAME, PASSWORD)

    def __del__(self) -> None:
        self.session.close()

    def _request(self, method: str, path: str, **kwargs) -> requests.Response:
        """Try each configured Atlas URL in order.

        On success the winning URL is promoted to the front of the list so it
        is preferred on the next call.  On failure the offending URL is demoted
        to the back so healthier nodes are tried first next time.
        If every URL fails the last exception is re-raised.
        """
        last_exc: Exception = RuntimeError("No Atlas URLs configured.")
        for i, url in enumerate(self.urls):
            full_url = f"{url}{path}"
            try:
                resp = self.session.request(
                    method,
                    full_url,
                    auth=self.auth,
                    timeout=60,
                    **kwargs,
                )
                resp.raise_for_status()
                # Promote the winner to the front
                if i != 0:
                    self.urls.insert(0, self.urls.pop(i))
                    logger.info("Promoted Atlas URL to front: %s", url)
                return resp
            except Exception as e:
                # Demote the failing URL to the back
                self.urls.append(self.urls.pop(i))
                logger.warning(
                    "Atlas URL %s failed (%s). Demoted to back. %s",
                    url, e,
                    "Trying next URL..." if self.urls else "No more URLs to try.",
                )
                last_exc = e
        raise last_exc

    @staticmethod
    def _extract_term_name(payload: Any) -> Optional[str]:
        if not isinstance(payload, dict):
            return None

        name = payload.get("name")
        if isinstance(name, str) and name.strip():
            return name.strip()

        display_text = payload.get("displayText")
        if isinstance(display_text, str) and display_text.strip():
            return display_text.strip()

        return None

    @staticmethod
    def _dedupe_keep_order(values: List[str]) -> List[str]:
        return list(dict.fromkeys(v for v in values if isinstance(v, str) and v.strip()))

    def _fetch_terms_via_search_basic(self, page_size: int = 200) -> List[str]:
        terms: List[str] = []
        offset = 0

        while True:
            resp = self._request(
                "GET",
                "/search/basic",
                params={
                    "typeName": "AtlasGlossaryTerm",
                    "excludeDeletedEntities": "true",
                    "limit": page_size,
                    "offset": offset,
                },
                headers={"Accept": "application/json"},
            )
            data = resp.json() or {}
            entities = data.get("entities", []) or []
            if not entities:
                break

            for entity in entities:
                if not isinstance(entity, dict):
                    continue
                attributes = entity.get("attributes") or {}
                term_name = self._extract_term_name(attributes) or self._extract_term_name(entity)
                if term_name:
                    terms.append(term_name)

            if len(entities) < page_size:
                break
            offset += page_size

        return self._dedupe_keep_order(terms)

    def get_all_term_names(self, page_size: int = 200) -> List[str]:
        """Return all glossary term names available in Atlas via REST API.

        Preferred flow:
        1) GET /glossary (paginated)
        2) GET /glossary/{guid}/terms/headers (paginated)

        Fallback flow:
        - GET /search/basic?typeName=AtlasGlossaryTerm
        """
        terms: List[str] = []
        glossary_guids: List[str] = []

        logger.info("Fetching Atlas glossaries and terms via API...")

        try:
            offset = 0
            while True:
                resp = self._request(
                    "GET",
                    "/glossary",
                    params={"limit": page_size, "offset": offset, "sort": "ASC"},
                    headers={"Accept": "application/json"},
                )
                glossaries = resp.json() or []

                if not isinstance(glossaries, list) or not glossaries:
                    break

                for glossary in glossaries:
                    if not isinstance(glossary, dict):
                        continue
                    guid = glossary.get("guid")
                    if isinstance(guid, str) and guid.strip():
                        glossary_guids.append(guid.strip())

                    # Some Atlas versions embed terms under each glossary payload.
                    for embedded_term in (glossary.get("terms") or []):
                        term_name = self._extract_term_name(embedded_term)
                        if term_name:
                            terms.append(term_name)

                if len(glossaries) < page_size:
                    break
                offset += page_size

            for glossary_guid in self._dedupe_keep_order(glossary_guids):
                offset = 0
                while True:
                    resp = self._request(
                        "GET",
                        f"/glossary/{glossary_guid}/terms/headers",
                        params={"limit": page_size, "offset": offset, "sort": "ASC"},
                        headers={"Accept": "application/json"},
                    )
                    headers_payload = resp.json() or []

                    if not isinstance(headers_payload, list) or not headers_payload:
                        break

                    for header in headers_payload:
                        term_name = self._extract_term_name(header)
                        if term_name:
                            terms.append(term_name)

                    if len(headers_payload) < page_size:
                        break
                    offset += page_size

            deduped = self._dedupe_keep_order(terms)
            if deduped:
                logger.info("Fetched %s Atlas terms via glossary API", len(deduped))
                return deduped

            logger.warning("Glossary API returned no Atlas terms. Falling back to search/basic endpoint.")
            fallback_terms = self._fetch_terms_via_search_basic(page_size=page_size)
            logger.info("Fetched %s Atlas terms via search/basic fallback", len(fallback_terms))
            return fallback_terms

        except Exception as e:
            logger.warning("Failed fetching terms via glossary API (%s). Trying search/basic fallback.", e)
            try:
                fallback_terms = self._fetch_terms_via_search_basic(page_size=page_size)
                logger.info("Fetched %s Atlas terms via search/basic fallback", len(fallback_terms))
                return fallback_terms
            except Exception as fallback_error:
                raise RuntimeError(f"Error fetching Atlas terms list: {fallback_error}")

    def _search_term(self, term_name: str) -> Optional[dict]:
        
        term_name = (term_name or "").strip()
        if not term_name:
            return None

        safe_name = term_name.replace("'", "''")
        dsl = f"from AtlasGlossaryTerm where name = '{safe_name}'"

        try:
            resp = self._request(
                "GET",
                "/search/dsl",
                params={"query": dsl, "limit": 1},
                headers={'Accept': 'application/json'},
            )
        except Exception as e:
            raise RuntimeError(f"Error consultando Atlas DSL: {e}")

        data = resp.json() or {}
        ents = data.get("entities", []) or []
        if not ents:
            return None

        ent = ents[0] or {}
        guid = ent.get("guid")
        attrs = ent.get("attributes") or {}
        name = attrs.get("name") or ent.get("displayText") or term_name
        # Traer el glossary/anchor si estuviera
        glossary = attrs.get("anchorDisplayName") or attrs.get("anchor") or None
        return {"guid": guid, "attributes": {"name": name, "anchorDisplayName": glossary}, "displayText": name}

    def _get_entity_by_guid(self, guid: str) -> Optional[dict]:
        try:
            ent_resp = self._request(
                "GET",
                f"/entity/guid/{guid}",
                params={"minExtInfo": "true", "ignoreRelationships": "false"},
            )
            entity = ent_resp.json().get("entity", {}) or {}
        
        except Exception as e:
             raise RuntimeError(f"Error consultando Atlas entity: {e}")

        try:
            cls_resp = self._request(
                "GET",
                f"/entity/guid/{guid}/classifications",
            )
            cls_payload = cls_resp.json() or {}
            if isinstance(cls_payload, dict) and "list" in cls_payload and isinstance(cls_payload["list"], list):
                cls_list = cls_payload["list"]
            elif isinstance(cls_payload, list):
                cls_list = cls_payload
            else:
                cls_list = []
            cls_list = [c for c in (cls_list or []) if isinstance(c, dict)]

        except Exception:
            cls_list = entity.get("classifications", []) or []

        entity["classifications"] = cls_list or (entity.get("classifications", []) or [])
        
        return entity

    def get_entity(self, term_name: str) -> Tuple[Optional[str], Optional[Dict], Optional[str]]:
        '''
        Busca la entidad en Atlas y la devuelve junto con su display name.

        Args:
            term_name (str): Nombre del Atlas Term

        Returns:
            tuple: (display_name, entity): El display name y la entidad.
        '''

        term = self._search_term(term_name)
        if not term:
            return None, None, None

        guid = term.get("guid")

        if not guid:
            return None, None, None
          
        display_name = term.get("attributes", {}).get("name") or term.get("displayText") or term_name
        entity = self._get_entity_by_guid(guid)

        return display_name, entity, guid