import requests

from typing import Optional, Dict, Tuple
from dotenv import load_dotenv, find_dotenv
from requests.auth import HTTPBasicAuth
from os import getenv
load_dotenv(find_dotenv(), override=True)

from .logger import setup_logger
logger = setup_logger('atlas')

# Support a comma-separated list of Atlas URLs for fallback (e.g. two cluster nodes).
_raw_urls = getenv('ATLAS_URL', '')
ATLAS_URLS = [u.strip() for u in _raw_urls.split(',') if u.strip()]
USERNAME   = getenv('ATLAS_USERNAME')
PASSWORD   = getenv('ATLAS_PASSWORD')

class Atlas:

    def __init__(self) -> None:
        if not ATLAS_URLS:
            raise RuntimeError("ATLAS_URL is not set in the environment.")
        self.session = requests.Session()
        self.urls = ATLAS_URLS

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
                    auth=HTTPBasicAuth(USERNAME, PASSWORD),
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