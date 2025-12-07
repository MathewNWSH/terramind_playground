import logging
from dataclasses import dataclass
from typing import Any

from fastapi import HTTPException
from httpx import AsyncClient, HTTPError
from stamina import retry
from subs2pgstac.config import OS_URL

_HTTP = AsyncClient(timeout=30, follow_redirects=True, verify=False)


class NotFoundError(Exception):
    """404 error exception."""

    def __init__(self, fearureID: str):
        super().__init__(
            f"Cannot perform PATCH/DELETE operation on not exising item: {fearureID}"
        )
        self.feature_id = fearureID


class NotFoundErrorOnPOST(Exception):
    """404 error exception."""

    def __init__(self, fearureID: str):
        super().__init__(
            f"Cannot perform POST operation. Endpoint - Collection probably does not exist: {fearureID}"
        )
        self.feature_id = fearureID


class AlreadyExistsError(Exception):
    """409 error exception."""

    def __init__(self, fearureID: str):
        super().__init__(f"Cannot perform PUT, an item already exists: {fearureID}")
        self.feature_id = fearureID


@dataclass
class TapiPayload:
    bearer: str
    collection_id: str
    feature: dict[str, Any] | str
    os_url: str = OS_URL

    @property
    def feature_id(self) -> str:
        if isinstance(self.feature, dict):
            return self.feature["id"]
        else:
            return self.feature

    @property
    def tapi_path(self) -> str:
        return f"{self.os_url}/collections/{self.collection_id}/items/"

    @retry(on=HTTPError)
    async def async_post(self) -> None:
        """Add new item to STAC."""
        logging.debug("About to perform POST request")
        logging.debug("HTTP Path: %r", self.tapi_path)
        r = await _HTTP.post(
            self.tapi_path,
            json=self.feature,
            headers={"Authorization": f"Bearer {self.bearer}"},
        )
        if not r.is_success:
            if r.status_code == 400:
                logging.error(f"An Error occurred: {r.json()}")
            elif r.status_code == 409:
                raise AlreadyExistsError(self.feature_id)
            elif r.status_code == 404:
                logging.warning(
                    "%r not added. Collection probably does not exist.", self.feature_id
                )
                raise NotFoundErrorOnPOST(self.feature_id)
            else:
                raise HTTPException(r.status_code, r.json())
        logging.debug("%r added", self.feature_id)

    @retry(on=HTTPError)
    async def async_put(self) -> None:
        """Modify an item in STAC."""
        logging.debug("About to perform put request")
        logging.debug("HTTP Path: %r", self.tapi_path)
        r = await _HTTP.put(
            self.tapi_path + self.feature_id,
            json=self.feature,
            headers={"Authorization": f"Bearer {self.bearer}"},
        )
        if not r.is_success:
            if r.status_code == 400:
                logging.error(f"An Error occurred: {r.json()}")
            elif r.status_code == 404:
                raise NotFoundError(self.feature_id)
            else:
                raise HTTPException(r.status_code, r.json())

        logging.debug("%r modified", self.feature_id)

    @retry(on=HTTPError)
    async def async_delete(self) -> None:
        """Delete an item from STAC."""
        logging.debug("About to perform DELETE request")
        logging.debug("HTTP Path: %r", self.tapi_path)
        r = await _HTTP.delete(
            self.tapi_path + self.feature_id,
            headers={"Authorization": f"Bearer {self.bearer}"},
        )
        if not r.is_success:
            if r.status_code == 400:
                logging.error(f"An Error occurred: {r.json()}")
            elif r.status_code == 404:
                logging.warning(
                    f"Cannot perform put/DELETE operation on not exising object: {self.feature_id}"
                )
            else:
                raise HTTPException(r.status_code, r.json())

        logging.debug("%r deleted", self.feature_id)
