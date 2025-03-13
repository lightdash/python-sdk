"""
Lightdash API Client

This module provides a client for interacting with the Lightdash API.
"""
from typing import Any, Dict, List, Optional
import httpx
import logging
from urllib.parse import urljoin

from .models import Model, Models


# Configure module logger
logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


class LightdashError(Exception):
    """Base exception for Lightdash API errors."""
    def __init__(self, message: str, name: str, status_code: int):
        self.message = message
        self.name = name
        self.status_code = status_code
        super().__init__(f"{name} ({status_code}): {message}")


class Client:
    """
    A client for interacting with the Lightdash API.

    Args:
        instance_url (str): The URL of your Lightdash instance
        access_token (str): The access token for authentication
        project_uuid (str): The UUID of the project to interact with
    """
    def __init__(self, instance_url: str, access_token: str, project_uuid: str):
        self.instance_url = instance_url.rstrip('/')
        self.access_token = access_token
        self.project_uuid = project_uuid
        self.models = Models(self)

    def _log_request(
        self,
        method: str,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        json: Optional[Dict[str, Any]] = None,
    ) -> None:
        logger.debug(f"Request: {method} {url}")
        if params:
            logger.debug(f"Query params: {params}")
        if json:
            logger.debug(f"JSON body: {json}")

    def _log_response(self, response: httpx.Response) -> None:
        """Log HTTP response details."""
        logger.debug(
            f"Response: {response.status_code} {response.reason_phrase}"
        )
        logger.debug(f"Response body: {response.text}")

    def _make_request(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        json: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Make an HTTP request to the Lightdash API.

        Args:
            method: HTTP method (GET, POST, etc.)
            path: API endpoint path
            params: Query parameters
            json: JSON body for POST/PUT requests

        Returns:
            The response data from the API

        Raises:
            LightdashError: If the API returns an error response
            httpx.HTTPError: If there's a network or HTTP protocol error
        """
        url = urljoin(self.instance_url, path)
        self._log_request(method, url, params, json)
        
        with httpx.Client(
            headers={
                "Authorization": f"ApiKey {self.access_token}",
                "Accept": "application/json",
            },
            timeout=30.0
        ) as client:
            response = client.request(
                method=method,
                url=url,
                params=params,
                json=json,
            )
            
            self._log_response(response)
            
            # Raise HTTP errors
            try:
                response.raise_for_status()
            except httpx.HTTPError as e:
                logger.error(f"HTTP Error: {str(e)}")
                logger.error(f"Response body: {response.text}")
                raise
            
            data = response.json()
            
            # Check for API error response
            if data.get("status") == "error":
                error = data.get("error", {})
                error_msg = error.get("message", "Unknown error")
                error_name = error.get("name", "ApiError")
                error_code = error.get("statusCode", response.status_code)
                
                logger.error(
                    f"API Error: {error_name} ({error_code}): {error_msg}"
                )
                logger.error(f"Full error response: {data}")
                
                raise LightdashError(
                    message=error_msg,
                    name=error_name,
                    status_code=error_code,
                )
                
            # Verify and return results for successful response
            if data.get("status") != "ok":
                logger.error(f"Invalid API response format: {data}")
                raise LightdashError(
                    message="Invalid API response format",
                    name="InvalidResponse",
                    status_code=response.status_code,
                )
                
            return data["results"]

    def _fetch_models(self) -> List[Model]:
        """Internal method to fetch models from API."""
        path = f"/api/v1/projects/{self.project_uuid}/explores"
        response_data = self._make_request("GET", path)
        return [Model.from_api_response(item) for item in response_data]

    def list_models(self) -> List[Model]:
        """
        List all available models (explores) in the project.

        Returns:
            A list of Model objects representing the available explores.

        Raises:
            LightdashError: If the API returns an error response
            httpx.HTTPError: If there's a network or HTTP protocol error
        """
        return self.models.list()

    def get_model(self, name: str) -> Model:
        """Get a model by name.

        Args:
            name: The name of the model to get

        Returns:
            A Model object representing the requested explore
        """
        return self.models.get(name)
