from logging import getLogger

import openmetaxis_sdk
from openmetaxis_sdk import Configuration
from openmetaxis_sdk.models import (
    CreateLocation,
    CreateTable,
    LocationDTO,
    StorageServiceDTO,
    Table,
)

LOGGER = getLogger(__name__)


class MetaxisClient:

    def __init__(self, host: str):
        configuration = Configuration()
        configuration.host = host
        api_client = openmetaxis_sdk.ApiClient(configuration)
        self._storage_api = openmetaxis_sdk.StorageApi(api_client)

    def get_storage_service_by_id(self, storage_service_id: str) -> StorageServiceDTO:
        try:
            response = self._storage_api.handle_get_storage_by_id(storage_service_id)
            return response.data
        except Exception as e:
            msg = f"request metaxis failure, error:{e}"
            LOGGER.error(msg)

    def get_storage_service_by_fqn(self, storage_service_fqn: str) -> StorageServiceDTO:
        try:
            response = self._storage_api.handle_get_storage_by_name(storage_service_fqn)
            return response.data
        except Exception as e:
            msg = f"request metaxis failure, error:{e}"
            LOGGER.error(msg)
            
            
            
