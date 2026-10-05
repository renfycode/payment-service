import secrets
from typing import Annotated

from dependency_injector.wiring import Provide, inject
from fastapi import Depends, HTTPException, Security, status
from fastapi.security import APIKeyHeader

from payments.config import Settings
from payments.containers import ApiContainer
from payments.services import PaymentService

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)

SettingsDep = Annotated[Settings, Depends(Provide[ApiContainer.settings])]
PaymentServiceDep = Annotated[PaymentService, Depends(Provide[ApiContainer.payment_service])]


@inject
async def require_api_key(
    settings: SettingsDep,
    api_key: Annotated[str | None, Security(api_key_header)],
) -> None:
    expected = settings.api.key.get_secret_value()
    if api_key is None or not secrets.compare_digest(api_key.encode(), expected.encode()):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing API key",
            headers={"WWW-Authenticate": "ApiKey"},
        )
