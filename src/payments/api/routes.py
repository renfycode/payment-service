from typing import Annotated
from uuid import UUID

from dependency_injector.wiring import inject
from fastapi import APIRouter, Depends, Header, Response, status

from payments.api.dependencies import PaymentServiceDep, require_api_key
from payments.schemas import PaymentAccepted, PaymentCreate, PaymentRead

router = APIRouter(
    prefix="/api/v1/payments",
    tags=["payments"],
    dependencies=[Depends(require_api_key)],
    responses={status.HTTP_401_UNAUTHORIZED: {"description": "Invalid or missing API key"}},
)


@router.post(
    "",
    status_code=status.HTTP_202_ACCEPTED,
    responses={
        status.HTTP_409_CONFLICT: {
            "description": "Idempotency-Key was already used with a different request body"
        }
    },
)
@inject
async def create_payment(
    data: PaymentCreate,
    service: PaymentServiceDep,
    response: Response,
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key", min_length=1, max_length=255)],
) -> PaymentAccepted:
    payment, created = await service.create(data, idempotency_key)
    if not created:
        response.headers["Idempotent-Replayed"] = "true"
    return PaymentAccepted(
        payment_id=payment.id, status=payment.status, created_at=payment.created_at
    )


@router.get(
    "/{payment_id}",
    responses={status.HTTP_404_NOT_FOUND: {"description": "Payment not found"}},
)
@inject
async def get_payment(payment_id: UUID, service: PaymentServiceDep) -> PaymentRead:
    payment = await service.get(payment_id)
    return PaymentRead.model_validate(payment)
