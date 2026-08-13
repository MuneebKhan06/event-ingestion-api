"""Request correlation IDs.

Metrics tell you *how many* requests failed; a correlation ID lets you follow
one. Without it, "Failed to publish event <id>" in a busy log has no link back
to the caller who saw the 503.

Scope: the API process only. The consumer runs separately and has no request
context, so its log lines carry no ID. Carrying one across would mean putting it
in the Kafka message, which changes the event schema — a deliberate design
decision rather than a detail to slip in here.
"""

import contextvars
import logging
import re
import uuid

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request

REQUEST_ID_HEADER = "X-Request-ID"

# Clients may supply their own ID so a trace spans several services. That makes
# it attacker-controlled text heading straight for the logs, so it is filtered
# rather than trusted: newlines would let a caller forge log lines, and an
# unbounded value would let them bloat every record. Anything outside this set
# is replaced with a generated ID instead of being sanitised into something the
# caller did not send and would not recognise.
_SAFE_REQUEST_ID = re.compile(r"^[A-Za-z0-9._:-]{1,64}$")

request_id_var: contextvars.ContextVar[str] = contextvars.ContextVar(
    "request_id", default="-"
)


class RequestIDFilter(logging.Filter):
    """Puts the current request's ID on every record, from anywhere.

    A logging filter reads the contextvar at emit time, so call sites don't
    have to accept and forward an ID they otherwise have no use for.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        record.request_id = request_id_var.get()
        return True


class RequestIDMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        supplied = request.headers.get(REQUEST_ID_HEADER)
        usable = bool(supplied and _SAFE_REQUEST_ID.match(supplied))
        request_id = supplied if usable else str(uuid.uuid4())

        token = request_id_var.set(request_id)
        try:
            response = await call_next(request)
        finally:
            # Reset rather than leave it set: without this the value would leak
            # into whatever the worker task handles next.
            request_id_var.reset(token)

        # Echoed back so the caller can quote it when reporting a problem.
        response.headers[REQUEST_ID_HEADER] = request_id
        return response
