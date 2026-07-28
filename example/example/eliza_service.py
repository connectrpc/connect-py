from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, cast

from starlette.applications import Starlette
from starlette.responses import PlainTextResponse
from starlette.routing import Mount, Route

from example import _eliza

from .gen.connectrpc.eliza.v1.eliza_connect import (
    ElizaService,
    ElizaServiceASGIApplication,
)
from .gen.connectrpc.eliza.v1.eliza_pb import (
    ConverseRequest,
    ConverseResponse,
    IntroduceRequest,
    IntroduceResponse,
    SayRequest,
    SayResponse,
)

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from connectrpc.request import RequestContext
    from starlette.types import ASGIApp


class DemoElizaService(ElizaService):
    stream_delay_secs: float
    """Delay between streaming response messages."""

    def __init__(self, stream_delay_secs: float = 0):
        self.stream_delay_secs = stream_delay_secs

    async def say(self, request: SayRequest, _ctx: RequestContext) -> SayResponse:
        reply, _ = _eliza.reply(request.sentence)
        return SayResponse(sentence=reply)

    async def converse(
        self, request: AsyncIterator[ConverseRequest], _ctx: RequestContext
    ) -> AsyncIterator[ConverseResponse]:
        async for req in request:
            reply, end = _eliza.reply(req.sentence)
            yield ConverseResponse(sentence=reply)
            if end:
                return

    async def introduce(
        self, request: IntroduceRequest, _ctx: RequestContext
    ) -> AsyncIterator[IntroduceResponse]:
        name = request.name
        if not name:
            name = "Anonymous User"
        intros = _eliza.get_intro_responses(name)
        for resp in intros:
            if self.stream_delay_secs > 0:
                await asyncio.sleep(self.stream_delay_secs)
            yield IntroduceResponse(sentence=resp)


eliza_app = ElizaServiceASGIApplication(DemoElizaService())

app = Starlette(
    routes=[
        Route("/healthz", lambda _: PlainTextResponse("OK")),
        Mount(eliza_app.path, cast("ASGIApp", eliza_app)),
    ]
)
