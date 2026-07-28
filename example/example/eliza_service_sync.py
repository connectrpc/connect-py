from __future__ import annotations

import time
from typing import TYPE_CHECKING

from flask import Flask
from werkzeug.middleware.dispatcher import DispatcherMiddleware

from . import _eliza
from .gen.connectrpc.eliza.v1.eliza_connect import (
    ElizaServiceSync,
    ElizaServiceWSGIApplication,
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
    from collections.abc import Iterator

    from connectrpc.request import RequestContext


class DemoElizaServiceSync(ElizaServiceSync):
    stream_delay_secs: float
    """Delay between streaming response messages."""

    def __init__(self, stream_delay_secs: float = 0):
        self.stream_delay_secs = stream_delay_secs

    def say(self, request: SayRequest, _ctx: RequestContext) -> SayResponse:
        reply, _ = _eliza.reply(request.sentence)
        return SayResponse(sentence=reply)

    def converse(
        self, request: Iterator[ConverseRequest], _ctx: RequestContext
    ) -> Iterator[ConverseResponse]:
        for req in request:
            reply, end = _eliza.reply(req.sentence)
            yield ConverseResponse(sentence=reply)
            if end:
                return

    def introduce(
        self, request: IntroduceRequest, _ctx: RequestContext
    ) -> Iterator[IntroduceResponse]:
        name = request.name
        if not name:
            name = "Anonymous User"
        intros = _eliza.get_intro_responses(name)
        for resp in intros:
            if self.stream_delay_secs > 0:
                time.sleep(self.stream_delay_secs)
            yield IntroduceResponse(sentence=resp)


app = Flask(__name__)


@app.route("/healthz")
def health_check():
    return "OK"


eliza_app = ElizaServiceWSGIApplication(DemoElizaServiceSync())

app.wsgi_app = DispatcherMiddleware(app.wsgi_app, {eliza_app.path: eliza_app})  # ty: ignore[invalid-assignment]
