# netty-transport-raknet

Auto-flush is disabled by default. Use `flush()` or `writeAndFlush()` to flush outgoing data explicitly.
Set `RakChannelOption.RAK_AUTO_FLUSH` to `true` to also send unflushed writes automatically: with
`RAK_FLUSH_INTERVAL` at its default of 0 the flush is queued on the event loop and the writes leave when
it runs, after the task that wrote them and any task queued ahead of the flush, coalesced with everything
those tasks wrote; a positive interval keeps them for a coalescing window of that many milliseconds
instead. Explicit flushes are honored in either mode; transmission remains subject to the congestion
window.
