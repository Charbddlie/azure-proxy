"""Terminal dashboard for azure-proxy.

Reads /healthz, /routes and /events over HTTP and draws three boards —
sources, models, events. It never starts, stops or writes to the proxy: it is
a separate process that can be opened and closed at will while the service
carries on. See tui/client.py.
"""
