# Discovery error that goes away (#811)

The first request for the service document gets HTTP 503. The second request
gets the service document. Discovery must try again, and the command must get
the layer.

## Source

- The bodies are the `layer_url` responses of the Boston service.
- The 503 response is hand-written.
