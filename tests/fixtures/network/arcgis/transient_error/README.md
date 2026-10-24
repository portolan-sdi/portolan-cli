# Discovery error that goes away (#811), and one that stays (#1035)

The first request for the service document gets HTTP 503. The second request
gets the service document. Discovery must try again, and the command must get
the layer.

Under `Closed_Space`, the feature query of layer 0 gets HTTP 404. The run
`layer_gone` passes `--retries 3`. A retry cannot fix a 404, so the run sends
one feature query and exits 1.

## Source

- The bodies are the `layer_url` responses of the Boston service.
- The 503 and 404 responses are hand-written.
