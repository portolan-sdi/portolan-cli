# Server with a token gate (#855)

For a request with no `token` query parameter, the replay server returns HTTP
200 and `{"error": {"code": 499, "message": "Token Required"}}`. ArcGIS
Enterprise sends this shape.

Under `Public/`, the service and layer documents need no token, and the
feature query gets the same gate. The run `token_gated-data` gets the gate in
the layer fetch. The run `token_gated-no_token` gets it in discovery. Each run
passes `--retries 3`. A retry cannot fix a token error, so each run sends one
request to the gated route and exits 1 (#1035).

## Source

- The bodies are the `layer_url` responses of the Boston service, under
  `Secured/`.
- The gate is hand-written, because no public server requires a token.
- The token `REPLAY-TOKEN` is a test value.
