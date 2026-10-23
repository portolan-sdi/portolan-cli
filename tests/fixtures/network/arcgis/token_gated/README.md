# Server with a token gate (#855)

For a request with no `token` query parameter, the replay server returns HTTP
200 and `{"error": {"code": 499, "message": "Token Required"}}`. ArcGIS
Enterprise sends this shape.

## Source

- The bodies are the `layer_url` responses of the Boston service, under
  `Secured/`.
- The gate is hand-written, because no public server requires a token.
- The token `REPLAY-TOKEN` is a test value.
