# Token-gated ArcGIS server (#855)

Every request needs a `token` query parameter. A request without it gets HTTP
200 with `{"error": {"code": 499, "message": "Token Required"}}`. ArcGIS
Enterprise sends this shape.

- Source: the bodies are the `layer_url` recording of the Boston service,
  served under `Secured/`. The gate is authored, because no public server
  requires a token.
- The token `REPLAY-TOKEN` is a test value.
