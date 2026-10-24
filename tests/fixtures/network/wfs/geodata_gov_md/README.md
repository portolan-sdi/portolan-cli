# Service address with a key (#981)

The service address contains `?api_key=SECRET`. For a request with no
`api_key`, the replay server returns HTTP 401. No file in the output root may
contain `SECRET`.

The run `geodata_gov_md-no_match` passes `--layers nomatch`, so discovery
selects no layer and the run fails. The error in text mode and in the JSON
envelope must name the host and must not contain `SECRET`.

## Source

- The bodies are the `workspace_layer` responses of
  `https://gs-stable.geo-solutions.it/geoserver/wfs`, cut to
  `topp:tasmania_roads`.
- The key gate is hand-written, because the public server requires no key.
- `SECRET` is a test value.
