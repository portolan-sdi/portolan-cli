# WFS URL with an API key (#981)

The service URL carries `?api_key=SECRET`. Each request without `api_key`
gets HTTP 401. No file under the output root may hold `SECRET`.

- Source: the bodies are the `workspace_layer` recording of
  `https://gs-stable.geo-solutions.it/geoserver/wfs`, trimmed to
  `topp:tasmania_roads`. The key gate is authored, because the public server
  needs no key. `SECRET` is a test value.
