# Transient ArcGIS discovery error (#811)

The first request for the service document answers HTTP 503. The second
request gets the service document. Discovery must retry and the layer must
extract.

- Source: the bodies are the `layer_url` recording of the Boston service. The
  503 is authored.
