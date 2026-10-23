# Recorded service responses

Each directory `<kind>/<scenario>/` is one scenario for `tests/replay_server.py`.
The directory contains `routes.json`, the body files, and a README. The README
states the issue, the source of the responses, and the changes to them.

`tests/integration/test_extract_recorded.py` runs the real `extract` command
against each scenario. `tests/network/test_recorded_services_live.py` checks
each night that each public source still sends the recorded shape.

## Source

Follow these rules for a new scenario:

- Record the real responses when a public server shows the shape.
- Remove each credential.
- Put `{{base_url}}` in place of the upstream origin in a link.
- Keep a few features in each page. Cut the vertices of each geometry.
- Make each count response match the page.
- Keep each scenario below 100 KB.
- Write a response by hand only when no public server shows the shape. Say so
  in the README.
