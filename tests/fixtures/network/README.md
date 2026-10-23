# Recorded service responses

Each directory `<kind>/<scenario>/` is one scenario for `tests/replay_server.py`.
It holds `routes.json`, the body files, and a README. The README names the issue,
the source of the recording, and what was trimmed or authored.

`tests/integration/test_extract_recorded.py` runs the real `extract` command
against each scenario. `tests/network/test_recorded_services_live.py` checks
nightly that each public source still answers in the recorded shape.

Rules for a new recording:

- Record the real responses where a public server exists. Remove every
  credential. Replace the upstream origin in a link with `{{base_url}}`.
- Trim feature pages to a few features and decimate their geometries. Set the
  feature count responses to match.
- Keep each scenario under 100 KB.
- Author a response only when no public server shows the shape, and say so in
  the README.
