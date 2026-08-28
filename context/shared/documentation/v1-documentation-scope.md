# V1 Documentation Scope

## Audience

The primary audience is GIS data publishers who create their first Portolan catalog.
Automation engineers are the secondary audience.

## Source Layout

The root README is the concise landing page.
MkDocs uses that file as its own landing page.

The Philadelphia housing example is the only human-written tutorial.
MkDocs includes its README, and tests execute its extraction, publication, and analysis scripts.

The reference generator reads the shipped Click tree and explicit public package exports.
It writes the CLI wrapper, Python API wrapper, and Reference navigation landing page.

## README Review Gates

1. Review the structure before review of wording.
2. Review headings and topic sentences before review of full prose.
3. Review final prose before a release.

The pull request for this reset provides the first review point.
Generated reference pages do not need prose review.
