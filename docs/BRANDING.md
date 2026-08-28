# Portolan Branding Guide

This document describes how the Portolan CLI documentation applies the brand.

## Where the values live

The brand kit in
[portolan-ops](https://github.com/portolan-sdi/portolan-ops/blob/main/brand/)
is the canonical home for the palette, the fonts, and the logo files.
`brand.json` holds every value. This repository reads those values and does
not restate them.

Three standing rules apply here. Corners are square. Surfaces are flat and
separated by rules, not by cards or shadows. Gradients are banned anywhere.

## Type

Hanken Grotesk sets Latin prose and headlines. JetBrains Mono sets the
machine register: code, labels, data, paths, and controls. Cairo sets all
Arabic.

## Logo assets

All logo files live in `docs/assets/images/`:

| File | Use |
|---|---|
| `icon.svg` | The mark in Portolan blue. Use it on a light ground. |
| `icon-white.svg` | The mark in cream. Use it on a dark ground. |
| `logo.svg` | The horizontal lockup. The mark with the wordmark beside it. |
| `logo.png` | 1000x1000 raster mark for a fallback. |
| `favicon.ico` | The browser tab icon at 16, 32, and 48 px. |
| `social-card.png` | 3500x1440 card for social media previews. |

Each file carries a solid fill. The mark never takes a gradient.

## MkDocs Configuration

Two files implement the branding:

1. `mkdocs.yml` sets the header logo and the favicon.
2. `docs/assets/stylesheets/extra.css` sets the color scheme.

## GitHub Repository Setup

Configure these settings in **Settings** and then **General**.

Upload `docs/assets/images/social-card.png` under **Social preview**.

Set the repository description to the brand messaging:

> A CLI for publishing and managing cloud-native geospatial data catalogs

## Documentation Site

Build the documentation locally:

```bash
uv run mkdocs serve
```

Then open http://127.0.0.1:8000 in a browser.
