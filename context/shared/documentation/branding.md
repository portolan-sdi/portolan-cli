# Brand

The brand kit in
[portolan-ops](https://github.com/portolan-sdi/portolan-ops/blob/main/brand/)
is the canonical home for the palette, the fonts, and the logo files.
`brand.json` defines each value. This repository reads them from the kit. A
new brand value belongs in portolan-ops.

Corners stay square. A rule separates one flat surface from the next. The
kit bans a gradient everywhere.

## Files the sync owns

The sync in portolan-ops writes these four files. Do not edit them here. To
change one, edit its source in portolan-ops. The next sync run overwrites a
local edit.

| File | Source in portolan-ops |
|---|---|
| `docs/assets/stylesheets/_brand-vars.css` | `brand/_brand-vars.css` |
| `docs/assets/images/portolan-logomark-4163cc.svg` | `brand/logos/portolan-logomark-4163cc.svg` |
| `docs/assets/images/portolan-logomark-fcfcfa.svg` | `brand/logos/portolan-logomark-fcfcfa.svg` |
| `docs/assets/images/portolan-logo-horizontal-light.svg` | `brand/logos/portolan-logo-horizontal-light.svg` |

## Colors

`_brand-vars.css` declares the tokens on `:root`. `mkdocs.yml` lists it
first under `extra_css`, so `extra.css` reads the tokens. A browser
resolves an undefined custom property to nothing, so the reverse order
drops each color from the page.

The kit declares two groups. Use a `--color-*` role first. Use a
`--palette-*` token only when the intent is a literal brand color. A cream
overlay on the blue header band is a literal brand color. Link text is a
role.

`extra.css` uses no hex value and no `rgba()` call. Every translucent color
uses `color-mix()` over a palette token.

## Type

Hanken Grotesk sets Latin prose and headlines. JetBrains Mono sets the
machine register: code, labels, data, and paths. `mkdocs.yml` names both
under `theme.font`, so Google Fonts serves them.

The kit also assigns a control to the machine register. Material renders
`.md-typeset .md-button` in `--md-text-font`. The site then sets a button in
Hanken Grotesk. This conflict is open.

## Logo assets

| File | Use |
|---|---|
| `portolan-logomark-4163cc.svg` | The mark in Portolan blue. Use it on a light ground. |
| `portolan-logomark-fcfcfa.svg` | The mark in cream. Use it on a dark ground. |
| `portolan-logo-horizontal-light.svg` | The horizontal lockup. The mark with the wordmark beside it. |
| `logo.png` | 1000x1000 raster mark for a fallback. |
| `favicon.ico` | Kept on disk for a manual upload. |
| `social-card.png` | 3500x1440 card for social media previews. |

Each file uses a solid fill. The mark never takes a gradient.

`mkdocs.yml` sets the header logo to the cream mark, because the header is a
solid blue band. It sets the favicon to the blue mark.

## Decisions

**The site uses Google Fonts.** `scripts/sync.py` in portolan-ops reads and
writes text only. A binary file needs a different transport. A self-hosted
font waits for that work. The cost of the Google Fonts route is one
third-party request for each visitor. The site has no fully offline build.

**The site skips Cairo.** `theme.language` is `en` and the site is English
only. Add Cairo when the site adds Arabic.

**The favicon is an SVG.** `brand/icons/` in portolan-ops is empty, and the
sync moves text only. The site points `theme.favicon` at the synced blue
mark. Every browser that Material targets reads an SVG favicon.

## Repository setup

Configure these settings under **Settings** and then **General**.

Upload `docs/assets/images/social-card.png` under **Social preview**.

Set the repository description to the brand messaging:

> A CLI for publishing and managing cloud-native geospatial data catalogs

## Build the site

```bash
uv run zensical serve
```

Then open http://127.0.0.1:8000 in a browser.
