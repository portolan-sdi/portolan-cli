# Portolan Branding Guide

This document outlines the branding implementation for Portolan CLI documentation and repository.

## Brand Identity

**Font**: Archivo Medium
**Designer**: Omnibus-Type
**License**: Open Font License
**Icon Designer**: Icons By Alfredo

## Color Palette

| Element | Hex | Usage |
|---------|-----|-------|
| Background | `#eaedf9` | Page backgrounds, light sections |
| Dark Text | `#202a4f` | Primary text, headings |
| Slogan | `#343e63` | Secondary text, captions |
| Primary | `#4163cc` | Links, buttons, accents |
| Gradient Start | `#395eca` | Gradient backgrounds (start) |
| Gradient End | `#848bd8` | Gradient backgrounds (end) |

## Logo Assets

All logo files are located in `docs/assets/images/`:

- **logo.svg** - Vector logo for header (scalable, preferred)
- **logo.png** - 1000x1000px PNG for favicon and fallbacks
- **social-card.png** - 3500x1440px cover image for social media previews

## MkDocs Configuration

The branding is implemented through:

1. **mkdocs.yml** - Logo, favicon, and theme configuration
2. **docs/assets/stylesheets/extra.css** - Custom color scheme and styling

### Key Features

- Custom color palette matching Portolan brand
- SVG logo in header
- PNG favicon
- Gradient styling on H1 headings
- Custom link and button styles
- Dark mode support with adjusted colors

## GitHub Repository Setup

### Social Preview Image

To configure the GitHub repository's social preview:

1. Navigate to **Settings** → **General**
2. Scroll to **Social preview** section
3. Click **Edit** and upload `docs/assets/images/social-card.png`

This ensures the Portolan branding appears when sharing the repository on:
- Social media (Twitter, LinkedIn, etc.)
- Chat apps (Slack, Discord, etc.)
- Link previews anywhere

### Repository Description

Ensure the repository description matches the brand messaging:

> A CLI for publishing and managing cloud-native geospatial data catalogs

## README Branding

The main README.md includes:

- Centered logo at the top
- Brand tagline
- Consistent color references in documentation links

## Documentation Site

View the branded documentation locally:

```bash
uv run mkdocs serve
```

Then open http://127.0.0.1:8000 in your browser.

## Deployment

When deploying to GitHub Pages, the branding will automatically be applied. The custom CSS and logo assets are included in the build.

## Customization

To modify the brand colors, edit:

```css
/* docs/assets/stylesheets/extra.css */
:root {
  --portolan-bg: #eaedf9;
  --portolan-dark-text: #202a4f;
  --portolan-primary: #4163cc;
  /* ... other colors */
}
```

## Attribution

All branding assets are sourced from `../Portolan-logo/` (one level up from project root).

**Font License**: Open Font License
**Icon Designer**: Icons By Alfredo via [branding tool/service]
