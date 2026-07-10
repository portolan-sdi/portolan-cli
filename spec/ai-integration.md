# AI & LLM Integration

This section covers practices for making Portolan catalogs discoverable and usable by AI agents and large language models. This is a rapidly evolving area — the recommendations here represent early patterns that have proven effective and will be refined as the community gains experience.

## AGENTS.md

[AGENTS.md](https://agents.md/) is an emerging cross-tool standard for a Markdown file that gives AI agents the context they need to work with a project. In Portolan, every catalog and collection includes an `AGENTS.md` file that gives AI agents the context they need to discover, understand, and query the data. (Portolan previously used `llms.txt` for this role; `AGENTS.md` replaces it.)

### Requirements

Every catalog and collection **MUST** include an `AGENTS.md` file in Markdown format.

The `AGENTS.md` file **MUST** be referenced in the STAC JSON `links` array:

```json
{
  "rel": "agents",
  "href": "./AGENTS.md",
  "type": "text/markdown",
  "title": "Agent/LLM usage guide"
}
```

- The `href` **MUST** use a relative path (consistent with `SELF_CONTAINED` catalog type)
- The `type` **MUST** be `text/markdown`
- `AGENTS.md` is a **link**, not an asset — it describes the data, it is not the data itself

### Content Recommendations

The requirement is only that `AGENTS.md` exist and be linked — its content is open-ended. The recommendations below are early and expected to evolve; they reflect patterns that have worked well in practice but are not exhaustive. Favor content that is **not already in the README** and that helps an agent use the data.

#### Catalog-Level AGENTS.md

A catalog-level `AGENTS.md` provides an overview of the entire catalog or sub-catalog. It **SHOULD** include:

- A summary of what the catalog contains and who publishes it
- A list of all collections with brief descriptions (what each contains, feature count, key fields)
- Data access patterns (base URLs, S3 paths, code examples)
- Coordinate system conventions used across the catalog
- License information
- Pointers to collection-level `AGENTS.md` files for detailed documentation

#### Collection-Level AGENTS.md

A collection-level `AGENTS.md` provides everything an AI agent needs to work with a specific collection. It **SHOULD** include:

- **What the collection is** — a plain-language description, including source, provider, and license
- **How to access the data** — URLs and working code examples for loading the data (e.g., DuckDB SQL, Python)
- **Schema documentation** — all field names, types, and meanings, ideally as a table
- **Data quality notes** — sentinel values, privacy suppressions, known quirks, or caveats that would trip up a naive query
- **Example queries** — practical, working examples showing common analysis patterns
- **Related collections** — cross-references to complementary collections, including join keys and how to combine them

#### General Guidance

- **SHOULD** be written for an AI agent audience — concise, structured, and practical
- **SHOULD** favor concrete examples over abstract descriptions
- **SHOULD** include working code that an agent can adapt, not just pseudocode
- **SHOULD** call out non-obvious pitfalls (e.g., coordinate systems in meters vs. degrees, coded values, null conventions)
