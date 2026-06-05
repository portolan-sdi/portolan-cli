# AI & LLM Integration

This section covers practices for making Portolan catalogs discoverable and usable by AI agents and large language models. This is a rapidly evolving area — the recommendations here represent early patterns that have proven effective and will be refined as the community gains experience.

## llms.txt

[llms.txt](https://llmstxt.org/) is an emerging standard for providing LLM-friendly documentation alongside web resources. In Portolan, every catalog and collection includes an `llms.txt` file that gives AI agents the context they need to discover, understand, and query the data.

### Requirements

Every catalog and collection **MUST** include an `llms.txt` file in markdown format.

The `llms.txt` file **MUST** be referenced in the STAC JSON `links` array:

```json
{
  "rel": "llms",
  "href": "./llms.txt",
  "type": "text/markdown",
  "title": "Agent/LLM usage guide"
}
```

- The `href` **MUST** use a relative path (consistent with `SELF_CONTAINED` catalog type)
- The `type` **MUST** be `text/markdown`
- `llms.txt` is a **link**, not an asset — it describes the data, it is not the data itself

### Content Recommendations

These recommendations are early and expected to evolve. They reflect patterns that have worked well in practice but are not exhaustive.

#### Catalog-Level llms.txt

A catalog-level `llms.txt` provides an overview of the entire catalog or sub-catalog. It **SHOULD** include:

- A summary of what the catalog contains and who publishes it
- A list of all collections with brief descriptions (what each contains, feature count, key fields)
- Data access patterns (base URLs, S3 paths, code examples)
- Coordinate system conventions used across the catalog
- License information
- Pointers to collection-level `llms.txt` files for detailed documentation

#### Collection-Level llms.txt

A collection-level `llms.txt` provides everything an AI agent needs to work with a specific dataset. It **SHOULD** include:

- **What the dataset is** — a plain-language description, including source, provider, and license
- **How to access the data** — URLs and working code examples for loading the data (e.g., DuckDB SQL, Python)
- **Schema documentation** — all field names, types, and meanings, ideally as a table
- **Data quality notes** — sentinel values, privacy suppressions, known quirks, or caveats that would trip up a naive query
- **Example queries** — practical, working examples showing common analysis patterns
- **Related datasets** — cross-references to complementary collections, including join keys and how to combine them

#### General Guidance

- **SHOULD** be written for an AI agent audience — concise, structured, and practical
- **SHOULD** favor concrete examples over abstract descriptions
- **SHOULD** include working code that an agent can adapt, not just pseudocode
- **SHOULD** call out non-obvious pitfalls (e.g., coordinate systems in meters vs. degrees, coded values, null conventions)
