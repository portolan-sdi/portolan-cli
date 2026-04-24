# CSW/ISO 19139 Test Fixtures

Test fixtures for CSW (Catalog Service for Web) and ISO 19139 metadata parsing.

## Fixtures

### belgium_buildings_iso19139.xml
- **Origin**: INSPIRE Wallonia geoportal (Belgium)
- **Purpose**: Full ISO 19139 metadata record for comprehensive parser testing
- **Size**: ~50KB (884 lines)
- **Tests using it**: `test_iso_parser.py`, `test_client.py`
- **Notes**: Contains all major ISO 19139 elements including contact info, keywords from multiple thesauri, CC-BY-4.0 license, lineage, and dates

### minimal_iso19139.xml
- **Origin**: Synthetic
- **Purpose**: Minimal valid ISO 19139 with only required fields
- **Size**: ~1KB
- **Tests using it**: Parser edge case tests
- **Notes**: Contains only file_identifier, title, and abstract

### csw_wrapped_iso19139.xml
- **Origin**: Synthetic
- **Purpose**: ISO 19139 wrapped in CSW GetRecordByIdResponse envelope
- **Size**: ~2KB
- **Tests using it**: CSW response unwrapping tests
- **Notes**: Tests that parser handles CSW wrapper correctly

### title_only_iso19139.xml
- **Origin**: Synthetic
- **Purpose**: ISO 19139 with title but no abstract
- **Size**: ~500B
- **Tests using it**: `has_useful_metadata()` negative tests
- **Notes**: Tests that sparse metadata is correctly identified

### invalid_iso19139.xml
- **Origin**: Synthetic
- **Purpose**: Malformed XML for error handling tests
- **Size**: ~200B
- **Tests using it**: `test_iso_parser.py` error path tests
- **Notes**: Valid XML but missing required ISO 19139 elements

## Schema Notes

ISO 19139 is the XML encoding of ISO 19115 (Geographic Metadata). Key namespaces:
- `gmd`: Geographic Metadata Domain (main elements)
- `gco`: Geographic Common (primitives like CharacterString)
- `gmx`: Geographic Metadata Extension (Anchor elements for URLs)
- `srv`: Service metadata
- `csw`: Catalog Service for Web (wrapper namespace)
