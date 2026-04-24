"""ISO 19139 XML parser.

This module parses ISO 19139 metadata XML (typically from CSW GetRecordById)
and extracts key fields into an ISOMetadata dataclass.

Handles both CSW-wrapped responses (GetRecordByIdResponse) and direct
MD_Metadata root elements.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import defusedxml.ElementTree as ET

if TYPE_CHECKING:
    from xml.etree.ElementTree import Element  # nosec B405 - type hints only

from portolan_cli.extract.csw.models import ISOMetadata

# ISO 19139 XML namespaces
NAMESPACES = {
    "csw": "http://www.opengis.net/cat/csw/2.0.2",
    "gmd": "http://www.isotc211.org/2005/gmd",
    "gco": "http://www.isotc211.org/2005/gco",
    "gmx": "http://www.isotc211.org/2005/gmx",
    "srv": "http://www.isotc211.org/2005/srv",
    "gml": "http://www.opengis.net/gml/3.2",
    "xlink": "http://www.w3.org/1999/xlink",
}


class ISOParseError(Exception):
    """Raised when ISO 19139 XML parsing fails."""

    pass


def parse_iso19139(xml_content: str) -> ISOMetadata:
    """Parse ISO 19139 XML and extract metadata fields.

    Args:
        xml_content: ISO 19139 XML string (CSW response or direct MD_Metadata).

    Returns:
        ISOMetadata with extracted fields.

    Raises:
        ISOParseError: If XML parsing fails or required fields are missing.
    """
    try:
        root = ET.fromstring(xml_content)
    except ET.ParseError as e:
        raise ISOParseError(f"Failed to parse XML: {e}") from e

    # Find MD_Metadata element (may be wrapped in CSW response)
    md_metadata = _find_md_metadata(root)
    if md_metadata is None:
        raise ISOParseError("No MD_Metadata element found in XML")

    # Extract required fields
    file_identifier = _get_text(md_metadata, ".//gmd:fileIdentifier/gco:CharacterString")
    if not file_identifier:
        raise ISOParseError("Missing required field: file_identifier")

    title = _get_title(md_metadata)
    if not title:
        raise ISOParseError("Missing required field: title")

    # Extract optional fields
    return ISOMetadata(
        file_identifier=file_identifier,
        title=title,
        abstract=_get_abstract(md_metadata),
        keywords=_get_keywords(md_metadata),
        contact_organization=_get_contact_organization(md_metadata),
        contact_email=_get_contact_email(md_metadata),
        license_url=_get_license_url(md_metadata),
        license_text=_get_license_text(md_metadata),
        access_constraints=_get_access_constraints(md_metadata),
        lineage=_get_lineage(md_metadata),
        thumbnail_url=_get_thumbnail_url(md_metadata),
        scale_denominator=_get_scale_denominator(md_metadata),
        topic_category=_get_topic_category(md_metadata),
        maintenance_frequency=_get_maintenance_frequency(md_metadata),
        date_created=_get_date_by_type(md_metadata, "creation"),
        date_revised=_get_date_by_type(md_metadata, "revision"),
        date_published=_get_date_by_type(md_metadata, "publication"),
    )


def _find_md_metadata(root: Element) -> Element | None:
    """Find MD_Metadata element in XML tree.

    Handles both CSW-wrapped responses and direct MD_Metadata roots.
    """
    # Check if root is MD_Metadata
    if root.tag.endswith("MD_Metadata"):
        return root

    # Look for MD_Metadata in CSW response
    md = root.find(".//gmd:MD_Metadata", NAMESPACES)
    if md is not None:
        return md

    # Try without namespace (some servers don't use prefixes)
    for elem in root.iter():
        if elem.tag.endswith("MD_Metadata"):
            return elem

    return None


def _get_text(element: Element, xpath: str) -> str | None:
    """Get text content from XPath, or None if not found."""
    found = element.find(xpath, NAMESPACES)
    if found is not None and found.text:
        return found.text.strip()
    return None


def _get_title(md: Element) -> str | None:
    """Extract dataset title."""
    return _get_text(
        md,
        ".//gmd:identificationInfo/gmd:MD_DataIdentification/gmd:citation"
        "/gmd:CI_Citation/gmd:title/gco:CharacterString",
    )


def _get_abstract(md: Element) -> str | None:
    """Extract dataset abstract."""
    return _get_text(
        md,
        ".//gmd:identificationInfo/gmd:MD_DataIdentification/gmd:abstract/gco:CharacterString",
    )


def _get_keywords(md: Element) -> list[str] | None:
    """Extract keywords from all thesauri, deduplicated."""
    keywords: set[str] = set()

    # Find all MD_Keywords blocks
    for md_keywords in md.findall(
        ".//gmd:identificationInfo/gmd:MD_DataIdentification"
        "/gmd:descriptiveKeywords/gmd:MD_Keywords",
        NAMESPACES,
    ):
        # Extract from gco:CharacterString
        for kw_elem in md_keywords.findall(".//gmd:keyword/gco:CharacterString", NAMESPACES):
            if kw_elem.text and kw_elem.text.strip():
                keywords.add(kw_elem.text.strip())

        # Extract from gmx:Anchor
        for kw_elem in md_keywords.findall(".//gmd:keyword/gmx:Anchor", NAMESPACES):
            if kw_elem.text and kw_elem.text.strip():
                keywords.add(kw_elem.text.strip())

    return sorted(keywords) if keywords else None


def _get_contact_organization(md: Element) -> str | None:
    """Extract primary contact organization."""
    # Try pointOfContact first
    org = _get_text(
        md,
        ".//gmd:identificationInfo/gmd:MD_DataIdentification/gmd:pointOfContact"
        "/gmd:CI_ResponsibleParty/gmd:organisationName/gco:CharacterString",
    )
    if org:
        return org

    # Fall back to metadata contact
    return _get_text(
        md, ".//gmd:contact/gmd:CI_ResponsibleParty/gmd:organisationName/gco:CharacterString"
    )


def _get_contact_email(md: Element) -> str | None:
    """Extract primary contact email."""
    # Try pointOfContact first
    email = _get_text(
        md,
        ".//gmd:identificationInfo/gmd:MD_DataIdentification/gmd:pointOfContact"
        "/gmd:CI_ResponsibleParty/gmd:contactInfo/gmd:CI_Contact/gmd:address"
        "/gmd:CI_Address/gmd:electronicMailAddress/gco:CharacterString",
    )
    if email:
        return email

    # Fall back to metadata contact
    return _get_text(
        md,
        ".//gmd:contact/gmd:CI_ResponsibleParty/gmd:contactInfo/gmd:CI_Contact"
        "/gmd:address/gmd:CI_Address/gmd:electronicMailAddress/gco:CharacterString",
    )


def _get_license_url(md: Element) -> str | None:
    """Extract license URL from use constraints."""
    # Look for Anchor elements with license URLs
    for constraint in md.findall(
        ".//gmd:identificationInfo/gmd:MD_DataIdentification/gmd:resourceConstraints"
        "/gmd:MD_LegalConstraints",
        NAMESPACES,
    ):
        # Check useConstraints
        for other in constraint.findall(".//gmd:otherConstraints/gmx:Anchor", NAMESPACES):
            href = other.get(f"{{{NAMESPACES['xlink']}}}href", "")
            if "creativecommons.org" in href or "opensource.org" in href:
                return href

    return None


def _get_license_text(md: Element) -> str | None:
    """Extract license text description."""
    # Look for Anchor elements with license text
    for constraint in md.findall(
        ".//gmd:identificationInfo/gmd:MD_DataIdentification/gmd:resourceConstraints"
        "/gmd:MD_LegalConstraints",
        NAMESPACES,
    ):
        for other in constraint.findall(".//gmd:otherConstraints/gmx:Anchor", NAMESPACES):
            href = other.get(f"{{{NAMESPACES['xlink']}}}href", "")
            if "creativecommons.org" in href or "opensource.org" in href:
                if other.text and other.text.strip():
                    return other.text.strip()

    return None


def _get_access_constraints(md: Element) -> str | None:
    """Extract access constraints text."""
    # Look for accessConstraints other constraints
    for constraint in md.findall(
        ".//gmd:identificationInfo/gmd:MD_DataIdentification/gmd:resourceConstraints"
        "/gmd:MD_LegalConstraints",
        NAMESPACES,
    ):
        # Check if this is an access constraint block
        access_code = constraint.find(".//gmd:accessConstraints", NAMESPACES)
        if access_code is not None:
            # Get the text from otherConstraints
            other = constraint.find(".//gmd:otherConstraints/gmx:Anchor", NAMESPACES)
            if other is not None and other.text:
                return other.text.strip()
            other_cs = constraint.find(".//gmd:otherConstraints/gco:CharacterString", NAMESPACES)
            if other_cs is not None and other_cs.text:
                return other_cs.text.strip()

    return None


def _get_lineage(md: Element) -> str | None:
    """Extract data lineage statement."""
    return _get_text(
        md,
        ".//gmd:dataQualityInfo/gmd:DQ_DataQuality/gmd:lineage"
        "/gmd:LI_Lineage/gmd:statement/gco:CharacterString",
    )


def _get_thumbnail_url(md: Element) -> str | None:
    """Extract thumbnail/graphic overview URL."""
    return _get_text(
        md,
        ".//gmd:identificationInfo/gmd:MD_DataIdentification/gmd:graphicOverview"
        "/gmd:MD_BrowseGraphic/gmd:fileName/gco:CharacterString",
    )


def _get_scale_denominator(md: Element) -> int | None:
    """Extract representative scale denominator."""
    text = _get_text(
        md,
        ".//gmd:identificationInfo/gmd:MD_DataIdentification/gmd:spatialResolution"
        "/gmd:MD_Resolution/gmd:equivalentScale/gmd:MD_RepresentativeFraction"
        "/gmd:denominator/gco:Integer",
    )
    if text:
        try:
            return int(text)
        except ValueError:
            return None
    return None


def _get_topic_category(md: Element) -> str | None:
    """Extract topic category code."""
    return _get_text(
        md,
        ".//gmd:identificationInfo/gmd:MD_DataIdentification/gmd:topicCategory"
        "/gmd:MD_TopicCategoryCode",
    )


def _get_maintenance_frequency(md: Element) -> str | None:
    """Extract maintenance frequency code."""
    freq_elem = md.find(
        ".//gmd:identificationInfo/gmd:MD_DataIdentification/gmd:resourceMaintenance"
        "/gmd:MD_MaintenanceInformation/gmd:maintenanceAndUpdateFrequency"
        "/gmd:MD_MaintenanceFrequencyCode",
        NAMESPACES,
    )
    if freq_elem is not None:
        return freq_elem.get("codeListValue")
    return None


def _get_date_by_type(md: Element, date_type: str) -> str | None:
    """Extract date by type (creation, revision, publication)."""
    for ci_date in md.findall(
        ".//gmd:identificationInfo/gmd:MD_DataIdentification/gmd:citation"
        "/gmd:CI_Citation/gmd:date/gmd:CI_Date",
        NAMESPACES,
    ):
        type_code = ci_date.find(".//gmd:dateType/gmd:CI_DateTypeCode", NAMESPACES)
        if type_code is not None and type_code.get("codeListValue") == date_type:
            date_text = _get_text(ci_date, ".//gmd:date/gco:Date")
            if date_text:
                return date_text
            # Try DateTime format
            date_text = _get_text(ci_date, ".//gmd:date/gco:DateTime")
            if date_text:
                # Extract date portion from datetime
                return date_text.split("T")[0]

    return None
