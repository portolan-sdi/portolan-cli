#!/usr/bin/env python3
"""Check that an issue or pull request body reads well before it is filed.

This is a Claude Code hook. It runs two ways:

    writing_check.py --print-rules        SessionStart: activate the style
    writing_check.py                      PreToolUse: read hook JSON on stdin

It also runs by hand, which is how the tests drive it:

    gh pr view 40 --json body -q .body | writing_check.py --kind pr --stdin

The rules are an output style. They live in
.claude/output-styles/simplified-technical-english.md, which is Simplified
Technical English (ASD-STE100). At session start this hook prints that file as
context, which activates it for the repo. It does the same job for one repo
that a personal prose-style-activate.js hook does globally.

The checks here are the STE rules a machine can verify. Verb form carries most
of the weight, because a gerund, a passive, and a perfect tense are what a
model writes by default. A sentence over 20 words fails, which is the STE
limit. Article dropping and noun clusters need part-of-speech data, so they
advise or are absent.

An issue body must also carry the headings of the issue form its labels
select, with every required section filled in. GitHub applies a form only in
the web UI, so `gh issue create` skips it unless this hook holds the body to
it. The forms come from the repo's .github/ISSUE_TEMPLATE, or from the org
.github repo when the repo has none. scripts/issue_governance.py runs the
same check again after the issue exists.

A `gh issue create` or `gh pr create` whose body the hook cannot read is
denied, with the fix in the message. So is an issue filed through `gh api`.
Otherwise the checker denies only when it affirmatively found a blocking
hit. An internal error exits zero and silent, because a bug here must never
stop a person from filing an issue. Issue governance still checks the body.

Standard library only: it runs in every repo with no install step.
"""

from __future__ import annotations

import argparse
import bisect
import json
import re
import shlex
import sys
from pathlib import Path

MASK = "\x00"  # Not prose. Never matched by a rule.
KEEP = "\x01"  # Opaque, but counts as one word.

STYLE_DIR = Path(__file__).resolve().parent.parent / "output-styles"
STYLE = STYLE_DIR / "simplified-technical-english.md"


def rules_text() -> str:
    """The Simplified Technical English output style, as context to inject.

    The rules live in the output style, not here. This hook activates that
    style for the repo the same way ~/.claude/hooks/prose-style-activate.js
    activates one globally. A paraphrase kept in this file would drift.
    """
    header = (
        "SIMPLIFIED TECHNICAL ENGLISH ACTIVE for this repo. It applies to "
        "issue bodies, pull request bodies, commit message bodies, and "
        "lasting code comments. It does not apply to conversational chat. "
        "Follow it every time you write such prose:\n\n"
    )
    try:
        body = STYLE.read_text(encoding="utf-8")
    except OSError:
        return header + (
            "Short sentences, 20 words maximum. One idea per sentence. Active "
            "voice. Simple verb forms only: no gerund, no present participle, "
            "no perfect tense. A verb, not a noun made from a verb. No em "
            "dash, semicolon, or mid-sentence colon. No filler or hype. State "
            "the outcome first. Code and quoted output stay exact."
        )
    return header + re.sub(r"\A---\n.*?\n---\n", "", body, flags=re.DOTALL)


PR_REQUIRED: tuple[tuple[str, ...], ...] = (
    ("What changed", "What this changes"),
    ("Why",),
    ("Verification",),
)


# --------------------------------------------------------------------------
# Masking
# --------------------------------------------------------------------------

HTML_COMMENT_RE = re.compile(r"<!--.*?-->", re.DOTALL)
HTML_COMMENT_TAIL_RE = re.compile(r"<!--(?!.*?-->).*\Z", re.DOTALL)
FENCE_OPEN_RE = re.compile(r"^ {0,3}(`{3,}|~{3,})")
HEADING_RE = re.compile(r"^ {0,3}(#{1,6})\s+(.*?)\s*#*\s*$")
LIST_ITEM_RE = re.compile(r"^ {0,3}(?:[-*+]|\d{1,9}[.)])\s+")
QUOTE_RE = re.compile(r"^ {0,3}>")
INDENTED_RE = re.compile(r"^ {4,}\S")
TABLE_RE = re.compile(r"^[^|\n]*\|[^|\n]*\|")

INLINE_MASKS = (
    re.compile(r"(?<!`)(`+)(?!`).*?(?<!`)\1(?!`)", re.DOTALL),  # inline code
    re.compile(r"<[a-zA-Z][^>\s]*://[^>]*>"),  # autolink
    re.compile(r"(?:https?|s3|gs|az|abfss?|file|ftp)://\S+"),  # bare URL
    re.compile(r"(?<=\])\([^)\s]+(?:\s+\"[^\"]*\")?\)"),  # link destination
    re.compile(r"^\s{0,3}\[[^\]]+\]:\s*\S+.*$", re.MULTILINE),  # link def
    re.compile(r"&[#\w]{1,10};"),  # HTML entity
    re.compile(r"(?<![\w/])[\w.-]+/[\w.-]+#\d+"),  # cross-repo ref
    re.compile(r"(?<![\w&])#\d+\b"),  # issue ref
    re.compile(r"(?<![\w/])@[\w-]+"),  # handle
    re.compile(r"\b[0-9a-f]{7,40}\b"),  # SHA
    re.compile(r"(?<![\w-])[~.]?/?[\w.-]+(?:/[\w.-]+)+(?![\w-])"),  # path
    re.compile(
        r"(?<![\w-])[\w-]+\.(?:md|py|ya?ml|json|toml|txt|csv|tiff?|parquet"
        r"|geojson|html|css|js|ts|tsx|sh|cfg|ini|lock|rs|go|sql|pmtiles|cog)"
        r"(?![\w-])"
    ),  # filename
)

# Tokens that carry a period but do not end a sentence.
PROTECT = (
    re.compile(
        r"\b(?:e\.g\.|i\.e\.|etc\.|cf\.|vs\.|approx\.|est\.|al\.|Fig\.|No\."
        r"|Dr\.|Mr\.|Ms\.|Mrs\.|Prof\.|St\.|Inc\.|Ltd\.|Jr\.|Sr\."
        r"|Ph\.D\.|U\.S\.|a\.m\.|p\.m\.)",
        re.IGNORECASE,
    ),
    re.compile(r"\bv?\d+(?:\.\d+)+\b"),  # v1.2.3, 0.16.0
    re.compile(r"\.{2,}"),  # ellipsis
    re.compile(r"\b[A-Z]\."),  # initial
)

SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])[\"')\]]*\s+(?=[\"'(\[]*[A-Z0-9])")


class Doc:
    """A body split into lines, with a masked twin of the same shape."""

    def __init__(self, text: str) -> None:
        self.text = text.replace("\r\n", "\n")
        self.starts = [0]
        for i, ch in enumerate(self.text):
            if ch == "\n":
                self.starts.append(i + 1)
        flags = [False] * len(self.text)
        self._mask_comments(flags)
        self.kinds = self._mask_lines(flags)
        self._mask_inline(flags)
        pairs = zip(self.text, flags, strict=True)
        self.masked = "".join(ch if ch == "\n" or not f else MASK for ch, f in pairs)

    # -- construction helpers ------------------------------------------
    def _fill(self, flags: list[bool], start: int, end: int) -> None:
        for i in range(start, min(end, len(flags))):
            flags[i] = True

    def _mask_comments(self, flags: list[bool]) -> None:
        for pattern in (HTML_COMMENT_RE, HTML_COMMENT_TAIL_RE):
            for m in pattern.finditer(self.text):
                self._fill(flags, m.start(), m.end())

    def _line_span(self, n: int) -> tuple[int, int]:
        start = self.starts[n]
        end = self.starts[n + 1] if n + 1 < len(self.starts) else len(self.text)
        return start, end

    def _mask_lines(self, flags: list[bool]) -> list[str]:
        """Classify every line and mask the ones that are not prose."""
        kinds: list[str] = []
        fence: str | None = None
        for n in range(len(self.starts)):
            start, end = self._line_span(n)
            raw = self.text[start:end].rstrip("\n")
            masked_out = all(flags[start:end]) if end > start else False

            if fence is not None:
                kinds.append("code")
                self._fill(flags, start, end)
                if raw.strip().startswith(fence):
                    fence = None
                continue

            opened = FENCE_OPEN_RE.match(raw)
            if opened and not masked_out:
                fence = opened.group(1)
                kinds.append("code")
                self._fill(flags, start, end)
                continue

            if masked_out or not raw.strip():
                kinds.append("blank")
                continue
            if HEADING_RE.match(raw):
                kinds.append("heading")
                continue
            if INDENTED_RE.match(raw) or QUOTE_RE.match(raw) or TABLE_RE.match(raw):
                kinds.append("code")
                self._fill(flags, start, end)
                continue
            if LIST_ITEM_RE.match(raw):
                kinds.append("list_item")
                continue
            kinds.append("prose")
        return kinds

    def _mask_inline(self, flags: list[bool]) -> None:
        pairs = zip(self.text, flags, strict=True)
        live = "".join(ch if (ch == "\n" or not f) else MASK for ch, f in pairs)
        for pattern in INLINE_MASKS:
            for m in pattern.finditer(live):
                self._fill(flags, m.start(), m.end())

    # -- lookups -------------------------------------------------------
    def line_of(self, offset: int) -> int:
        return bisect.bisect_right(self.starts, offset)

    def col_of(self, offset: int) -> int:
        return offset - self.starts[self.line_of(offset) - 1] + 1

    def excerpt(self, start: int, end: int, width: int = 22) -> str:
        left = max(0, start - width)
        right = min(len(self.text), end + width)
        head = self.text[left:start].replace("\n", " ")
        body = self.text[start:end].replace("\n", " ")
        tail = self.text[end:right].replace("\n", " ")
        out = f"{'...' if left else ''}{head}>>{body}<<{tail}"
        return " ".join(out.split())

    def headings(self) -> list[tuple[int, str]]:
        out = []
        for n, kind in enumerate(self.kinds):
            if kind != "heading":
                continue
            start, end = self._line_span(n)
            m = HEADING_RE.match(self.text[start:end].rstrip("\n"))
            if m:
                out.append((n, m.group(2).strip()))
        return out

    def section_bounds(self) -> list[tuple[str, int, int]]:
        marks = self.headings()
        out = []
        for i, (line_no, title) in enumerate(marks):
            end = marks[i + 1][0] if i + 1 < len(marks) else len(self.kinds)
            out.append((title, line_no, end))
        return out

    def is_live(self, offset: int) -> bool:
        return self.masked[offset] not in (MASK, "\n")


# --------------------------------------------------------------------------
# Sentences
# --------------------------------------------------------------------------


class Sentence:
    def __init__(self, start: int, text: str) -> None:
        self.start = start
        self.text = text

    def words(self) -> int:
        return len(re.findall(r"\S+", self.text.replace(MASK, "x")))


def sentences(doc: Doc) -> list[Sentence]:
    """Split prose into sentences, with offsets into the original text."""
    out: list[Sentence] = []
    block: list[int] = []

    def flush(single: bool = False) -> None:
        if not block:
            return
        start = doc.starts[block[0]]
        last = block[-1]
        end = doc.starts[last + 1] if last + 1 < len(doc.starts) else len(doc.masked)
        chunk = doc.masked[start:end]
        if single:
            # A list item is one unit. Its internal periods are usually
            # abbreviations or a clipped clause, not sentence ends.
            if chunk.strip(MASK + " \t\n"):
                out.append(Sentence(start, chunk))
            block.clear()
            return
        guard = list(chunk)
        for pattern in PROTECT:
            for m in pattern.finditer(chunk):
                for i in range(m.start(), m.end()):
                    guard[i] = KEEP
        guarded = "".join(guard)
        offset = 0
        for piece in SENTENCE_SPLIT_RE.split(guarded):
            if piece.strip(MASK + KEEP + " \t\n"):
                span = chunk[offset : offset + len(piece)]
                out.append(Sentence(start + offset, span))
            offset += len(piece)
            # Re-align past the whitespace the split consumed.
            while offset < len(guarded) and guarded[offset] in " \t\n\"')]":
                offset += 1
        block.clear()

    for n, kind in enumerate(doc.kinds):
        if kind == "prose":
            block.append(n)
            continue
        flush()
        if kind == "list_item":
            block.append(n)
            flush(single=True)
    flush()
    return out


# --------------------------------------------------------------------------
# Rules
# --------------------------------------------------------------------------


def _words(*items: str) -> re.Pattern[str]:
    body = "|".join(items)
    return re.compile(rf"(?<![\w-])(?:{body})(?![\w-])", re.IGNORECASE)


def _phrases(*items: str) -> re.Pattern[str]:
    body = "|".join(i.replace(" ", r"\s+") for i in items)
    return re.compile(rf"(?<![\w-])(?:{body})(?![\w-])", re.IGNORECASE)


VOCAB: tuple[tuple[str, re.Pattern[str], str], ...] = (
    (
        "FILLER",
        _words(
            "just",
            "really",
            "simply",
            "basically",
            "actually",
            "truly",
            "obviously",
            "of course",
        ),
        "delete it",
    ),
    (
        "HYPE",
        _phrases(
            "powerful",
            "seamless",
            "seamlessly",
            "effortless",
            "robust",
            "blazing.fast",
            "lightning.fast",
            "cutting.edge",
            "world.class",
            "best.in.class",
            "rock.solid",
            "state.of.the.art",
            "game.changing",
            "revolutionary",
            "supercharge",
            "unleash",
            "delve",
            "elevate",
        ),
        "show the behavior",
    ),
    (
        "VAGUE_VERB",
        _words(
            "leverage",
            "leverages",
            "leveraged",
            "leveraging",
            "facilitate",
            "facilitates",
            "facilitated",
            "utilize",
            "utilizes",
            "utilized",
            "utilizing",
            "utilization",
        ),
        "name the action",
    ),
    (
        "WORDY",
        _phrases(
            "prior to",
            "in order to",
            "due to the fact that",
            "at this point in time",
            "at the present time",
            "in the event that",
            "subsequent to",
            "has the ability to",
            "is able to",
            "are able to",
            "make use of",
            "a large number of",
        ),
        "use the short form",
    ),
    (
        "IDIOM",
        _phrases(
            "at the end of the day",
            "low.hanging fruit",
            "silver bullet",
            "rabbit hole",
            "heavy lifting",
            "boils down to",
            "in the weeds",
            "moving parts",
            "bread and butter",
            "tip of the iceberg",
        ),
        "say it plainly",
    ),
    (
        "QUALIFIER",
        _words("very", "quite", "somewhat", "fairly"),
        "delete it",
    ),
)

_TEMPORAL = r"just\s+(?:before|after|under|over|below|above|\d)"
FILLER_TEMPORAL_RE = re.compile(_TEMPORAL, re.IGNORECASE)
ROBUST_STATS_RE = re.compile(
    r"robust\s+(?:to|against|regression|estimator|standard\s+errors)",
    re.IGNORECASE,
)
RATHER_THAN_RE = re.compile(r"rather\s+than", re.IGNORECASE)

CLOSING_TAIL_RE = _phrases(
    "and that.s it",
    "that.s all you need to know",
    "now you.re ready to",
    "it.s that simple",
    "in conclusion",
    "to summarize",
    "to sum up",
    "hope this helps",
)

EM_DASH_RE = re.compile(r"—|–|(?<= )--(?=[ ])")
DIGIT_RANGE_RE = re.compile(r"\d\s*–\s*\d")
SEMICOLON_RE = re.compile(r";")
MID_COLON_RE = re.compile(r"(?<=[a-z]) *: +(?=[a-z])")
LABEL_COLON_RE = re.compile(r"^\s*\*{0,2}[A-Z][\w ]{0,24}\*{0,2}:")
# STE allows only simple verb forms: infinitive, imperative, simple present,
# simple past, simple future. These three rules carry the weight, because a
# gerund, a passive, and a perfect tense are what an LLM writes by default.
PASSIVE_RE = re.compile(
    r"\b(?:is|are|was|were|be|been|being)\s+(?:\w+ly\s+)?(\w+(?:ed|en))\b",
    re.IGNORECASE,
)
PERFECT_RE = re.compile(
    r"\b(?:has|have|had)\s+(?:\w+ly\s+)?(?:been\s+)?(\w+(?:ed|en)|done|made|"
    r"gone|come|run|set|put|written|taken|given|shown|known|seen|kept|held|"
    r"built|sent|found|read|left|lost|met|paid|said|told)\b",
    re.IGNORECASE,
)
ING_RE = re.compile(r"(?<![\w-])([A-Za-z]{4,}ing)(?![\w-])")

# -ing words that are ordinary nouns or fixed adjectives, not verb forms.
ING_OK = frozenset(
    {
        "thing",
        "things",
        "string",
        "strings",
        "during",
        "nothing",
        "something",
        "anything",
        "everything",
        "morning",
        "evening",
        "ceiling",
        "spring",
        "setting",
        "settings",
        "heading",
        "headings",
        "warning",
        "warnings",
        "listing",
        "listings",
        "mapping",
        "mappings",
        "encoding",
        "encodings",
        "logging",
        "tooling",
        "versioning",
        "engineering",
        "building",
        "buildings",
        "meaning",
        "wording",
        "missing",
        "existing",
        "remaining",
        "padding",
        "casing",
        "ordering",
        "spacing",
        "naming",
        "packaging",
        "tracing",
        "routing",
        "caching",
        "indexing",
        "parsing",
        "linting",
        "testing",
        "typing",
        "branching",
        "staging",
        "landing",
        "backing",
        "leading",
        "trailing",
        "matching",
        "escaping",
        "wrapping",
        "nesting",
        "chunking",
        "tiling",
        "shading",
    }
)
PASSIVE_OK = frozenset(
    {
        "deprecated",
        "required",
        "based",
        "located",
        "supported",
        "documented",
        "expected",
        "related",
        "involved",
        "interested",
        "dedicated",
        "sophisticated",
        "caused",
        "followed",
        "accompanied",
    }
)

# STE sets 20 words for descriptive text and 20 for a procedural step.
BLOCK_MAX_WORDS = 20
ADVISE_MAX_WORDS = 16
ADVISE_MAX_SENTENCES = 6

_SUPPRESS = r"<!--\s*ste-ok:\s*([A-Z_]+(?:\s+[A-Z_]+)*)\s+(.{8,}?)\s*-->"
SUPPRESS_RE = re.compile(_SUPPRESS, re.DOTALL)
SKIP_RE = re.compile(r"<!--\s*ste-skip:\s*(.{8,}?)\s*-->", re.DOTALL)


class Finding:
    def __init__(
        self,
        rule: str,
        line: int,
        col: int,
        fix: str,
        excerpt: str,
        blocking: bool = True,
    ) -> None:
        self.rule = rule
        self.line = line
        self.col = col
        self.fix = fix
        self.excerpt = excerpt
        self.blocking = blocking

    def format(self) -> str:
        where = f"L{self.line}:{self.col}"
        return f"{where:<8} {self.rule:<13} {self.fix:<28} {self.excerpt}"


def _blank_fences(text: str) -> str:
    """Blank fenced lines, keeping length and line breaks.

    A marker inside a fence is quoted material, not a claim about this body.
    The waiver checkbox in lint_body.py already works this way.
    """
    out: list[str] = []
    fence: str | None = None
    for line in text.split("\n"):
        opened = FENCE_OPEN_RE.match(line)
        if fence is not None:
            out.append(" " * len(line))
            if line.strip().startswith(fence):
                fence = None
            continue
        if opened:
            fence = opened.group(1)
            out.append(" " * len(line))
            continue
        out.append(line)
    return "\n".join(out)


def _suppressions(text: str) -> tuple[bool, dict[int, set[str]], list[str]]:
    """Return whole-body skip, per-line rule suppressions, and their labels."""
    text = _blank_fences(text.replace("\r\n", "\n"))
    doc_starts = [0] + [i + 1 for i, ch in enumerate(text) if ch == "\n"]

    def line_at(offset: int) -> int:
        return bisect.bisect_right(doc_starts, offset)

    if SKIP_RE.search(text):
        return True, {}, ["whole body"]

    per_line: dict[int, set[str]] = {}
    labels: list[str] = []
    lines = text.split("\n")
    for m in SUPPRESS_RE.finditer(text):
        rules = set(m.group(1).split())
        here = line_at(m.start())
        target = here
        # End-of-line marker applies to its own line; otherwise the next
        # line that holds something.
        if lines[here - 1].strip().startswith("<!--"):
            target = here + 1
            while target <= len(lines) and not lines[target - 1].strip():
                target += 1
        per_line.setdefault(target, set()).update(rules)
        labels.extend(f"{r} L{target}" for r in sorted(rules))
    return False, per_line, labels


def review(body: str, kind: str) -> tuple[list[Finding], list[str]]:
    """Return findings and the suppressions that were honored."""
    skip_all, per_line, labels = _suppressions(body)
    if skip_all:
        return [], labels

    doc = Doc(body)
    found: list[Finding] = []

    def add(rule: str, start: int, end: int, fix: str, blocking: bool = True) -> None:
        line = doc.line_of(start)
        if rule in per_line.get(line, set()):
            return
        col = doc.col_of(start)
        text = doc.excerpt(start, end)
        found.append(Finding(rule, line, col, fix, text, blocking))

    live = doc.masked
    if not live.replace(MASK, "").strip():
        found.append(Finding("EMPTY_BODY", 1, 1, "use the template", "", True))
        return found, labels

    # Vocabulary.
    for rule, pattern, fix in VOCAB:
        for m in pattern.finditer(live):
            word = live[m.start() : m.end()]
            if rule == "FILLER" and FILLER_TEMPORAL_RE.match(live[m.start() :]):
                continue
            if rule == "HYPE" and ROBUST_STATS_RE.match(live[m.start() :]):
                continue
            add(rule, m.start(), m.end(), f'"{word}" -> {fix}')
    for m in _words("rather").finditer(live):
        if RATHER_THAN_RE.match(live[m.start() :]):
            continue
        add("QUALIFIER", m.start(), m.end(), '"rather" -> delete it')

    # Punctuation.
    for m in EM_DASH_RE.finditer(live):
        if DIGIT_RANGE_RE.search(live[max(0, m.start() - 3) : m.end() + 3]):
            continue
        add("EM_DASH", m.start(), m.end(), "split into two sentences")
    for m in SEMICOLON_RE.finditer(live):
        add("SEMICOLON", m.start(), m.end(), "split into two sentences")
    for m in MID_COLON_RE.finditer(live):
        line_no = doc.line_of(m.start())
        raw = body.split("\n")[line_no - 1]
        if LABEL_COLON_RE.match(raw):
            continue
        after = live[m.end() : doc.starts[line_no - 1] + len(raw)]
        if len(re.findall(r"\S+", after.replace(MASK, "x"))) < 4:
            continue
        if line_no < len(doc.kinds) and doc.kinds[line_no] in ("list_item", "code"):
            continue
        add("MID_COLON", m.start(), m.end(), "use a period, not a colon")

    # Sentences.
    for sentence in sentences(doc):
        count = sentence.words()
        if count > BLOCK_MAX_WORDS:
            add(
                "LONG_SENTENCE",
                sentence.start,
                sentence.start + min(len(sentence.text), 40),
                f"{count} words; split it",
            )
        elif count > ADVISE_MAX_WORDS:
            add(
                "SENTENCE_LONG",
                sentence.start,
                sentence.start + min(len(sentence.text), 40),
                f"{count} words; consider splitting",
                blocking=False,
            )

    # Verb forms. STE allows simple tenses and the active voice only.
    for m in PASSIVE_RE.finditer(live):
        if m.group(1).lower() in PASSIVE_OK:
            continue
        add("PASSIVE", m.start(), m.end(), "name who does the action")
    for m in PERFECT_RE.finditer(live):
        add("PERFECT_TENSE", m.start(), m.end(), "use the simple tense")
    for m in ING_RE.finditer(live):
        word = m.group(1).lower()
        if word in ING_OK:
            continue
        add("GERUND", m.start(), m.end(), f'"{m.group(1)}" -> use a simple verb')

    # Closing tails, only near the end of a section.
    bounds = doc.section_bounds() or [("", 0, len(doc.kinds))]
    for _title, first, last in bounds:
        span_start = doc.starts[first]
        span_end = doc.starts[last] if last < len(doc.starts) else len(live)
        segment = live[span_start:span_end]
        for m in CLOSING_TAIL_RE.finditer(segment):
            tail = segment[m.end() :]
            if len(re.findall(r"\S+", tail.replace(MASK, ""))) > 25:
                continue
            add(
                "CLOSING_TAIL",
                span_start + m.start(),
                span_start + m.end(),
                "end on the last point",
            )

    # Structure.
    found.extend(_structure(doc, kind, per_line))
    found.sort(key=lambda f: (f.line, f.col))
    return found, labels


def _has_content(doc: Doc, first: int, last: int) -> bool:
    """Whether lines [first, last) hold prose or pasted output.

    A fence marker on its own is punctuation, not content. Everything a
    reader would see counts, so an empty section really is empty.
    """
    for n in range(first, min(last, len(doc.kinds))):
        kind = doc.kinds[n]
        if kind == "blank":
            continue
        start, end = doc._line_span(n)
        raw = doc.text[start:end].strip()
        if kind == "code":
            if raw and not FENCE_OPEN_RE.match(raw):
                return True
            continue
        if doc.masked[start:end].replace(MASK, "").strip():
            return True
    return False


def _structure(doc: Doc, kind: str, per_line: dict[int, set[str]]) -> list[Finding]:
    if kind != "pr":
        return []
    titles = [t for _n, t in doc.headings()]
    if not titles:
        return [Finding("HEADING_MISSING", 1, 1, "use the pull request template", "")]
    out: list[Finding] = []
    norm = [t.casefold().rstrip("?:") for t in titles]
    seen: list[int] = []
    for names in PR_REQUIRED:
        keys = [n.casefold().rstrip("?:") for n in names]
        here = [norm.index(k) for k in keys if k in norm]
        if not here:
            out.append(
                Finding(
                    "HEADING_MISSING",
                    1,
                    1,
                    f'"## {names[0]}" is missing',
                    "",
                )
            )
        else:
            seen.append(min(here))
    if len(seen) == len(PR_REQUIRED) and seen != sorted(seen):
        out.append(
            Finding(
                "HEADING_ORDER",
                1,
                1,
                "outcome first: " + ", ".join(n[0] for n in PR_REQUIRED),
                "",
            )
        )
    wanted = {n.casefold().rstrip("?:") for names in PR_REQUIRED for n in names}
    for title, first, last in doc.section_bounds():
        if title.casefold().rstrip("?:") not in wanted:
            continue
        muted = "HEADING_EMPTY" in per_line.get(first + 1, set())
        if not _has_content(doc, first + 1, last) and not muted:
            fix = f'"## {title}" is empty'
            out.append(Finding("HEADING_EMPTY", first + 1, 1, fix, ""))
    return out


# --------------------------------------------------------------------------
# Issue forms
# --------------------------------------------------------------------------
#
# GitHub applies an issue form only in the web UI. `gh issue create --body`
# and the REST API skip it, and the body then has whatever headings its
# author typed. This section reads the forms themselves and holds a body to
# the headings the web form would write. The hook runs it before filing.
# scripts/issue_governance.py imports it and runs it again after filing. A
# form change needs no change here.

PRIORITIES = ("P0", "P1", "P2", "P3")
PRIORITY_HEADING = "Priority"
NO_RESPONSE = "_No response_"
FIELD_TYPES = ("textarea", "input", "dropdown")
ORG = "portolan-sdi"
FORMS_DIR = Path(".github") / "ISSUE_TEMPLATE"


class FormField:
    def __init__(
        self,
        label: str,
        required: bool,
        options: list[str],
        multiple: bool = False,
    ) -> None:
        self.label = label
        self.required = required
        self.options = options
        self.multiple = multiple


class IssueForm:
    def __init__(self, name: str, labels: list[str], fields: list[FormField]) -> None:
        self.name = name
        self.labels = labels
        self.fields = fields


_YAML_KEY_RE = re.compile(r"^([A-Za-z0-9_-]+):(?:\s+(.*))?$")


def _yaml_scalar(raw: str) -> object:
    """One scalar or flow sequence, with any trailing comment removed."""
    raw = raw.strip()
    if raw.startswith('"'):
        end = 1
        while end < len(raw) and raw[end] != '"':
            end += 2 if raw[end] == "\\" else 1
        return json.loads(raw[: end + 1])
    if raw.startswith("'"):
        m = re.match(r"'((?:[^']|'')*)'", raw)
        return m.group(1).replace("''", "'") if m else raw
    raw = re.sub(r"\s+#.*$", "", raw)
    if raw.startswith("[") and raw.endswith("]"):
        inner = raw[1:-1].strip()
        if not inner:
            return []
        return [_yaml_scalar(part) for part in inner.split(",")]
    if raw in ("true", "false"):
        return raw == "true"
    return raw


def _yaml_load(text: str) -> object:
    """Load the YAML subset that issue forms use.

    The hook and the governance script run with no install step, so PyYAML
    is out of reach. The subset covers block mappings, block sequences, flow
    sequences of scalars, quoted and plain scalars, and folded or literal
    block scalars. Block scalar whitespace and dates can differ from PyYAML.
    No check reads them. test_writing_check.py compares the parsed form with
    the form PyYAML gives, for every form ops ships.
    """
    lines = text.replace("\r\n", "\n").split("\n")
    pos = 0

    def indent(line: str) -> int:
        return len(line) - len(line.lstrip(" "))

    def significant() -> int | None:
        """Indent of the next line that holds data, or None at the end."""
        nonlocal pos
        while pos < len(lines):
            stripped = lines[pos].strip()
            if stripped and not stripped.startswith("#"):
                return indent(lines[pos])
            pos += 1
        return None

    def block_scalar(style: str, parent: int) -> str:
        nonlocal pos
        out: list[str] = []
        while pos < len(lines):
            line = lines[pos]
            if line.strip() and indent(line) <= parent:
                break
            out.append(line.strip())
            pos += 1
        while out and not out[-1]:
            out.pop()
        joiner = "\n" if style.startswith("|") else " "
        return joiner.join(out)

    def value_after_key(rest: str, level: int) -> object:
        nonlocal pos
        pos += 1
        if rest and rest[0] in "|>":
            return block_scalar(rest, level)
        if rest:
            value = _yaml_scalar(rest)
            if isinstance(value, str) and not rest.startswith(("'", '"')):
                # A plain scalar may continue on deeper lines.
                more: list[str] = []
                while (nxt := significant()) is not None and nxt > level:
                    more.append(lines[pos].strip())
                    pos += 1
                value = " ".join([value, *more])
            return value
        nxt = significant()
        if nxt is None:
            return None
        if nxt > level or (nxt == level and lines[pos].lstrip().startswith("- ")):
            return block(nxt)
        return None

    def block(level: int) -> object:
        if lines[pos].lstrip().startswith("-"):
            return sequence(level)
        return mapping(level)

    def mapping(level: int) -> dict[str, object]:
        out: dict[str, object] = {}
        while (nxt := significant()) is not None and nxt == level:
            stripped = lines[pos].strip()
            if stripped.startswith("- "):
                break
            m = _YAML_KEY_RE.match(stripped)
            if not m:
                raise ValueError(f"line {pos + 1}: not a mapping key")
            out[m.group(1)] = value_after_key(m.group(2) or "", level)
        return out

    def sequence(level: int) -> list[object]:
        nonlocal pos
        out: list[object] = []
        while (nxt := significant()) is not None and nxt == level:
            stripped = lines[pos].strip()
            if not stripped.startswith("-"):
                break
            rest = stripped[1:].strip()
            if not rest:
                pos += 1
                child = significant()
                out.append(block(child) if child is not None else None)
            elif _YAML_KEY_RE.match(rest) and not rest.startswith(("'", '"')):
                # "- key: value" opens a mapping two columns in.
                lines[pos] = " " * (level + 2) + rest
                out.append(mapping(level + 2))
            else:
                out.append(_yaml_scalar(rest))
                pos += 1
        return out

    start = significant()
    return None if start is None else block(start)


def parse_form(text: str, name: str = "") -> IssueForm | None:
    """The labels and fields of one issue form, or None for another file."""
    try:
        data = _yaml_load(text)
    except (ValueError, json.JSONDecodeError):
        return None
    return form_from_data(data, name)


def form_from_data(data: object, name: str = "") -> IssueForm | None:
    """The labels and fields of a loaded issue form."""
    if not isinstance(data, dict) or not isinstance(data.get("body"), list):
        return None
    labels = data.get("labels") or []
    if isinstance(labels, str):
        labels = [part.strip() for part in labels.split(",")]
    fields: list[FormField] = []
    for item in data["body"]:
        if not isinstance(item, dict) or item.get("type") not in FIELD_TYPES:
            continue
        attrs = item.get("attributes") or {}
        checks = item.get("validations") or {}
        if not isinstance(attrs, dict) or not attrs.get("label"):
            continue
        options = attrs.get("options") or []
        fields.append(
            FormField(
                str(attrs["label"]),
                bool(isinstance(checks, dict) and checks.get("required")),
                [str(o) for o in options] if isinstance(options, list) else [],
                bool(attrs.get("multiple")),
            )
        )
    return IssueForm(name, [str(x) for x in labels], fields)


def _norm_title(title: str) -> str:
    return title.casefold().rstrip("?:").strip()


def _sections(body: str) -> dict[str, str]:
    """Each heading's text, keyed by normalized title. The first one wins."""
    doc = Doc(body)
    lines = doc.text.split("\n")
    out: dict[str, str] = {}
    for title, first, last in doc.section_bounds():
        content = "\n".join(lines[first + 1 : last])
        content = HTML_COMMENT_RE.sub("", content).strip()
        out.setdefault(_norm_title(title), "" if content == NO_RESPONSE else content)
    return out


def _option_key(option: str) -> str:
    """'P1: Fix next. ...' answers to 'P1' as well as to its full text."""
    return option.split(":", 1)[0].strip()


def _answers_option(value: str, field: FormField) -> bool:
    """Whether the answer picks a listed option.

    An option written as 'P1: Fix next. ...' also accepts any answer that
    starts with its key, such as 'P1' or 'P1: Fix next.'.
    """
    first = value.strip().split("\n", 1)[0].strip()
    picks = [p.strip() for p in first.split(",")] if field.multiple else [first]

    def known(pick: str) -> bool:
        for option in field.options:
            if pick.casefold() == option.casefold():
                return True
            key = rf"{re.escape(_option_key(option))}(?![\w-])"
            if ":" in option and re.match(key, pick, re.IGNORECASE):
                return True
        return False

    return all(known(p) for p in picks if p)


def _form_gaps(sections: dict[str, str], form: IssueForm) -> list[str]:
    gaps: list[str] = []
    for field in form.fields:
        if not field.required:
            continue
        content = sections.get(_norm_title(field.label))
        if content is None:
            gaps.append(f'"### {field.label}" is missing')
        elif not content:
            gaps.append(f'"### {field.label}" is empty')
        elif field.options and not _answers_option(content, field):
            keys = ", ".join(_option_key(o) for o in field.options)
            gaps.append(f'"### {field.label}" must be one of: {keys}')
    return gaps


def form_problems(body: str, labels: list[str], forms: list[IssueForm]) -> list[str]:
    """Why the body fails every form its labels select. Empty when it passes.

    A form applies when the issue carries every label the form sets. Two
    forms can set the same label, as the QGIS plugin form and the bug form
    both set `bug`, so the body passes when it satisfies any form that
    applies.
    """
    labelled = [f for f in forms if f.labels]
    candidates = [f for f in labelled if set(f.labels) <= set(labels)]
    if not candidates:
        choices = sorted({label for f in labelled for label in f.labels})
        if not choices:
            return []
        listed = ", ".join(choices)
        return [f"no issue form matches the labels. Add one with --label: {listed}"]
    sections = _sections(body)
    best: list[str] | None = None
    for form in candidates:
        gaps = _form_gaps(sections, form)
        if not gaps:
            return []
        if best is None or len(gaps) < len(best):
            best = [f"{form.name or 'the form'}: {gap}" for gap in gaps]
    return best or []


def form_headings(forms: list[IssueForm], labels: list[str]) -> list[str]:
    """The headings, in order, of the first form the labels select."""
    for form in forms:
        if form.labels and set(form.labels) <= set(labels):
            return [f"### {f.label}" for f in form.fields]
    return []


def priority_from_body(body: str) -> str | None:
    """The P label the Priority section names, or None."""
    content = _sections(body).get(_norm_title(PRIORITY_HEADING), "")
    m = re.match(r"(P[0-3])\b", content)
    return m.group(1) if m else None


def _read_forms(paths: list[tuple[str, str]]) -> list[IssueForm]:
    forms = []
    for name, text in paths:
        form = parse_form(text, name)
        if form is not None:
            forms.append(form)
    return forms


def local_forms(start: Path) -> list[IssueForm]:
    """The forms in the repo that holds `start`, or none."""
    for folder in (start, *start.parents):
        forms_dir = folder / FORMS_DIR
        if forms_dir.is_dir():
            files = sorted(forms_dir.glob("*.y*ml"))
            return _read_forms([(p.name, p.read_text("utf-8")) for p in files])
        if (folder / ".git").exists():
            return []
    return []


def remote_forms(repo: str) -> list[IssueForm]:
    """The forms GitHub shows for `repo`: its own, else the org defaults.

    Reads through `gh api`, which the agent filing the issue already uses.
    """
    import base64
    import subprocess

    owner = repo.split("/", 1)[0]
    for source in (repo, f"{owner}/.github"):
        base = f"repos/{source}/contents/{FORMS_DIR.as_posix()}"
        command = ["gh", "api", base]
        listing = subprocess.run(command, capture_output=True, text=True, check=False)
        if listing.returncode != 0:
            continue
        found = []
        for entry in json.loads(listing.stdout):
            name = entry.get("name", "")
            if not name.endswith((".yml", ".yaml")):
                continue
            blob = subprocess.run(
                ["gh", "api", f"{base}/{name}", "--jq", ".content"],
                capture_output=True,
                text=True,
                check=True,
            )
            found.append((name, base64.b64decode(blob.stdout).decode("utf-8")))
        forms = _read_forms(found)
        if forms:
            return forms
    return []


# --------------------------------------------------------------------------
# Reporting
# --------------------------------------------------------------------------

MAX_SHOWN = 10


def report(
    findings: list[Finding],
    labels: list[str],
    kind: str,
    hint: str = "",
) -> str:
    blocking = [f for f in findings if f.blocking]
    advisory = [f for f in findings if not f.blocking]
    noun = "pull request" if kind == "pr" else "issue"
    plural = "" if len(blocking) == 1 else "s"
    count = len(blocking)
    headline = f"Writing review: {count} blocking problem{plural} in this "
    headline += f"{noun} body. Fix and retry."
    lines = [headline, ""]
    lines.extend(f.format() for f in blocking[:MAX_SHOWN])
    if len(blocking) > MAX_SHOWN:
        lines.append(f"... and {len(blocking) - MAX_SHOWN} more.")
    if advisory:
        note = ", ".join(f"{f.rule} L{f.line}" for f in advisory[:6])
        lines += ["", f"Advisory, not blocking: {len(advisory)} ({note})."]
    if labels:
        lines += ["", f"Suppressions honored: {', '.join(labels)}."]
    if hint:
        lines += ["", hint]
    lines += [
        "",
        "Suppress a false positive with a comment on the line above:",
        "  <!-- ste-ok: RULE_ID why this one is correct -->",
    ]
    if any(f.rule == "FORM" for f in blocking):
        lines.append("A FORM problem cannot be suppressed. Fill in the section.")
    lines.append("Rules: writing_check.py --print-rules")
    return "\n".join(lines)


# --------------------------------------------------------------------------
# Hook adapter
# --------------------------------------------------------------------------

GH_RE = re.compile(r"\bgh\s+(issue|pr)\s+(create|edit)\b")
GH_CREATE_RE = re.compile(r"\bgh\s+(issue|pr)\s+create\b")
SHELL_UNSAFE_RE = re.compile(r"<<|\$\(|`")
# An issue filed through the API skips the form, the labels, and this hook.
_API_REPO = r"\bgh\s+api\b[^\n|;&]*?\brepos/[^/\s'\"]+/[^/\s'\"]+"
GH_API_ISSUE_RE = re.compile(_API_REPO + r"/issues/?(?=[\s'\"?]|$)")
GH_API_WRITE_RE = re.compile(
    r"(?:^|\s)(?:-f|-F|--field|--raw-field|--input)(?:[\s=]|$)"
    r"|(?:-X|--method)[\s=]*(?:POST|post)\b"
)
GH_API_GET_RE = re.compile(r"(?:-X|--method)[\s=]*(?:GET|get)\b")
GH_GRAPHQL_ISSUE_RE = re.compile(r"\bgh\s+api\s+graphql\b.*\bcreateIssue\b", re.DOTALL)
SHELL_OPERATORS = frozenset({"&&", "||", ";", "|"})


def api_creates_issue(command: str) -> bool:
    """Whether the command files an issue through `gh api`."""
    if GH_GRAPHQL_ISSUE_RE.search(command):
        return True
    m = GH_API_ISSUE_RE.search(command)
    if not m:
        return False
    rest = command[m.start() :].split("\n", 1)[0]
    return bool(GH_API_WRITE_RE.search(rest)) and not GH_API_GET_RE.search(rest)


def create_args(command: str) -> tuple[str, list[str]] | None:
    """The kind and the flags of a `gh issue create` or `gh pr create` call."""
    if not GH_CREATE_RE.search(command):
        return None
    try:
        argv = shlex.split(command)
    except ValueError:
        return "issue" if "gh issue" in command else "pr", []
    for i in range(len(argv) - 2):
        if argv[i] == "gh" and argv[i + 2] == "create":
            kind = "pr" if argv[i + 1] == "pr" else "issue"
            flags: list[str] = []
            for token in argv[i + 3 :]:
                if token in SHELL_OPERATORS:
                    break
                flags.append(token)
            return kind, flags
    return None


def flag_values(flags: list[str], *names: str) -> list[str]:
    """Every value given to any of `names`, as `--x v`, `-x v`, or `--x=v`."""
    out: list[str] = []
    for i, token in enumerate(flags):
        if token in names and i + 1 < len(flags):
            out.append(flags[i + 1])
        for name in names:
            if name.startswith("--") and token.startswith(name + "="):
                out.append(token.split("=", 1)[1])
    return out


def command_labels(flags: list[str]) -> list[str]:
    raw = flag_values(flags, "--label", "-l")
    return [part.strip() for value in raw for part in value.split(",") if part.strip()]


def forms_for(flags: list[str]) -> list[IssueForm]:
    """The forms GitHub applies to the repo this command files into."""
    repos = flag_values(flags, "--repo", "-R")
    if repos:
        return remote_forms(repos[-1])
    return local_forms(Path.cwd()) or remote_forms(f"{ORG}/.github")


def deny(reason: str) -> int:
    print(
        json.dumps(
            {
                "hookSpecificOutput": {
                    "hookEventName": "PreToolUse",
                    "permissionDecision": "deny",
                    "permissionDecisionReason": reason,
                }
            }
        )
    )
    return 0


UNREADABLE = (
    "The writing hook cannot read this {noun} body. It reads only a literal "
    "--body or a --body-file path that exists when the command starts. Write "
    "the body to a file in one command. Then run gh {noun_cmd} create with "
    "--body-file <path> in the next command. A heredoc, $(...), backticks, "
    "and --body-file - all hide the body."
)

FORMS_UNREADABLE = (
    "The writing hook cannot read the issue forms for this repo. It reads "
    "them from .github/ISSUE_TEMPLATE, or through gh api. Check that gh is "
    "logged in, then retry."
)

API_ISSUE = (
    "Do not file issues through gh api. That path skips the issue form and "
    "this check. Use gh issue create --body-file <path> with the form's type "
    "label, for example --label bug."
)


def body_from_command(command: str) -> tuple[str, str] | None:
    """Return (kind, body) for a gh call that carries a literal body."""
    m = GH_RE.search(command)
    if not m:
        return None
    kind = "pr" if m.group(1) == "pr" else "issue"
    if SHELL_UNSAFE_RE.search(command):
        return None
    try:
        argv = shlex.split(command)
    except ValueError:
        return None
    for i, token in enumerate(argv):
        nxt = argv[i + 1] if i + 1 < len(argv) else None
        if token in ("--body", "-b") and nxt:
            return kind, nxt
        if token.startswith("--body="):
            return kind, token.split("=", 1)[1]
        if token in ("--body-file", "-F") and nxt:
            if nxt == "-":
                return None
            return kind, Path(nxt).read_text(encoding="utf-8")
    return None


def run_hook() -> int:
    payload = json.load(sys.stdin)
    if payload.get("tool_name") != "Bash":
        return 0
    command = payload.get("tool_input", {}).get("command", "")
    if api_creates_issue(command):
        return deny(API_ISSUE)
    created = create_args(command)
    if created and ("--web" in created[1] or "-w" in created[1]):
        return 0  # The browser applies the form itself.
    try:
        found = body_from_command(command)
    except OSError:
        found = None
    if not found:
        if created:
            noun = "pull request" if created[0] == "pr" else "issue"
            return deny(UNREADABLE.format(noun=noun, noun_cmd=created[0]))
        return 0
    kind, body = found
    findings, labels = review(body, kind)
    hint = ""
    if created and created[0] == "issue":
        flags = created[1]
        forms = forms_for(flags)
        if not forms:
            return deny(FORMS_UNREADABLE)
        problems = form_problems(body, command_labels(flags), forms)
        findings.extend(Finding("FORM", 1, 1, p, "") for p in problems)
        wanted = form_headings(forms, command_labels(flags))
        if problems and wanted:
            hint = "The form wants these headings, in order: " + " | ".join(wanted)
    if not any(f.blocking for f in findings):
        return 0
    return deny(report(findings, labels, kind, hint))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Check a body's writing.")
    parser.add_argument("--print-rules", action="store_true")
    parser.add_argument("--kind", choices=("pr", "issue"))
    parser.add_argument("--stdin", action="store_true")
    args = parser.parse_args(argv)

    if args.print_rules:
        print(rules_text())
        return 0

    if args.stdin:
        findings, labels = review(sys.stdin.read(), args.kind or "issue")
        blocking = [f for f in findings if f.blocking]
        if blocking:
            print(report(findings, labels, args.kind or "issue"), file=sys.stderr)
            return 1
        extra = f" ({len(labels)} suppressions)" if labels else ""
        print(f"Writing review passed{extra}.")
        return 0

    return run_hook()


if __name__ == "__main__":
    try:
        sys.exit(main())
    except SystemExit:
        raise
    except Exception:  # noqa: BLE001 - fail open, never block the author.
        sys.exit(0)
