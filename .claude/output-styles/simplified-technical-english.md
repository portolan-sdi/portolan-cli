---
name: Simplified Technical English
description: ASD-STE100 controlled English. Short sentences, one idea each, active voice, approved vocabulary.
---

Write all issue bodies, pull request bodies, commit message bodies, and lasting
code comments in Simplified Technical English (ASD-STE100). Apply these rules to
every sentence.

norms/prose.md governs public-facing copy such as the website, announcements, and
docs. These rules govern development writing. Follow these for issues and pull
requests.

## Sentence rules

- Write short sentences. 20 words maximum for descriptive text. 20 words
  maximum for a procedural step.
- Put one idea in one sentence. Do not join two ideas with a comma or a dash.
- Write in the active voice. Name the agent that does the action.
- Write in the present tense when the fact is always true.
- Use only simple verb forms. Do not use a gerund, a present participle, or a
  perfect tense. Write "the check reports the error", not "the check is
  reporting the error" or "the check has reported the error".
- Start an instruction with the verb. Example: "Run the tests."
- Keep the subject, the verb, and the object close together.
- Write no more than 6 sentences in one paragraph.
- Do not use an em dash, a semicolon, or a mid-sentence colon. Write two
  sentences instead. A colon before a list is correct.

## Word rules

- Use one word for one meaning. Do not change the word for variety.
- Use the simple word. Write "use", not "utilize". Write "start", not
  "initiate". Write "before", not "prior to". Write "about", not "regarding".
- Use a verb, not a noun made from a verb. Write "the check verifies the
  body", not "verification of the body occurs".
- Do not use noun clusters of more than 3 nouns.
- Do not drop articles. Write "the file", not "file".
- Do not use idioms, metaphors, or figurative language.
- Do not use filler words: just, really, actually, simply, basically, of
  course.
- Do not use hype words: powerful, seamless, robust, blazing-fast. State the
  measured behavior instead.

## Structure

- State the outcome first. Give the detail after it.
- Use a bulleted list for parallel items.
- Use a numbered list for a sequence of steps.
- Use a table when you compare items on the same attributes.
- End on the last technical point. Do not add a summary or a closing line.
- Do not write a sentence that argues for the work the document describes.

## What does not change

- Code, commands, file paths, identifiers, and error text stay exact. Do not
  rewrite them.
- Quoted output stays verbatim.
- Technical terms stay exact. Do not replace a term with a simpler word if the
  meaning changes.
- Length is not a fault. A long body is correct when it leads with the
  outcome. Do not compress meaning to make a document short.
- Safety warnings, destructive-action confirmations, and security notes are
  written in full clear sentences. Clarity has priority over brevity here.
