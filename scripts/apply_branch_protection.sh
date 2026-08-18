#!/usr/bin/env bash
# =============================================================================
# apply_branch_protection.sh
#
# WHAT IT DOES
#   Codifies portolan-cli's branch protection as GitHub repository rulesets and
#   enables repo-level auto-merge. This is the anchor that makes "green means
#   green" enforceable and Dependabot auto-merge (see
#   .github/workflows/dependabot-automerge.yml) safe.
#
#   Concretely:
#     1. Enables repository auto-merge.
#     2. Deletes any legacy classic branch-protection rule on a protected branch.
#     3. Creates-or-updates TWO rulesets:
#        A. "protected: PR + green checks" over refs/heads/main — every push
#           goes through a PR, and every required status check must pass.
#        B. "protected: release branches" over refs/heads/release/* — the same
#           rules, minus the two org checks.
#     4. Deletes the superseded rulesets, including the review rule, so a
#        re-run does not leave overlapping copies bound to the same refs.
#     5. Prints a verification summary.
#
#   The split is deliberate. A release branch cannot require "checks / layout"
#   until it carries .claude/hooks/writing_check.py, so that check would block
#   every merge into it. See portolan-sdi/portolan-cli#773.
#
# NO REVIEW REQUIREMENT
#   This script used to create a "protected: review required" ruleset asking
#   for 1 approving review. It no longer does, and it deletes that ruleset.
#   GitHub auto-merge ignores the admin bypass, so every ops-sync PR sat
#   unmerged until a person approved it, and a solo author could not approve
#   their own work. The checks are the gate. See portolan-sdi/portolan-ops,
#   norms/ci.md, "Branch Protection".
#
# THE RECORD LIVES IN portolan-ops
#   sync/protection.yml in portolan-sdi/portolan-ops records the contexts and
#   the review count this repo should hold, and protection-audit.yml compares
#   the live setting against it every Monday. Change that file in the same
#   pull request that changes this script, or the audit reports the difference.
#
# WHICH BRANCHES
#   `main` plus `release/*`. Release branches are long-lived, take PRs directly,
#   and merge back into main, so work lands on them that main never sees before
#   release. Protecting only main left that work ungated.
#
#   ORDERING: a required check that has never reported blocks every PR on the
#   branch it guards. Confirm "CI Success" reports on a PR targeting the branch
#   pattern BEFORE adding that pattern here.
#
# REQUIRED STATUS CHECKS
#   On main, four contexts:
#     - "CI Success"           — the ci.yml aggregation job that gates on
#                                quality, security, the test matrix, iceberg,
#                                docs, and build. Requiring this one context
#                                (not each matrix cell) means adding a Python
#                                version or an OS never drops a required check.
#     - "codecov/patch"        — changed-line coverage (target in codecov.yml).
#     - "checks / layout"      — the org layout check, from repo-checks.yml.
#     - "checks / pull-request" — the org body check, from repo-checks.yml.
#
#   On release/*, the first two only.
#
#   "codecov/project" is absent on purpose. It reported nothing, so requiring
#   it blocked every merge. See #733.
#
#   A required context that never reports blocks every merge on the branch it
#   guards. Confirm a context appears on a PR before adding it here.
#
# REQUIREMENTS
#   - `gh` authenticated as a user with ADMIN on the repo.
#   - `jq` on PATH.
#
# SAFE TO RE-RUN
#   Idempotent. Rulesets are matched by name (GET), updated (PUT) if present, or
#   created (POST) if not. Enabling auto-merge and deleting classic protection
#   both no-op on a second run.
#
# RELATED MANUAL STEP (not done here)
#   The Dependabot auto-merge workflow approves + queues bot PRs using the
#   built-in GITHUB_TOKEN, which is sufficient. If you later want security
#   auto-fix PRs to trigger CI (GITHUB_TOKEN-created PRs do not), create a
#   fine-grained PAT (contents + pull-requests: write) and store it as the
#   BOT_PR_TOKEN repository secret:
#       gh secret set BOT_PR_TOKEN --repo <owner>/<repo>
# =============================================================================
set -euo pipefail

REPO="${1:-portolan-sdi/portolan-cli}"

# Ref patterns, one set per ruleset (GitHub fnmatch syntax, `*` spans one path
# segment). Read the ORDERING note in the header before adding a pattern.
MAIN_REF_PATTERNS=("refs/heads/main")
RELEASE_REF_PATTERNS=("refs/heads/release/*")

# Ruleset names. The names below match what the repo holds today. Anything in
# LEGACY_RULESET_NAMES is deleted, so a re-run leaves one copy of each and
# removes the review rule this script used to create.
CHECKS_RULESET_NAME="protected: PR + green checks"
RELEASE_RULESET_NAME="protected: release branches"
LEGACY_RULESET_NAMES=(
  "main: PR + green checks"
  "main: review required"
  "protected: review required"
)

command -v jq >/dev/null || { echo "error: jq is required" >&2; exit 1; }

echo ">> Target repository: ${REPO}"

# -----------------------------------------------------------------------------
# 1. Enable repository auto-merge.
# -----------------------------------------------------------------------------
echo ">> Enabling auto-merge ..."
gh api -X PATCH "repos/${REPO}" -F allow_auto_merge=true >/dev/null
echo "   auto-merge enabled."

# -----------------------------------------------------------------------------
# 2. Delete classic branch protection on protected branches (404 = already gone).
#    Rulesets and classic protection stack rather than replace, so a leftover
#    classic rule would silently keep enforcing its own stale check list.
# -----------------------------------------------------------------------------
echo ">> Removing classic branch protection (if present) ..."
# Assigned on its own line: `local`/inline assignment would mask gh's exit
# status, and an API failure here must abort rather than read as "no branches".
PROTECTED_BRANCHES=$(gh api --paginate "repos/${REPO}/branches?protected=true" --jq '.[].name')
CLASSIC_FOUND=0
while IFS= read -r branch; do
  [ -n "${branch}" ] || continue
  case "${branch}" in
    main|release/*) ;;
    *) continue ;;
  esac
  # `protected=true` also reports ruleset-covered branches, so the GET may 404.
  # That is the "nothing to do" case, not an error.
  if gh api "repos/${REPO}/branches/${branch}/protection" >/dev/null 2>&1; then
    gh api -X DELETE "repos/${REPO}/branches/${branch}/protection" >/dev/null
    echo "   classic protection deleted on ${branch}."
    CLASSIC_FOUND=1
  fi
done <<<"${PROTECTED_BRANCHES}"
if [ "${CLASSIC_FOUND}" -eq 0 ]; then
  echo "   none found (nothing to delete)."
fi

# -----------------------------------------------------------------------------
# Helper: create-or-update a ruleset by name.
#   $1 = ruleset name   $2 = full ruleset JSON payload (its "name" must match $1)
# -----------------------------------------------------------------------------
upsert_ruleset() {
  local name="$1" payload="$2" existing_id
  existing_id=$(
    gh api "repos/${REPO}/rulesets?includes_parents=false" \
      --jq ".[] | select(.name == \"${name}\") | .id" 2>/dev/null | head -1
  )
  if [ -n "${existing_id}" ]; then
    echo ">> Updating ruleset '${name}' (id ${existing_id}) ..."
    gh api -X PUT "repos/${REPO}/rulesets/${existing_id}" \
      --input - <<<"${payload}" >/dev/null
  else
    echo ">> Creating ruleset '${name}' ..."
    gh api -X POST "repos/${REPO}/rulesets" \
      --input - <<<"${payload}" >/dev/null
  fi
  echo "   '${name}' applied."
}

# -----------------------------------------------------------------------------
# Helper: delete a ruleset by name if it exists (used to retire the old names).
# -----------------------------------------------------------------------------
delete_ruleset() {
  local name="$1" existing_id
  existing_id=$(
    gh api "repos/${REPO}/rulesets?includes_parents=false" \
      --jq ".[] | select(.name == \"${name}\") | .id" 2>/dev/null | head -1
  )
  if [ -n "${existing_id}" ]; then
    gh api -X DELETE "repos/${REPO}/rulesets/${existing_id}" >/dev/null
    echo "   retired superseded ruleset '${name}'."
  fi
}

# Ref-pattern include lists, one per ruleset. Built from the bash arrays via
# jq so a pattern containing a shell metacharacter cannot break the JSON.
MAIN_REF_INCLUDES=$(printf '%s\n' "${MAIN_REF_PATTERNS[@]}" | jq -R . | jq -sc .)
RELEASE_REF_INCLUDES=$(printf '%s\n' "${RELEASE_REF_PATTERNS[@]}" | jq -R . | jq -sc .)

# -----------------------------------------------------------------------------
# 3A. main: PR required + the four required status checks.
# -----------------------------------------------------------------------------
CHECKS_RULESET=$(jq -n --arg name "${CHECKS_RULESET_NAME}" \
  --argjson refs "${MAIN_REF_INCLUDES}" '
{
  name: $name,
  target: "branch",
  enforcement: "active",
  conditions: { ref_name: { include: $refs, exclude: [] } },
  bypass_actors: [],
  rules: [
    { type: "deletion" },
    { type: "non_fast_forward" },
    { type: "pull_request",
      parameters: {
        required_approving_review_count: 0,
        dismiss_stale_reviews_on_push: false,
        require_code_owner_review: false,
        require_last_push_approval: false,
        required_review_thread_resolution: false
      }
    },
    { type: "required_status_checks",
      parameters: {
        strict_required_status_checks_policy: true,
        do_not_enforce_on_create: false,
        required_status_checks: [
          { context: "CI Success" },
          { context: "codecov/patch" },
          { context: "checks / layout" },
          { context: "checks / pull-request" }
        ]
      }
    }
  ]
}')

# -----------------------------------------------------------------------------
# 3B. release/*: the same rules, minus the two org checks. checks / layout
#     fails on that branch until it carries the writing hook (#773), so
#     requiring it would block every merge into the release branch.
# -----------------------------------------------------------------------------
RELEASE_RULESET=$(jq -n --arg name "${RELEASE_RULESET_NAME}" \
  --argjson refs "${RELEASE_REF_INCLUDES}" '
{
  name: $name,
  target: "branch",
  enforcement: "active",
  conditions: { ref_name: { include: $refs, exclude: [] } },
  bypass_actors: [],
  rules: [
    { type: "deletion" },
    { type: "non_fast_forward" },
    { type: "pull_request",
      parameters: {
        required_approving_review_count: 0,
        dismiss_stale_reviews_on_push: false,
        require_code_owner_review: false,
        require_last_push_approval: false,
        required_review_thread_resolution: false
      }
    },
    { type: "required_status_checks",
      parameters: {
        strict_required_status_checks_policy: true,
        do_not_enforce_on_create: false,
        required_status_checks: [
          { context: "CI Success" },
          { context: "codecov/patch" }
        ]
      }
    }
  ]
}')

upsert_ruleset "${CHECKS_RULESET_NAME}" "${CHECKS_RULESET}"
upsert_ruleset "${RELEASE_RULESET_NAME}" "${RELEASE_RULESET}"

# -----------------------------------------------------------------------------
# 4. Retire the superseded rulesets, the review rule among them. Runs last:
#    if an upsert above failed, the old rules stay in force rather than
#    leaving the branch unprotected.
# -----------------------------------------------------------------------------
for legacy in "${LEGACY_RULESET_NAMES[@]}"; do
  delete_ruleset "${legacy}"
done

# -----------------------------------------------------------------------------
# 5. Verification summary.
# -----------------------------------------------------------------------------
echo
echo ">> Current rulesets on ${REPO}:"
gh api "repos/${REPO}/rulesets?includes_parents=false" \
  --jq '.[] | "   - \(.name) [\(.enforcement)]"'
echo ">> Protected refs: ${MAIN_REF_PATTERNS[*]} ${RELEASE_REF_PATTERNS[*]}"
echo ">> main requires: CI Success, codecov/patch, checks / layout," \
     "checks / pull-request."
echo ">> release/* requires: CI Success, codecov/patch."
echo ">> No branch requires an approving review."
