#!/usr/bin/env bash
# =============================================================================
# apply_branch_protection.sh
#
# WHAT IT DOES
#   Codifies portolan-cli's `main` branch protection as GitHub repository
#   rulesets and enables repo-level auto-merge. This is the anchor that makes
#   "green means green" enforceable and Dependabot auto-merge (see
#   .github/workflows/dependabot-automerge.yml) safe.
#
#   Concretely:
#     1. Enables repository auto-merge.
#     2. Deletes the legacy classic branch-protection rule on `main` (if any).
#     3. Creates-or-updates TWO rulesets on `main`:
#        A. "main: PR + green checks" — every push goes through a PR and every
#           required status check must pass. NO bypass actors: this applies to
#           admins too, so nobody merges red.
#        B. "main: review required" — 1 approving review, but repository admins
#           may bypass (so an admin can land a green, bot-approved Dependabot PR,
#           and admins are never hard-blocked on review).
#     4. Prints a verification summary.
#
#   Splitting into two rulesets is deliberate: the strict checks (A) bind
#   everyone, while only the softer review rule (B) grants admin bypass.
#
# REQUIRED STATUS CHECKS
#   Just three contexts, on purpose:
#     - "CI Success"      — the ci.yml aggregation job that gates on quality,
#                           security, the test matrix, iceberg, docs, and build.
#                           Requiring this one context (not each matrix cell)
#                           means adding a Python/OS never drops a required check.
#     - "codecov/patch"   — changed-line coverage (target in codecov.yml).
#     - "codecov/project" — overall coverage floor.
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
BRANCH="main"

# Repository role id 5 == "admin" (GitHub built-in role). Used as a bypass actor
# for the review ruleset only.
ADMIN_ROLE_ID=5

command -v jq >/dev/null || { echo "error: jq is required" >&2; exit 1; }

echo ">> Target repository: ${REPO}"

# -----------------------------------------------------------------------------
# 1. Enable repository auto-merge.
# -----------------------------------------------------------------------------
echo ">> Enabling auto-merge ..."
gh api -X PATCH "repos/${REPO}" -F allow_auto_merge=true >/dev/null
echo "   auto-merge enabled."

# -----------------------------------------------------------------------------
# 2. Delete classic branch protection on main (tolerate 404 = already gone).
# -----------------------------------------------------------------------------
echo ">> Removing classic branch protection on ${BRANCH} (if present) ..."
if gh api "repos/${REPO}/branches/${BRANCH}/protection" >/dev/null 2>&1; then
  gh api -X DELETE "repos/${REPO}/branches/${BRANCH}/protection" >/dev/null
  echo "   classic protection deleted."
else
  echo "   no classic protection found (nothing to delete)."
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
# 3A. Ruleset: PR required + required status checks (no bypass — binds admins).
# -----------------------------------------------------------------------------
CHECKS_RULESET=$(jq -n '
{
  name: "main: PR + green checks",
  target: "branch",
  enforcement: "active",
  conditions: { ref_name: { include: ["refs/heads/main"], exclude: [] } },
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
          { context: "codecov/project" }
        ]
      }
    }
  ]
}')

# -----------------------------------------------------------------------------
# 3B. Ruleset: 1 approving review, bypassable by repository admins.
# -----------------------------------------------------------------------------
REVIEW_RULESET=$(jq -n --argjson admin "${ADMIN_ROLE_ID}" '
{
  name: "main: review required",
  target: "branch",
  enforcement: "active",
  conditions: { ref_name: { include: ["refs/heads/main"], exclude: [] } },
  bypass_actors: [
    { actor_id: $admin, actor_type: "RepositoryRole", bypass_mode: "always" }
  ],
  rules: [
    { type: "pull_request",
      parameters: {
        required_approving_review_count: 1,
        dismiss_stale_reviews_on_push: false,
        require_code_owner_review: false,
        require_last_push_approval: false,
        required_review_thread_resolution: false
      }
    }
  ]
}')

upsert_ruleset "main: PR + green checks" "${CHECKS_RULESET}"
upsert_ruleset "main: review required" "${REVIEW_RULESET}"

# -----------------------------------------------------------------------------
# 4. Verification summary.
# -----------------------------------------------------------------------------
echo
echo ">> Current rulesets on ${REPO}:"
gh api "repos/${REPO}/rulesets?includes_parents=false" \
  --jq '.[] | "   - \(.name) [\(.enforcement)]"'
echo ">> Done. Required checks: CI Success, codecov/patch, codecov/project."
