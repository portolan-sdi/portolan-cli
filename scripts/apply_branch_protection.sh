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
#     3. Creates-or-updates TWO rulesets over PROTECTED_REF_PATTERNS:
#        A. "protected: PR + green checks" — every push goes through a PR and
#           every required status check must pass. NO bypass actors: this applies
#           to admins too, so nobody merges red.
#        B. "protected: review required" — 1 approving review, but repository
#           admins may bypass (so an admin can land a green, bot-approved
#           Dependabot PR, and admins are never hard-blocked on review).
#     4. Deletes the superseded "main: ..." rulesets, so a re-run after the
#        rename does not leave two overlapping copies bound to the same refs.
#     5. Prints a verification summary.
#
#   Splitting into two rulesets is deliberate: the strict checks (A) bind
#   everyone, while only the softer review rule (B) grants admin bypass.
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
#   Just three contexts, on purpose:
#     - "CI Success"      — the ci.yml aggregation job that gates on quality,
#                           security, the test matrix, iceberg, docs, and build.
#                           Requiring this one context (not each matrix cell)
#                           means adding a Python/OS never drops a required check.
#     - "codecov/patch"   — changed-line coverage (target in codecov.yml).
#
#   NOT required: "codecov/project". It is configured in codecov.yml but has
#   never posted a status here — 20 consecutive merged PRs show `codecov/patch`
#   and no `codecov/project`. A required context that never reports blocks every
#   merge, so it stays out until Codecov is fixed to emit it. Re-add it in the
#   same PR that makes it report, never before.
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

# Ref patterns both rulesets bind to (GitHub fnmatch syntax, `*` spans one path
# segment). Read the ORDERING note in the header before adding a pattern.
PROTECTED_REF_PATTERNS=("refs/heads/main" "refs/heads/release/*")

# Ruleset names. Renamed from the earlier "main: ..." pair when the rulesets
# outgrew main; the old names are deleted below so a re-run leaves one copy.
CHECKS_RULESET_NAME="protected: PR + green checks"
REVIEW_RULESET_NAME="protected: review required"
LEGACY_RULESET_NAMES=("main: PR + green checks" "main: review required")

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

# Ref-pattern include list, shared by both rulesets. Built from the bash array
# via jq so a pattern containing a shell metacharacter cannot break the JSON.
REF_INCLUDES=$(printf '%s\n' "${PROTECTED_REF_PATTERNS[@]}" | jq -R . | jq -sc .)

# -----------------------------------------------------------------------------
# 3A. Ruleset: PR required + required status checks (no bypass — binds admins).
# -----------------------------------------------------------------------------
CHECKS_RULESET=$(jq -n --arg name "${CHECKS_RULESET_NAME}" --argjson refs "${REF_INCLUDES}" '
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

# -----------------------------------------------------------------------------
# 3B. Ruleset: 1 approving review, bypassable by repository admins.
# -----------------------------------------------------------------------------
REVIEW_RULESET=$(jq -n --argjson admin "${ADMIN_ROLE_ID}" \
  --arg name "${REVIEW_RULESET_NAME}" --argjson refs "${REF_INCLUDES}" '
{
  name: $name,
  target: "branch",
  enforcement: "active",
  conditions: { ref_name: { include: $refs, exclude: [] } },
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

upsert_ruleset "${CHECKS_RULESET_NAME}" "${CHECKS_RULESET}"
upsert_ruleset "${REVIEW_RULESET_NAME}" "${REVIEW_RULESET}"

# -----------------------------------------------------------------------------
# 4. Retire the pre-rename rulesets. Runs last: if an upsert above failed, the
#    old rules stay in force rather than leaving the branch unprotected.
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
echo ">> Protected refs: ${PROTECTED_REF_PATTERNS[*]}"
echo ">> Done. Required checks: CI Success, codecov/patch."
