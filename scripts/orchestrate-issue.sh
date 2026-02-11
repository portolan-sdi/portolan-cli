#!/usr/bin/env bash
#
# Orchestrates autonomous issue implementation:
#   1. Create worktree + planning agent (speckit)
#   2. Implementation agent (commit, push, PR)
#   3. Review agent (roborev loop until passing)
#
# Usage: ./scripts/orchestrate-issue.sh <issue-url>
# Example: ./scripts/orchestrate-issue.sh https://github.com/portolan-sdi/portolan-cli/issues/10#issuecomment-3871537791

set -euo pipefail

# ─────────────────────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────────────────────

ISSUE_URL="${1:-}"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORKTREE_BASE="${REPO_ROOT}/../worktrees"
LOG_DIR="${REPO_ROOT}/logs/orchestration"
MAX_REVIEW_LOOPS=5

# Notification helpers
ping_user() { echo -e "\a"; echo "✓ $1"; }
gong_user() { echo -e "\a\a\a"; echo "✗ ERROR: $1"; }

# ─────────────────────────────────────────────────────────────────────────────
# Validation
# ─────────────────────────────────────────────────────────────────────────────

if [[ -z "$ISSUE_URL" ]]; then
    echo "Usage: $0 <github-issue-url>"
    echo "Example: $0 https://github.com/portolan-sdi/portolan-cli/issues/10#issuecomment-3871537791"
    exit 1
fi

# Extract issue number from URL
if [[ "$ISSUE_URL" =~ issues/([0-9]+) ]]; then
    ISSUE_NUM="${BASH_REMATCH[1]}"
else
    gong_user "Could not parse issue number from URL: $ISSUE_URL"
    exit 1
fi

# Extract repo from URL
if [[ "$ISSUE_URL" =~ github\.com/([^/]+/[^/]+)/issues ]]; then
    REPO="${BASH_REMATCH[1]}"
else
    gong_user "Could not parse repo from URL: $ISSUE_URL"
    exit 1
fi

echo "───────────────────────────────────────────────────────────────────────"
echo "Orchestrating issue #${ISSUE_NUM} from ${REPO}"
echo "───────────────────────────────────────────────────────────────────────"

# ─────────────────────────────────────────────────────────────────────────────
# Setup
# ─────────────────────────────────────────────────────────────────────────────

# Fetch issue title for branch name
ISSUE_TITLE=$(gh issue view "$ISSUE_NUM" --repo "$REPO" --json title --jq '.title')
# Convert to branch name: lowercase, replace spaces/special chars with hyphens
BRANCH_NAME="feature/$(echo "$ISSUE_TITLE" | tr '[:upper:]' '[:lower:]' | sed 's/[^a-z0-9]/-/g' | sed 's/--*/-/g' | sed 's/^-//' | sed 's/-$//' | cut -c1-50)"

WORKTREE_PATH="${WORKTREE_BASE}/${BRANCH_NAME}"
TIMESTAMP=$(date +%Y%m%d-%H%M%S)
mkdir -p "$LOG_DIR"

echo "Branch: $BRANCH_NAME"
echo "Worktree: $WORKTREE_PATH"
echo "Logs: $LOG_DIR"
echo ""

# ─────────────────────────────────────────────────────────────────────────────
# Phase 1: Planning Agent
# ─────────────────────────────────────────────────────────────────────────────

echo "╔═══════════════════════════════════════════════════════════════════════╗"
echo "║ PHASE 1: Planning                                                      ║"
echo "╚═══════════════════════════════════════════════════════════════════════╝"

# Create worktree
if [[ -d "$WORKTREE_PATH" ]]; then
    echo "Worktree already exists, reusing: $WORKTREE_PATH"
else
    echo "Creating worktree..."
    git worktree add -b "$BRANCH_NAME" "$WORKTREE_PATH" main 2>/dev/null || \
    git worktree add "$WORKTREE_PATH" "$BRANCH_NAME" 2>/dev/null || \
    { gong_user "Failed to create worktree"; exit 1; }
fi

# Fetch issue body and comment for context
ISSUE_BODY=$(gh issue view "$ISSUE_NUM" --repo "$REPO" --json body --jq '.body')
COMMENT_BODY=""
if [[ "$ISSUE_URL" =~ issuecomment-([0-9]+) ]]; then
    COMMENT_ID="${BASH_REMATCH[1]}"
    COMMENT_BODY=$(gh api "repos/${REPO}/issues/${ISSUE_NUM}/comments" --jq ".[] | select(.id == ${COMMENT_ID}) | .body" 2>/dev/null || echo "")
fi

PLAN_LOG="${LOG_DIR}/${TIMESTAMP}-phase1-plan.log"

# Build the planning prompt
PLAN_PROMPT="You are a planning agent. Your job is to create a speckit plan for implementing this GitHub issue.

## Issue #${ISSUE_NUM}: ${ISSUE_TITLE}

### Issue Body:
${ISSUE_BODY}

### Design Comment (if present):
${COMMENT_BODY}

## Your Task:
1. Run /speckit.specify to create the feature specification based on the issue
2. Run /speckit.clarify if there are ambiguities (auto-answer based on the design comment above)
3. Run /speckit.plan to generate the implementation plan
4. Run /speckit.tasks to generate the task list

When all speckit artifacts are complete (.specify/features/ should have spec.md, plan.md, tasks.md),
commit them with message 'docs: add speckit plan for issue #${ISSUE_NUM}'

Signal completion by creating a file: .planning-complete

DO NOT implement any code. Only create the plan."

echo "Launching planning agent..."
echo "Logging to: $PLAN_LOG"

# Run claude in the worktree
cd "$WORKTREE_PATH"
claude --print "$PLAN_PROMPT" 2>&1 | tee "$PLAN_LOG"

# Check for completion
if [[ -f "$WORKTREE_PATH/.planning-complete" ]]; then
    echo "✓ Planning phase complete"
    rm -f "$WORKTREE_PATH/.planning-complete"
else
    # Check if speckit files exist as alternative completion signal
    if [[ -d "$WORKTREE_PATH/.specify/features" ]] && \
       find "$WORKTREE_PATH/.specify/features" -name "plan.md" -o -name "tasks.md" 2>/dev/null | grep -q .; then
        echo "✓ Planning phase complete (speckit artifacts found)"
    else
        gong_user "Planning phase did not complete successfully. Check log: $PLAN_LOG"
        exit 1
    fi
fi

# ─────────────────────────────────────────────────────────────────────────────
# Phase 2: Implementation Agent
# ─────────────────────────────────────────────────────────────────────────────

echo ""
echo "╔═══════════════════════════════════════════════════════════════════════╗"
echo "║ PHASE 2: Implementation                                                ║"
echo "╚═══════════════════════════════════════════════════════════════════════╝"

IMPL_LOG="${LOG_DIR}/${TIMESTAMP}-phase2-implement.log"

IMPL_PROMPT="You are an implementation agent. Your job is to implement the plan created by the planning agent.

## Context
- Issue #${ISSUE_NUM}: ${ISSUE_TITLE}
- Branch: ${BRANCH_NAME}
- The speckit plan is in .specify/features/

## Your Task:
1. Read the plan and tasks from .specify/features/
2. Run /speckit.implement to execute all tasks
3. Follow TDD: write tests first, then implementation
4. After implementation is complete:
   - Run all tests: uv run pytest
   - Run linting: uv run ruff check . && uv run ruff format .
   - Run type checking: uv run mypy portolan_cli
5. Commit all changes with conventional commit messages
6. Push the branch: git push -u origin ${BRANCH_NAME}
7. Open a PR: gh pr create --title 'feat: implement issue #${ISSUE_NUM}' --body 'Implements #${ISSUE_NUM}'

When the PR is created, output the PR URL and create a file: .implementation-complete with the PR URL inside.

DO NOT skip tests. TDD is mandatory per CLAUDE.md."

echo "Launching implementation agent..."
echo "Logging to: $IMPL_LOG"

cd "$WORKTREE_PATH"
claude --print "$IMPL_PROMPT" 2>&1 | tee "$IMPL_LOG"

# Extract PR URL
PR_URL=""
if [[ -f "$WORKTREE_PATH/.implementation-complete" ]]; then
    PR_URL=$(cat "$WORKTREE_PATH/.implementation-complete")
    rm -f "$WORKTREE_PATH/.implementation-complete"
fi

# Fallback: try to get PR URL from git
if [[ -z "$PR_URL" ]]; then
    PR_URL=$(gh pr view --json url --jq '.url' 2>/dev/null || echo "")
fi

if [[ -z "$PR_URL" ]]; then
    gong_user "Implementation phase did not create a PR. Check log: $IMPL_LOG"
    exit 1
fi

echo "✓ Implementation phase complete"
echo "PR URL: $PR_URL"

# ─────────────────────────────────────────────────────────────────────────────
# Phase 3: Review Loop
# ─────────────────────────────────────────────────────────────────────────────

echo ""
echo "╔═══════════════════════════════════════════════════════════════════════╗"
echo "║ PHASE 3: Review Loop                                                   ║"
echo "╚═══════════════════════════════════════════════════════════════════════╝"

# Extract PR number from URL
if [[ "$PR_URL" =~ /pull/([0-9]+) ]]; then
    PR_NUM="${BASH_REMATCH[1]}"
else
    gong_user "Could not parse PR number from URL: $PR_URL"
    exit 1
fi

review_loop=0
review_passed=false

while [[ $review_loop -lt $MAX_REVIEW_LOOPS ]]; do
    review_loop=$((review_loop + 1))
    REVIEW_LOG="${LOG_DIR}/${TIMESTAMP}-phase3-review-${review_loop}.log"

    echo ""
    echo "─── Review iteration ${review_loop}/${MAX_REVIEW_LOOPS} ───"
    echo "Logging to: $REVIEW_LOG"

    # Run roborev
    echo "Running roborev..."
    if roborev "$PR_URL" 2>&1 | tee "${REVIEW_LOG}.roborev"; then
        ROBOREV_OUTPUT=$(cat "${REVIEW_LOG}.roborev")
    else
        ROBOREV_OUTPUT=$(cat "${REVIEW_LOG}.roborev")
    fi

    # Check if review passed (no findings)
    if echo "$ROBOREV_OUTPUT" | grep -qi "no issues\|passed\|approved\|lgtm\|no findings"; then
        echo "✓ Review passed!"
        review_passed=true
        break
    fi

    # If there are findings, launch agent to fix them
    echo "Review found issues. Launching fix agent..."

    FIX_PROMPT="You are a review-fix agent. Roborev found issues in PR #${PR_NUM}.

## Roborev Output:
${ROBOREV_OUTPUT}

## Your Task:
1. Read and understand each finding
2. Fix each issue in the code
3. Run tests to ensure fixes don't break anything: uv run pytest
4. Commit fixes with message: 'fix: address review feedback (iteration ${review_loop})'
5. Push changes: git push

After pushing, create file: .fixes-complete"

    cd "$WORKTREE_PATH"
    claude --print "$FIX_PROMPT" 2>&1 | tee "$REVIEW_LOG"

    if [[ -f "$WORKTREE_PATH/.fixes-complete" ]]; then
        rm -f "$WORKTREE_PATH/.fixes-complete"
        echo "✓ Fixes applied, re-running review..."
    else
        echo "⚠ Fix agent may not have completed successfully"
    fi
done

# ─────────────────────────────────────────────────────────────────────────────
# Completion
# ─────────────────────────────────────────────────────────────────────────────

echo ""
echo "═══════════════════════════════════════════════════════════════════════"

if [[ "$review_passed" == "true" ]]; then
    ping_user "PR is ready for merge: $PR_URL"
    echo ""
    echo "Summary:"
    echo "  - Issue: #${ISSUE_NUM}"
    echo "  - Branch: ${BRANCH_NAME}"
    echo "  - PR: ${PR_URL}"
    echo "  - Review iterations: ${review_loop}"
    echo "  - Logs: ${LOG_DIR}/${TIMESTAMP}-*"
    echo ""
    echo "Next steps:"
    echo "  1. Review the PR: $PR_URL"
    echo "  2. Merge when satisfied"
    echo "  3. Clean up worktree: git worktree remove $WORKTREE_PATH"
else
    gong_user "Review loop exhausted after ${MAX_REVIEW_LOOPS} iterations. Manual intervention required."
    echo ""
    echo "PR: $PR_URL"
    echo "Worktree: $WORKTREE_PATH"
    echo "Logs: ${LOG_DIR}/${TIMESTAMP}-*"
    exit 1
fi
