# Repository stabilization: local execution checklist

This package updates documentation, Git hygiene, and unit-test CI **only**. It does **not** modify `compose.yaml`, the five Python runtime services, Redis data, or the existing Phase 2 smoke/recovery tools.

## 1. Preserve the verified Phase 2.2 implementation first

From PowerShell in `D:\Dev\safestream`:

```powershell
git status
git add PHASE2-2.md docs/ADR-003-recovery-testing.md src/safestream/recovery_probe.py tests/test_recovery_probe.py
git commit -m "test: verify Redis pending-event recovery"
```

Do **not** push yet. Ensure the four new files are committed before applying the documentation overlay.

## 2. Apply the documentation overlay

Extract `safestream-repo-polish-overlay.zip` into `D:\Dev\safestream` (the archive has paths rooted at the repository), overwriting only the packaged files. Do not copy any older `compose.yaml` over your local no-host-port configuration.

## 3. Review changes before committing

```powershell
git status
git diff -- README.md .gitignore .dockerignore docs/ADR-001-architecture.md docs/ADR-002-processing.md docs/ADR-003-recovery-testing.md
git diff --no-index NUL .gitattributes
```

The last command is optional and may return a nonzero status because the file is new. Verify `.github/workflows/tests.yml`, `docs/KNOWN_LIMITATIONS.md`, and this checklist manually in your editor.

### Environment check

```powershell
docker compose -f compose.yaml -f compose.phase2.yaml config --quiet
.\.venv\Scripts\python.exe -m pytest -q
```

Expected test baseline: 38 passed. If you want to repeat the live Phase 2.1 smoke test, run:

```powershell
docker compose -f compose.yaml -f compose.phase2.yaml exec processor python -m safestream.phase2_smoke --timeout 45
```

## 4. Normalize line endings (optional but recommended)

The new `.gitattributes` commits text with LF line endings. Run this only after reviewing pending changes:

```powershell
git add --renormalize .
git status
```

This can stage line-ending-only differences on Windows. Inspect `git diff --cached --stat` and `git diff --cached` before committing.

## 5. Commit the stabilization work locally

```powershell
git add README.md .gitattributes .gitignore .dockerignore .github/workflows/tests.yml docs/KNOWN_LIMITATIONS.md docs/REPOSITORY_REVIEW.md docs/ADR-001-architecture.md docs/ADR-002-processing.md docs/ADR-003-recovery-testing.md
git diff --cached --stat
git commit -m "docs: stabilize SafeStream repository and add unit-test CI"
git status
```

Do not publish to GitHub until checking for secrets, external dependencies, license choice, and CI permissions. The GitHub Actions workflow will run when the repository is pushed; it **does not** validate Docker networking or live recovery.
