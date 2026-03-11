## Summary

<!-- What does this PR do? 2-3 sentences. -->

## Type of Change

- [ ] Bug fix (non-breaking change that fixes an issue)
- [ ] New feature (non-breaking change that adds functionality)
- [ ] Breaking change (fix or feature that would cause existing functionality to change)
- [ ] Documentation update
- [ ] Refactoring (no functional changes)
- [ ] Test coverage improvement

## LLM Self-Review

<!-- If an LLM assisted with this PR, have it review the diff and confirm each item. -->
<!-- If no LLM was used, check the items manually or write "N/A - no LLM used." -->

### Code Quality
- [ ] All public functions have docstrings
- [ ] Logging used instead of `print()` for operational messages
- [ ] Errors caught with specific exceptions and clear messages
- [ ] File paths use `pathlib.Path`
- [ ] Type hints on function signatures

### CLI Specifics
- [ ] CLI arguments follow existing Typer patterns in `cli.py`
- [ ] Output targets GeoParquet as primary format
- [ ] Cloud-native patterns preserved

### HEC-HMS Specifics
- [ ] HMS data extraction uses hms-commander API
- [ ] No raw HMS file parsing that bypasses hms-commander

## Test Plan

<!-- How did you test this? Check all that apply. -->

- [ ] `pytest tests/` passes
- [ ] Tested with a real HEC-HMS project
- [ ] Tested CLI commands manually
- [ ] Added new test cases
- [ ] Docs build cleanly (`mkdocs build`)

## LLM Attribution

<!-- If an LLM helped write this code, note which one. This is encouraged, not penalized. -->
<!-- Example: "Claude via Claude Code" or "GPT-4o via Cursor" or "N/A" -->

**LLM used:**

## Additional Notes

<!-- Anything else reviewers should know? -->
