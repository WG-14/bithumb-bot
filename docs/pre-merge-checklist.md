# Pre-Merge Validation Checklist

Run these repository-local checks before merging changes that touch config,
operator output, docs, templates, live safety, or runtime contracts.
The `safety-regression` GitHub Actions workflow runs the same targeted gate
commands after `uv sync --dev` and virtualenv activation.

```bash
python3 tools/check_text_hygiene.py
python3 tools/check_env_drift.py
python3 tools/generate_config_docs.py --check
python3 tools/generate_env_example.py --check
python3 -m pytest tests/test_text_hygiene.py tests/test_config_contract.py -q
python3 -m pytest tests/test_live_preflight.py::test_live_execution_contract_emits_safe_env_metadata_and_lints tests/test_live_preflight.py::test_live_execution_contract_log_emits_redacted_fingerprint -q
python3 -m pytest tests/test_operator_commands.py::test_cmd_signal_no_data_output_is_clean_and_actionable tests/test_operator_commands.py::test_cmd_explain_no_data_output_is_clean_and_actionable tests/test_operator_commands.py::test_cmd_status_missing_candle_output_is_clean_and_actionable -q
```

The full-suite gate is:

```bash
uv run pytest -q
```

When working through `scripts/run_codex_pipeline.sh`, do not run the full-suite
command directly. Use the dedicated full-pytest repair pipeline instead:

```bash
./scripts/run_codex_pytest_pipeline.sh
```

Required gate coverage:

- Text hygiene rejects BOM, Hangul, replacement characters, long question runs,
  and known mojibake fragments.
- Env drift rejects undeclared runtime env reads, undeclared `.env.example`
  keys, unverified docs/example drift, unsafe secret examples, unlabeled
  deprecated keys, and missing live-required examples.
- Config reference and `.env.example` stay verified against ConfigSpec.
- Live execution contract metadata includes config, docs, template, effective
  settings, env-file, provenance, approved-profile, managed-root, and runtime
  path fingerprints.
- Operator-facing no-data diagnostics stay English, reason-coded, and
  action-oriented.
