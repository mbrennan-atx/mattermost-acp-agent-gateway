# Preparing a public candidate

The original repository and history may contain private deployment material.
Keep them private. Export only committed files listed in
`release/public-files.txt`, into a new directory and optionally fresh Git history.
The export refuses an existing destination and never adds a remote or publishes.
Commit the reviewed manifest and source changes before exporting.

```bash
python3 scripts/export-public.py /absolute/path/to/new-candidate --init-git \
  --policy /absolute/private/path/identifier-policy.json > /absolute/private/path/export-evidence.json
```

The optional private policy is a JSON array of prohibited literal strings. Keep
original identifiers and scan evidence outside the public checkout. The public
checks reject common credential shapes and runtime file classes. They do not
prove that arbitrary secrets or every identifying phrase are absent. Inspect the
exact candidate, its only history, license, documentation and built archives.

Run tests, lint and build in the exported checkout. Scan the wheel and source
archive, then install the wheel in a clean environment outside the repository.
Confirm that examples and documented tools work without private source files.
Use a private CI destination until publication is explicitly authorized.

```bash
python3 scripts/release_check.py dist/package.whl --policy /absolute/private/path/identifier-policy.json
python3 scripts/release_check.py dist/source.tar.gz --policy /absolute/private/path/identifier-policy.json
```

Replace those illustrative artifact names with the actual build outputs. The
wheel contains runtime code and required license/metadata. The source archive
uses explicit build includes. Fixtures and examples are synthetic. The gateway
uses installed dependencies rather than vendoring provider clients; preserve the
project LICENSE and dependency license metadata when redistributing them.
