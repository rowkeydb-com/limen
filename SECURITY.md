# Security Policy

## Reporting a vulnerability

If you believe you have found a security vulnerability in Limen,
please **do not** open a public issue or pull request. Instead,
report it privately through GitHub's
[private security advisory mechanism](https://github.com/rowkeydb-com/limen/security/advisories/new).

We will acknowledge receipt within 72 hours, investigate, and work
with you on coordinated disclosure. We aim to release a fix within
30 days for high-severity issues, faster where the issue's
exploitability warrants it.

## Scope

In scope:

- Issues affecting the correctness or safety of admission control —
  for example, a bug that lets requests escape the configured limit.
- Memory-safety issues in the library (despite ASan and UBSan
  coverage in CI).
- Lock-ordering or data-race issues that could cause deadlock or
  data corruption in callers.
- Logic issues in the gRPC adapter that could be triggered by a
  malicious or malformed request.

Out of scope:

- Issues in upstream Netflix `concurrency-limits`, OpenTelemetry,
  Abseil, or gRPC. Report those to the respective projects.
- Operational issues (a server is misconfigured and rejects too
  much or too little traffic). Those belong in
  [issues](https://github.com/rowkeydb-com/limen/issues).
- Issues that require attacker control of the server itself.

## Public advisories

Confirmed and fixed vulnerabilities will be published as GitHub
Security Advisories on this repository, with CVE assignment where
appropriate.
