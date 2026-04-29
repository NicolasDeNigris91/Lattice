# Security policy

## Supported versions

Lattice is pre-release. Until v1.0, security fixes land on `main` only.

## Reporting a vulnerability

If you find a security issue, please open a private security advisory on
GitHub at https://github.com/NicolasDeNigris91/Lattice/security/advisories
or email nicolas.denigris91@icloud.com with the subject prefixed
`[lattice security]`.

Please do not file a public issue for vulnerabilities. I will acknowledge
within seven days and aim to ship a fix or mitigation within thirty days.

## Threat model

Lattice is an embedded library. It expects a trusted local process and a
trusted local filesystem. Network exposure, multi-tenant isolation, and
resistance to malicious input crafted by remote attackers are out of scope
for v1.0.
