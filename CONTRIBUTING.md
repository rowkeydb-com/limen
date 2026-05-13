# Contributing to Limen

Thank you for considering a contribution. This guide covers the
practical mechanics of getting a change reviewed and merged.

## Code of Conduct

By participating, you agree to abide by our
[Code of Conduct](CODE_OF_CONDUCT.md).

## Pull-request process

1. Fork the repository and create a topic branch from `main`.
2. Make your change. Code and tests should ship in the same commit.
   The library follows the architecture described in
   [`design/architecture.md`](design/architecture.md); if your
   change touches the architecture, update the design document in
   the same pull request.
3. Run the full CI matrix locally before pushing:
   ```
   ./.github/scripts/run-bazel-in-docker.sh release test
   ./.github/scripts/run-bazel-in-docker.sh asan-ubsan test
   ./.github/scripts/run-bazel-in-docker.sh tsan test
   ./.github/scripts/run-bazel-in-docker.sh clang-tidy build
   ```
   These are the same commands the GitHub Actions workflows run.
4. Run `clang-format -i` on every touched C++ file. The
   static-analysis workflow rejects pull requests with format
   violations.
5. Open a pull request against `main`. Reference any related issue
   in the description.
6. CI runs automatically. Address every failure before requesting
   review.

## Commit messages

Plain English. Title-only is fine for trivial fixes. For
substantive changes, use a short body with blank-line-separated
stanzas — one stanza per logical thought.

No synthetic prefix codes (no `F-1`, `C-8`, `M-3`, etc.). No
AI/Claude attribution. No `Co-Authored-By: Claude` lines.

## Code style

We follow the
[Google C++ Style Guide](https://google.github.io/styleguide/cppguide.html)
with two project-specific orientation choices:

- Pointer alignment: `int& foo`, not `int &foo`.
- East const: `int const&`, not `const int&`.

Both are encoded in the project's `.clang-format`. Running
`clang-format -i` on your files is sufficient.

No exceptions (`-fno-exceptions`). No RTTI (`-fno-rtti`). No raw
owning pointers — use `std::unique_ptr` or `std::shared_ptr` for
ownership; raw pointers and references are fine for non-owning
access.

`absl::Mutex` for all synchronisation primitives, with Clang
thread-safety annotations on every shared-memory member and method.
The `-Werror=thread-safety` build flag turns annotation mismatches
into compile errors.

## Sign-off

Add `Signed-off-by: Your Name <you@example.com>` to each commit
(equivalent to `git commit -s`). This certifies that you have the
right to contribute the change under the project's Apache 2.0
license.

## Reporting issues

Bug reports and feature requests are welcome via GitHub Issues.
For security-sensitive reports, follow the process described in
[SECURITY.md](SECURITY.md) instead.
