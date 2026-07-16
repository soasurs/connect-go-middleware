<!-- SPDX-License-Identifier: Apache-2.0 -->

# Repository Guidelines

## Project Structure & Module Organization

This repository provides reusable interceptors for `connectrpc.com/connect`. Keep each middleware in a top-level package named for its capability, such as `timeout/` or `breaker/`. Public API, implementation, package documentation, and tests should remain together:

```text
timeout/
  doc.go
  timeout.go
  streaming.go
  timeout_test.go
  example_test.go
```

Use `internal/` only for code shared by multiple middleware packages that must not become public API. GitHub Actions workflows live in `.github/workflows/`. There are currently no generated files or runtime assets.

## Build, Test, and Development Commands

- `gofmt -w .` formats all Go source files.
- `go build ./...` compiles every package.
- `go vet ./...` runs standard static analysis.
- `go test ./...` runs the complete test suite.
- `go test -race ./...` checks concurrent code with the race detector.

Run formatting, build, vet, tests, and race tests before submitting a pull request. The PR workflow repeats build, test, and race checks using the Go version declared in `go.mod`.

## Coding Style & Naming Conventions

Follow standard Go conventions and let `gofmt` control indentation and import grouping. Package names must be short, lowercase, and capability-oriented. Exported identifiers use `PascalCase`; unexported identifiers use `camelCase`. Constructors should follow established Go naming such as `NewInterceptor`.

Keep public APIs small and preserve Connect interceptor semantics. Document every exported symbol, wrap underlying errors where appropriate, and avoid unrelated dependencies. Add `SPDX-License-Identifier: Apache-2.0` to new source and configuration files using the comment syntax appropriate for the file type.

## Testing Guidelines

Use Go's standard `testing` package. Place tests beside their package and name them `TestXxx`; use `ExampleXxx` for executable API examples. Cover unary and streaming behavior, deadline precedence, cleanup paths, Connect error codes, and concurrency where applicable. Prefer deterministic synchronization over timing-sensitive sleeps. No fixed coverage percentage is required, but new behavior must have focused tests.

## Commit & Pull Request Guidelines

Use scoped Conventional Commits, for example `feat(timeout): add stream deadline support` or `ci(actions): add pull request checks`. Keep each commit focused. Pull requests should explain behavior and API changes, note compatibility considerations, link relevant issues, and list commands run. Update documentation and examples whenever public behavior changes; screenshots are unnecessary unless repository presentation files are affected.
