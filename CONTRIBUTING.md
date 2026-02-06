# Contributing to LakeSync

Thank you for your interest in contributing to LakeSync! This guide covers the development setup, coding standards, and pull request process.

## Development Setup

### Prerequisites

- [Bun](https://bun.sh/) >= 1.1
- [Docker](https://www.docker.com/) (for integration tests)
- [Node.js](https://nodejs.org/) >= 20 (optional, for compatibility testing)

### Getting Started

```bash
git clone https://github.com/radekdymacz/lakesync.git
cd lakesync
bun install
bun run build
bun run test
```

### Running Infrastructure

```bash
docker compose -f docker/docker-compose.yml up -d
```

This starts MinIO (object store) and Nessie (Iceberg catalogue) for integration tests.

## Code Style

### Language

- **British English** in all comments, documentation, and user-facing strings: serialise, initialise, synchronise, catalogue, behaviour, colour, centre, licence (noun).

### TypeScript

- Strict mode, no `any` type
- Functional style where practical; classes for stateful components
- `Result<T, E>` pattern — never throw from public APIs
- JSDoc on all public APIs

### Formatting

We use [Biome](https://biomejs.dev/) for linting and formatting:

```bash
bun run lint        # check
bun run lint --fix  # auto-fix
```

### Testing

- [Vitest](https://vitest.dev/) for all tests
- Tests are co-located in `__tests__/` directories next to source
- Integration tests requiring Docker use `describe.skipIf` when Docker is unavailable

```bash
bun run test          # all tests
turbo test            # via TurboRepo
```

## Pull Request Process

1. Fork the repository and create a feature branch from `main`
2. Make your changes, ensuring all tests pass
3. Update documentation if your changes affect public APIs
4. Submit a pull request with a clear description of the changes

### PR Checklist

- [ ] Tests added or updated
- [ ] TypeScript strict mode passes (`bun run typecheck`)
- [ ] Linting passes (`bun run lint`)
- [ ] British English used in comments and docs
- [ ] No `any` types introduced
- [ ] Public APIs return `Result<T, E>` (no thrown exceptions)

## Architecture

See [plan.md](plan.md) for the full implementation plan and [adrs/](adrs/) for Architecture Decision Records explaining key design choices.

## Reporting Issues

Use [GitHub Issues](https://github.com/radekdymacz/lakesync/issues) with the provided templates for bug reports and feature requests.
