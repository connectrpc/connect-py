# Contributing to Connect

## Before You Contribute

If you're planning to add or change a public API, please open an issue describing your
proposal before starting work.
This helps ensure alignment with the project's direction and makes the review process
smoother for everyone.

## Developer Certificate of Origin

All commits must be signed off to affirm compliance with the
[Developer Certificate of Origin](https://developercertificate.org/).
Configure your git identity to match your GitHub account, then use the `-s` flag when
committing:

```console
$ git commit -s -m "your commit message"
```

## Setting Up Your Development Environment

### Prerequisites

- [uv](https://docs.astral.sh/uv/) for dependency management

### Installation

1. Fork and clone the repository:

   ```console
   $ gh repo fork connectrpc/connect-py --clone
   $ cd connect-py
   ```

2. Verify everything is working:

   ```console
   $ uv run poe check
   ```

## Development Workflow

We use `poe` as our task runner.
Run `uv run poe` to see all available commands.

## Submitting a Pull Request

1. Create a feature branch from an up-to-date `main`:

   ```console
   $ git checkout -b your-feature-branch
   ```

2. Make your changes and ensure all checks pass:

   ```console
   $ uv run poe check
   ```

3. Commit with a sign-off and a clear message, then push to your fork and open a pull
   request.

Pull requests are more likely to be accepted when they:

- Include tests for new functionality
- Maintain backward compatibility
- Have clear commit messages

We aim to respond to issues and pull requests within a few business days.
