# Contributing

When contributing to this repository, please first discuss the change you wish to make via issue with the repository owners before making a change.

## Local development

As a starting point, you need to set up the environment for the development process.

### Requirements

- [Python](https://www.python.org/) >= 3.8 ([pyenv](https://github.com/pyenv/pyenv) is a recommended way to manage Python versions).
- [Make](https://www.gnu.org/software/make/) - GNU Make tool.

Once you have the necessary tools, you are ready to set up the project using the following [Makefile](../Makefile) target:

```bash
make clean install
```

It creates virtual environment and install all dependencies.  
From now, you are all set for coding!

Other helpful [Makefile](../Makefile) targets:

```bash
make check  # Run formatters and linters
make unit   # Run unit tests
```

## Integration tests setup

We have integration tests against real Kusto instance. To set up such tests you need to create `.env` file and provide your Kusto instance credentials. See [.env.smaple](../.env.sample) for more details.

To run integration tests use one of the following commands:

```bash
make integration  # Run integration tests
make test         # Run both unit and integration tests
```

You are __not required__ to set up and run integration tests for contributing.

## Use a Consistent Coding Style

This project uses [EditorConfig](https://editorconfig.org/). All style settings you may find in the [.editorconfig](../.editorconfig) file.
Additionally, we use [Black](https://github.com/psf/black) as a default formatting tool.

Please, use consistent code style in your contributions.

## Pull Request Process

First, your PRs are very welcome! Use the following guidelines to make your PRs even better:

1. Please fork this repository, and create a new branch for your feature/bugfix.
2. After finishing the development, run the following commands:
    - `make check` to make sure that code is properly formatted, and comply with [PEP 8](https://www.python.org/dev/peps/pep-0008/) style guide.
    - `make test` to run the test suite.
3. When opening a PR, it's mandatory to reference an issue (or set of issues) it resolves. PRs without linked issues won't be reviewed.
4. Please describe the purpose of the PR. What problem are you trying to solve, and what is the solution?
5. Please add tests to your PR.
6. Please make sure that all PR automatic checks are passed successfully.

Please do not hesitate to let us know if you have any issues during development process. Happy coding!

## Report bugs and feature suggestions using GitHub's issues

We use GitHub issues to track public bugs and feature suggestions. Report a bug or suggest a feature by opening a new issue.

## License

By contributing, you agree that your contributions will be licensed under its [Apache License 2.0](../LICENSE).

## Code of Conduct

This project has adopted the [Code of Conduct](CODE_OF_CONDUCT.md).
