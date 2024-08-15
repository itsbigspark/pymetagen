# CHANGELOG

## Upcoming Release

General changes:

- Adds a changelog to the repository to track changes for each release.
- Updates the issue and pull request templates to include a changelog checklist.
- Adds a new release workflow issue template to guide the release process.
- Adds sonarcloud configuration to the repository.
- Adds mypy checking to the repository.

Development changes:

- Adds new method in `MetaGen` class to write extracts to a file.
- Uses `Enum` class to define the metadata colum names.
- Now we can view the metadata table without the need to parse an output path to write or `-P` flag.
- Adds a polars version compatibility function to get the schema.
- Removes tracking `poetry.lock` file from the repository.
