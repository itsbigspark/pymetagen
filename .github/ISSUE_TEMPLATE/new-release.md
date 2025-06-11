---
name: New Release Workflow Issue
about: Create a new release
title: '[Release]: [Version Number]'
labels: release  
assignees: ''
---

# New Release Workflow Checklist

## Releasing into the Most Recent Version

### 1. Prepare for Release

- [ ] Ensure `dev/main` is up to date with all changes that are to be released.
- [ ] Create a new branch off of `dev/main` to prepare the release.
- [ ] Update the version number in `pyproject.toml`.

### 2. Update Documentation

- [ ] Ensure the changelog is up to date. Update the heading `## Upcoming Release` to `## pymetagen-<version> - <date>`.
- [ ] Add any additional notes to the changelog as necessary.

### 3. Merge Changes

- [ ] Merge the release preparation branch into `dev/main` via a pull request.

### 4. Deploy the Release

- [ ] Trigger the [release](https://github.com/itsbigspark/pymetagen/actions/workflows/release-action.yml) job via GitHub Actions. This will:
  - Validate the version number in `pyproject.toml` and the changelog.
    - If validation fails (version already released or changelog not updated), return to step 1.
- [ ] Trigger the [PyPi release](https://github.com/itsbigspark/pymetagen/actions/workflows/pypi-release.yml) job via GitHub Actions. 

### 5. Verify the Release

- [ ] Check the `releases` page on GitHub to ensure the release has been created.
- [ ] Verify the release on [PyPi](https://pypi.org/project/pymetagen/) to ensure it is available for download.

### 6. Post-Release

- [ ] Add a new heading to the top of the changelog to gather changes for the next release: `## Upcoming Release`.
