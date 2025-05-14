#!/bin/bash
# version.sh - Version management helper

echo "PyIceberg BigQuery Catalog - Version Management"
echo "============================================="

# Show current version
current_version=$(poetry version -s)
echo "Current version: $current_version"

# Show next version based on commits
echo -e "\nNext version will be determined by conventional commits:"
echo "  feat: minor version bump (0.x.0)"
echo "  fix: patch version bump (0.0.x)"
echo "  BREAKING CHANGE: major version bump (x.0.0)"

# Show recent commits
echo -e "\nRecent commits:"
git log --oneline -5

# Check if there are unreleased changes
echo -e "\nUnreleased changes:"
git log $(git describe --tags --abbrev=0)..HEAD --oneline

echo -e "\nTo create a release:"
echo "1. Ensure all changes are committed with conventional commit messages"
echo "2. Push to main branch"
echo "3. GitHub Actions will automatically create a release and tag"
echo "4. The version will be determined by your commit messages"

echo -e "\nLocal version preview:"
echo "Run: poetry build"
echo "This will show you what version would be generated"