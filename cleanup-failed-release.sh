#!/bin/bash
# Cleanup script for failed maven releases

set -e

echo "=== Cleaning up failed release artifacts ==="
echo

# Get current version from pom.xml
CURRENT_VERSION=$(grep -m 1 "<version>" pom.xml | sed 's/.*<version>\(.*\)-SNAPSHOT<\/version>.*/\1/')
VERSION_TAG="sparkey-$CURRENT_VERSION"

echo "Current version: $CURRENT_VERSION"
echo "Expected tag: $VERSION_TAG"
echo

# 1. Delete release files
echo "1. Cleaning up release files..."
if [ -f release.properties ] || [ -f pom.xml.releaseBackup ]; then
    rm -f release.properties pom.xml.releaseBackup
    echo "   ✓ Deleted release.properties and pom.xml.releaseBackup"
else
    echo "   ✓ No release files to clean"
fi
echo

# 2. Delete local tag
echo "2. Checking for local tag..."
if git tag | grep -q "^$VERSION_TAG\$"; then
    git tag -d "$VERSION_TAG"
    echo "   ✓ Deleted local tag: $VERSION_TAG"
else
    echo "   ✓ No local tag to delete"
fi
echo

# 3. Check for remote tag
echo "3. Checking for remote tag..."
if git ls-remote --tags origin | grep -q "refs/tags/$VERSION_TAG\$"; then
    echo "   ⚠ Remote tag exists: $VERSION_TAG"
    read -p "   Delete remote tag? (y/N) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        git push --delete origin "$VERSION_TAG"
        echo "   ✓ Deleted remote tag: $VERSION_TAG"
    else
        echo "   ⚠ Remote tag NOT deleted (you can delete it later with: git push --delete origin $VERSION_TAG)"
    fi
else
    echo "   ✓ No remote tag to delete"
fi
echo

# 4. Reset pom.xml if needed
echo "4. Checking pom.xml version..."
POM_VERSION=$(grep -m 1 "<version>" pom.xml | sed 's/.*<version>\(.*\)<\/version>.*/\1/')
if [[ "$POM_VERSION" != *"-SNAPSHOT" ]]; then
    echo "   ⚠ WARNING: pom.xml version is $POM_VERSION (not a SNAPSHOT)"
    echo "   You may need to manually reset to $CURRENT_VERSION-SNAPSHOT"
    echo "   Or run: mvn release:rollback"
else
    echo "   ✓ pom.xml version is correct: $POM_VERSION"
fi
echo

echo "=== Cleanup complete ==="
echo
echo "Run ./verify-release-ready.sh to check if everything is ready for release"
