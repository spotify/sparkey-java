#!/bin/bash
# Pre-release verification script for sparkey-java
# Run this before starting the release process to catch common issues

set -e

echo "===== Sparkey-Java Pre-Release Verification ====="
echo

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

ERRORS=0

# 1. Check Java version
echo "1. Checking Java version..."
JAVA_VERSION=$(java -version 2>&1 | head -n 1 | cut -d'"' -f2 | cut -d'.' -f1)
if [ "$JAVA_VERSION" = "1" ]; then
    # Java 8 reports as 1.8.x
    JAVA_VERSION=$(java -version 2>&1 | head -n 1 | cut -d'"' -f2 | cut -d'.' -f2)
fi

# Build requires Java 25+ (tests use release=25)
if [ "$JAVA_VERSION" -lt "25" ]; then
    echo -e "${RED}✗ FAIL: Java version is $JAVA_VERSION, but build requires Java 25+${NC}"
    echo "  Tests are compiled with release=25"
    echo "  Run: sdk use java 25-amzn"
    ERRORS=$((ERRORS + 1))
else
    echo -e "${GREEN}✓ PASS: Java $JAVA_VERSION detected (Maven compiler.release=8 ensures Java 8 bytecode for main code)${NC}"
fi
echo

# 2. Check git status
echo "2. Checking git status..."
if ! git diff-index --quiet HEAD --; then
    echo -e "${RED}✗ FAIL: Working directory has uncommitted changes${NC}"
    git status --short
    ERRORS=$((ERRORS + 1))
else
    echo -e "${GREEN}✓ PASS: Working directory is clean${NC}"
fi
echo

# 2a. Check for leftover release artifacts
echo "2a. Checking for leftover release artifacts..."
ARTIFACTS_FOUND=0

if [ -f release.properties ]; then
    echo -e "${RED}✗ FAIL: release.properties exists from failed release${NC}"
    echo "  Run: rm -f release.properties pom.xml.releaseBackup"
    ARTIFACTS_FOUND=1
fi

if [ -f pom.xml.releaseBackup ]; then
    echo -e "${RED}✗ FAIL: pom.xml.releaseBackup exists from failed release${NC}"
    echo "  Run: rm -f release.properties pom.xml.releaseBackup"
    ARTIFACTS_FOUND=1
fi

# Check for version tag that shouldn't exist yet
CURRENT_VERSION=$(grep -m 1 "<version>" pom.xml | sed 's/.*<version>\(.*\)-SNAPSHOT<\/version>.*/\1/')
if [ -n "$CURRENT_VERSION" ]; then
    VERSION_TAG="sparkey-$CURRENT_VERSION"
    if git tag | grep -q "^$VERSION_TAG\$"; then
        echo -e "${RED}✗ FAIL: Tag $VERSION_TAG already exists (from failed release)${NC}"
        echo "  Run: git tag -d $VERSION_TAG"
        echo "  If also on remote: git push --delete origin $VERSION_TAG"
        ARTIFACTS_FOUND=1
    fi
fi

if [ $ARTIFACTS_FOUND -eq 0 ]; then
    echo -e "${GREEN}✓ PASS: No leftover release artifacts${NC}"
else
    ERRORS=$((ERRORS + 1))
fi
echo

# 3. Check we're on master branch
echo "3. Checking branch..."
BRANCH=$(git branch --show-current)
if [ "$BRANCH" != "master" ]; then
    echo -e "${RED}✗ FAIL: Current branch is '$BRANCH', should be 'master'${NC}"
    ERRORS=$((ERRORS + 1))
else
    echo -e "${GREEN}✓ PASS: On master branch${NC}"
fi
echo

# 3a. Check master is up to date with origin
echo "3a. Checking master is up to date with origin..."
git fetch origin -q 2>/dev/null
LOCAL_COMMIT=$(git rev-parse master)
REMOTE_COMMIT=$(git rev-parse origin/master 2>/dev/null || git rev-parse origin/HEAD 2>/dev/null)

if [ "$LOCAL_COMMIT" != "$REMOTE_COMMIT" ]; then
    # Check if we're ahead or behind
    if git merge-base --is-ancestor origin/master master 2>/dev/null || git merge-base --is-ancestor origin/HEAD master 2>/dev/null; then
        echo -e "${YELLOW}⚠ WARNING: Local master is ahead of origin${NC}"
        echo "  You may have unpushed commits"
        echo "  Local: $LOCAL_COMMIT"
        echo "  Remote: $REMOTE_COMMIT"
    else
        echo -e "${RED}✗ FAIL: Local master is out of sync with origin${NC}"
        echo "  Run: git pull --rebase origin master"
        echo "  Local: $LOCAL_COMMIT"
        echo "  Remote: $REMOTE_COMMIT"
        ERRORS=$((ERRORS + 1))
    fi
else
    echo -e "${GREEN}✓ PASS: Master is up to date with origin${NC}"
fi
echo

# 4. Check Maven effective settings for FOSS deployment servers
echo "4. Checking Maven configuration for FOSS deployment..."
EFFECTIVE_SETTINGS=$(mvn help:effective-settings 2>&1)

# Check for required servers for Sonatype OSSRH deployment
MISSING_SERVERS=""
for SERVER_ID in "ossrh" "sonatype-nexus-snapshots" "sonatype-nexus-staging"; do
    if ! echo "$EFFECTIVE_SETTINGS" | grep -q "<id>$SERVER_ID</id>"; then
        MISSING_SERVERS="$MISSING_SERVERS $SERVER_ID"
    fi
done

if [ -n "$MISSING_SERVERS" ]; then
    echo -e "${RED}✗ FAIL: Maven settings missing required servers:$MISSING_SERVERS${NC}"
    echo "  These servers are required for FOSS deployment to Maven Central"
    echo "  Configure your settings file or use .mvn/maven.config to point to FOSS settings"
    ERRORS=$((ERRORS + 1))
else
    echo -e "${GREEN}✓ PASS: Maven configured with FOSS deployment servers (ossrh, sonatype-nexus-*)${NC}"
fi
echo

# 5. Test Javadoc generation (with release profile)
echo "5. Testing Javadoc generation (with release profile)..."
if mvn -Psonatype-oss-release javadoc:jar -q 2>&1 | grep -i "error\|BUILD FAILURE" > /dev/null; then
    echo -e "${RED}✗ FAIL: Javadoc generation failed with release profile${NC}"
    echo "  Run: mvn -Psonatype-oss-release javadoc:jar"
    ERRORS=$((ERRORS + 1))
else
    echo -e "${GREEN}✓ PASS: Javadoc generates without errors (release profile)${NC}"
fi
echo

# 6. Check SCM configuration
echo "6. Checking SCM configuration..."
POM_SCM=$(grep -A 1 "<developerConnection>" pom.xml | grep "scm:git:" | sed 's/.*scm:git:\(.*\)<.*/\1/')
GIT_REMOTE=$(git remote get-url origin)

# Extract just the host:path part for comparison
POM_HOST=$(echo "$POM_SCM" | sed 's|.*@\(.*\)|\1|')
GIT_HOST=$(echo "$GIT_REMOTE" | sed 's|.*@\(.*\)|\1|')

if [ "$POM_HOST" != "$GIT_HOST" ]; then
    # Check if git URL rewriting is configured
    URL_REWRITE=$(git config --get-regexp "url\..*\.insteadof" | grep "git@github.com:" || true)
    if [ -n "$URL_REWRITE" ]; then
        echo -e "${GREEN}✓ PASS: SCM URL mismatch, but git URL rewriting is configured${NC}"
        echo "  URL rewrite: $URL_REWRITE"
    else
        echo -e "${RED}✗ FAIL: SCM URL in pom.xml doesn't match git remote${NC}"
        echo "  pom.xml: $POM_SCM"
        echo "  git remote: $GIT_REMOTE"
        echo "  Fix: git config --local url.\"git@YOUR-ALIAS:\".insteadOf \"git@github.com:\""
        ERRORS=$((ERRORS + 1))
    fi
else
    echo -e "${GREEN}✓ PASS: SCM URL matches git remote${NC}"
fi
echo

# 7. Run tests
echo "7. Running tests (this may take a while)..."
if mvn test -q 2>&1 | tee /tmp/sparkey-test-output.txt | grep -i "BUILD FAILURE" > /dev/null; then
    echo -e "${RED}✗ FAIL: Tests failed${NC}"
    echo "  Check /tmp/sparkey-test-output.txt for details"
    ERRORS=$((ERRORS + 1))
else
    echo -e "${GREEN}✓ PASS: All tests passed${NC}"
fi
echo

# 8. Check GPG key
echo "8. Checking GPG key..."
if ! gpg --list-secret-keys --keyid-format LONG | grep -q "sec"; then
    echo -e "${RED}✗ FAIL: No GPG secret key found${NC}"
    echo "  You need a GPG key to sign artifacts"
    ERRORS=$((ERRORS + 1))
else
    KEY_ID=$(gpg --list-secret-keys --keyid-format LONG | grep -A 1 "^sec" | tail -1 | tr -d ' ')
    echo -e "${GREEN}✓ PASS: GPG key found: $KEY_ID${NC}"
fi
echo

# 9. Verify bytecode version
echo "9. Verifying bytecode version..."
mvn clean compile -q > /dev/null 2>&1
BYTECODE_VERSION=$(javap -verbose target/classes/com/spotify/sparkey/Sparkey.class 2>/dev/null | grep "major version" | awk '{print $NF}')
if [ "$BYTECODE_VERSION" != "52" ]; then
    echo -e "${RED}✗ FAIL: Bytecode version is $BYTECODE_VERSION, expected 52 (Java 8)${NC}"
    ERRORS=$((ERRORS + 1))
else
    echo -e "${GREEN}✓ PASS: Bytecode is Java 8 compatible (version 52)${NC}"
fi
echo

# Summary
echo "=========================================="
if [ $ERRORS -eq 0 ]; then
    echo -e "${GREEN}✓ ALL CHECKS PASSED${NC}"
    echo
    echo "You're ready to release!"
    echo
    echo "To test the release process (dry run - no commits/tags/pushes):"
    echo "  mvn -B -Psonatype-oss-release release:prepare -DdryRun=true -Darguments=\"-DskipTests=true\""
    echo
    echo "To perform the actual release:"
    echo "  mvn -B -Psonatype-oss-release release:clean release:prepare release:perform -Darguments=\"-DskipTests=true\""
    exit 0
else
    echo -e "${RED}✗ $ERRORS CHECK(S) FAILED${NC}"
    echo
    echo "Fix the issues above before releasing."
    exit 1
fi
