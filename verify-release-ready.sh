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

if [ "$JAVA_VERSION" != "8" ]; then
    echo -e "${RED}✗ FAIL: Java version is $JAVA_VERSION, but release requires Java 8${NC}"
    echo "  Run: sdk use java 8.0.472-amzn"
    ERRORS=$((ERRORS + 1))
else
    echo -e "${GREEN}✓ PASS: Java 8 detected${NC}"
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

# 4. Check Maven settings
echo "4. Checking Maven settings..."
if [ ! -f ~/.m2/settings.xml ]; then
    echo -e "${RED}✗ FAIL: ~/.m2/settings.xml not found${NC}"
    ERRORS=$((ERRORS + 1))
elif [ ! -L ~/.m2/settings.xml ]; then
    echo -e "${YELLOW}⚠ WARNING: ~/.m2/settings.xml is not a symlink${NC}"
    echo "  For release, should point to settings-spfoss.xml"
else
    SETTINGS_TARGET=$(readlink ~/.m2/settings.xml)
    if [[ "$SETTINGS_TARGET" == *"spfoss"* ]]; then
        echo -e "${GREEN}✓ PASS: Maven settings point to spfoss configuration${NC}"
    else
        echo -e "${YELLOW}⚠ WARNING: Maven settings point to: $SETTINGS_TARGET${NC}"
        echo "  For release, should point to settings-spfoss.xml"
    fi
fi
echo

# 5. Test Javadoc generation
echo "5. Testing Javadoc generation..."
if mvn javadoc:javadoc -q 2>&1 | grep -i "error\|BUILD FAILURE" > /dev/null; then
    echo -e "${RED}✗ FAIL: Javadoc generation failed${NC}"
    echo "  Run: mvn javadoc:javadoc"
    ERRORS=$((ERRORS + 1))
else
    echo -e "${GREEN}✓ PASS: Javadoc generates without errors${NC}"
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
    echo "You're ready to release! Run:"
    echo "  mvn -B -Psonatype-oss-release release:clean release:prepare -Darguments=\"-DskipTests=true\""
    echo "  mvn -B -Psonatype-oss-release release:perform -Darguments=\"-DskipTests=true\""
    exit 0
else
    echo -e "${RED}✗ $ERRORS CHECK(S) FAILED${NC}"
    echo
    echo "Fix the issues above before releasing."
    exit 1
fi
