# Post-Release Checklist

After a successful `mvn release:perform`, verify everything completed correctly.

## Immediate Verification (< 5 minutes)

### 1. Check Git State

```bash
# Verify tag was created and pushed
git fetch --tags
git tag | grep sparkey-X.X.X
git ls-remote --tags origin | grep sparkey-X.X.X

# Verify version was bumped
grep "<version>" pom.xml | head -1
# Should show X.X.X+1-SNAPSHOT

# Check release commits
git log --oneline -3
# Should show:
# - [maven-release-plugin] prepare for next development iteration
# - [maven-release-plugin] prepare release sparkey-X.X.X

# Verify working tree is clean
git status
```

### 2. Verify Central Publishing Portal

Check deployment status:
https://central.sonatype.com/publishing/deployments

Look for your deployment:
- **Status**: Should be "PUBLISHED" or "PUBLISHING"
- **Deployment ID**: From the release log
- **No validation errors**

### 3. Check Local Artifacts

Verify artifacts were installed to local Maven repo:
```bash
ls -lh ~/.m2/repository/com/spotify/sparkey/sparkey/X.X.X/
```

Should contain:
- `sparkey-X.X.X.jar`
- `sparkey-X.X.X-sources.jar`
- `sparkey-X.X.X-javadoc.jar`
- `sparkey-X.X.X.pom`
- All `.asc` signature files

## Maven Central Verification (30 minutes - 2 hours)

### 4. Check Maven Central Search

After ~30 minutes, verify artifact appears:
- https://central.sonatype.com/artifact/com.spotify.sparkey/sparkey/X.X.X
- https://search.maven.org/artifact/com.spotify.sparkey/sparkey/X.X.X

### 5. Verify Downloadable

Try downloading the artifact:
```bash
curl -O https://repo1.maven.org/maven2/com/spotify/sparkey/sparkey/X.X.X/sparkey-X.X.X.pom
cat sparkey-X.X.X.pom | grep -A 2 "<version>"
rm sparkey-X.X.X.pom
```

## GitHub Release (Optional but Recommended)

### 6. Create GitHub Release

```bash
# View CHANGELOG for release notes
cat CHANGELOG.md | head -30

# Create GitHub release
gh release create sparkey-X.X.X \
  --title "Version X.X.X" \
  --notes "$(sed -n '/^#### X.X.X$/,/^#### [0-9]/p' CHANGELOG.md | head -n -1)"
```

Or manually at: https://github.com/spotify/sparkey-java/releases/new

## Communication (If Needed)

### 7. Announce Release

If this is a major release or contains important fixes:
- Update README.md if needed
- Post announcement (Slack, mailing list, etc.)
- Update dependent projects

## Rollback (If Problems Found)

If you discover issues AFTER release:

**DO NOT delete from Maven Central** (artifacts are immutable)

Instead:
1. Fix the issue
2. Release a new patch version immediately (X.X.X+1)
3. Document the issue in CHANGELOG.md

## Troubleshooting

### Deployment shows "FAILED" on Central Portal

1. Check the error message on https://central.sonatype.com/publishing/deployments
2. Common issues:
   - Missing metadata (name, description, url)
   - Invalid POM structure
   - Missing or invalid signatures

### Tag pushed but no artifacts on Maven Central

1. Check if `release:perform` completed successfully
2. Look for deployment errors in the Maven output
3. Check https://central.sonatype.com/publishing/deployments for the deployment

### Version not bumped correctly

The release plugin should have bumped the version. If not:
```bash
# Manually bump version
# Edit pom.xml: <version>X.X.X-SNAPSHOT</version> → <version>X.X.X+1-SNAPSHOT</version>
git add pom.xml
git commit -m "Bump version to X.X.X+1-SNAPSHOT"
git push
```
