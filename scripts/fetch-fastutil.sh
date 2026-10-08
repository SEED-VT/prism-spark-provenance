#!/bin/sh
# Downloads fastutil 8.5.15, the one Prism dependency that Spark does not ship, into jars/
# and checks it against the SHA-1 that Maven Central publishes.
set -e
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
JAR="$ROOT/jars/fastutil-8.5.15.jar"
URL=https://repo1.maven.org/maven2/it/unimi/dsi/fastutil/8.5.15/fastutil-8.5.15.jar
SHA1=1e885b40c9563ab0d3899b871fd0b30e958705dc
[ -f "$JAR" ] && exit 0
mkdir -p "$ROOT/jars"
curl -fsSL -o "$JAR.part" "$URL"
echo "$SHA1  $JAR.part" | sha1sum -c --quiet
mv "$JAR.part" "$JAR"
echo "downloaded $JAR"
