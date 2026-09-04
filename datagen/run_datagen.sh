#!/usr/bin/env bash
# clona il datagen LDBC FinBench (tag v0.1.0), lo builda e genera SF 1.
# i CSV finiscono in data/bulk-load/. serve java 8, maven, python 3.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
REPO_DIR="$HERE/_repo"
OUT_DIR="$ROOT/data/bulk-load"

# 1) clone (solo se non già presente)
if [ ! -d "$REPO_DIR" ]; then
  echo ">> clono ldbc_finbench_datagen @ v0.1.0"
  git clone --depth 1 --branch v0.1.0 \
    https://github.com/ldbc/ldbc_finbench_datagen.git "$REPO_DIR"
fi

cd "$REPO_DIR"

# 2) build con maven (skip test per velocità)
if [ ! -f target/ldbc_finbench_datagen-0.1.0-jar-with-dependencies.jar ]; then
  echo ">> build maven"
  mvn clean package -DskipTests
fi

# 3) scarica Spark locale se manca (lo script del datagen lo mette in ~/spark)
if [ ! -d "$HOME/spark" ]; then
  echo ">> scarico Spark"
  bash scripts/get-spark-to-home.sh
fi
export SPARK_HOME="$HOME/spark"
export PATH="$SPARK_HOME/bin:$PATH"

# 4) generazione dataset SF 0.1
echo ">> genero dataset SF 1"
rm -rf out
python3 scripts/run.py \
  --jar target/ldbc_finbench_datagen-0.1.0-jar-with-dependencies.jar \
  --main-class ldbc.finbench.datagen.LdbcDatagen \
  --memory 8g \
  -- --scale-factor 1 --output-dir out

# 5) sposta i CSV grezzi in data/bulk-load/
echo ">> copio CSV in $OUT_DIR"
mkdir -p "$OUT_DIR"
rm -rf "$OUT_DIR"/*
cp -r out/* "$OUT_DIR"/

echo ">> fatto. CSV disponibili in $OUT_DIR"
