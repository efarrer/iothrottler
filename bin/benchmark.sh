#!/bin/bash -e

cd $(dirname "$0")

BIN=./command-line-arguments.test
DIR=benchmarks

rm -rf ../${DIR}
mkdir -p ../${DIR}
cd ../${DIR};

function benchmark
{
    bench="$1"

    ${BIN} \
    -test.run "$1" \
    -test.bench "$1" \
    -test.memprofile=$1.mem \
    -test.cpuprofile=$1.prof
}

go test -c ../*.go &&
benchmark BenchmarkNew &&
benchmark BenchmarkAdd &&
benchmark BenchmarkFull

echo ""
echo "View profiles with:"
echo "go tool pprof -gv ./${DIR}/${BIN} ./${DIR}/<file>.prof"
