#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"

mkdir -p "$ROOT/models"

if [ ! -x "$ROOT/llama.cpp/build/bin/llama" ]; then
    rm -rf "$ROOT/llama.cpp"
    git clone --depth 1 https://github.com/ggml-org/llama.cpp.git "$ROOT/llama.cpp"
    cmake -S "$ROOT/llama.cpp" -B "$ROOT/llama.cpp/build" -DCMAKE_BUILD_TYPE=Release
    cmake --build "$ROOT/llama.cpp/build" --config Release -j2
fi

if [ ! -f "$ROOT/models/Qwen3-0.6B-Q4_K_M.gguf" ]; then
    curl -L --fail --retry 3 \
        -o "$ROOT/models/Qwen3-0.6B-Q4_K_M.gguf" \
        "https://huggingface.co/Qwen/Qwen3-0.6B-GGUF/resolve/1208e45d782fe18602c5eaf10e5758d5b0f24c03/Qwen3-0.6B-Q4_K_M.gguf"
fi

echo "OUR SEARCH learned language runtime ready."
