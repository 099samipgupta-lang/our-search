from __future__ import annotations

import subprocess
from pathlib import Path


class LlamaRuntime:
    VERSION = "llama-runtime.v1"

    def __init__(self, model_path=None, executable=None):
        root = Path(__file__).resolve().parents[2]
        self.model_path = Path(
            model_path or root / "models" / "Qwen3-0.6B-Q4_K_M.gguf"
        )
        self.executable = Path(
            executable or root / "llama.cpp" / "build" / "bin" / "llama"
        )

    def generate(self, prompt, max_tokens=128):
        command = [
            str(self.executable),
            "cli",
            "-m",
            str(self.model_path),
            "-c",
            "512",
            "-t",
            "2",
            "-n",
            str(max_tokens),
            "--single-turn",
            "--simple-io",
            "--no-display-prompt",
            "-p",
            str(prompt),
        ]

        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=120,
        )

        if result.returncode != 0:
            raise RuntimeError(
                f"llama.cpp failed with code {result.returncode}: "
                f"stdout={result.stdout.strip()!r} "
                f"stderr={result.stderr.strip()!r} "
                f"command={command!r}"
            )

        output = result.stdout + "\n" + result.stderr
        if "Assistant:" in output:
            output = output.split("Assistant:", 1)[1]
        if "[Start thinking]" in output and "[End thinking]" in output:
            output = output.split("[End thinking]", 1)[1]
        if "[ Prompt:" in output:
            output = output.split("[ Prompt:", 1)[0]
        if "Exiting..." in output:
            output = output.split("Exiting...", 1)[0]
        return output.strip()
