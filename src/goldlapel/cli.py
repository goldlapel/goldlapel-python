import os
import subprocess
import sys

from goldlapel.proxy import _find_binary


def main():
    try:
        binary = _find_binary()
    except FileNotFoundError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

    if os.name != "nt":
        os.execvp(binary, [binary] + sys.argv[1:])
    else:
        result = subprocess.run([binary] + sys.argv[1:])
        sys.exit(result.returncode)
