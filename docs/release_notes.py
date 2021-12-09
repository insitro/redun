"""
Generate release notes for redun's release.

Usage::
    python release_notes.py <current-tag> <new-tag>

Example::
    python release_notes.py 0.4.10 0.4.11
"""
import argparse

from datetime import date
import re
import subprocess


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("current_release_tag")
    parser.add_argument("new_release_tag")
    args = parser.parse_args()

    print(f"## {args.new_release_tag}")
    today = date.today().strftime("%B %d, %Y")
    print(f"{today}\n")

    merge_base = subprocess.check_output(
        f"git merge-base {args.current_release_tag} origin/main", shell=True
    ).decode("utf8")
    commits = subprocess.check_output(
        f"git --no-pager log --pretty='%s' --abbrev-commit {merge_base.strip()}..origin/main",
        shell=True,
    ).decode("utf8")
    for commit in commits.splitlines():
        match = re.search(r"\((#\d+)\)", commit)
        if match:
            pr_num = match.group(1).lstrip("#")
            title = re.sub(r"\(#\d+\)", "", commit)
            print(f"* `#{pr_num}` - {title}")
        else:
            print(f"* {commit}")


if __name__ == "__main__":
    main()
