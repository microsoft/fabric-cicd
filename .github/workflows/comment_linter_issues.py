import os
import requests

def post_comment(repo, pr_number, commit_id, path, line, message):
    url = f"https://api.github.com/repos/{repo}/pulls/{pr_number}/comments"
    headers = {
        "Authorization": f"token {os.getenv('GITHUB_TOKEN')}",
        "Accept": "application/vnd.github.v3+json",
    }
    data = {
        "body": message,
        "commit_id": commit_id,
        "path": path,
        "side": "RIGHT",
        "line": line,
    }
    response = requests.post(url, headers=headers, json=data)
    response.raise_for_status()

def parse_flake8_output(repo, pr_number, commit_id, linter_output):
    for line in linter_output.splitlines():
        parts = line.split(":")
        if len(parts) >= 4:
            path, line_number, _, message = parts[0], int(parts[1]), parts[2], ":".join(parts[3:])
            post_comment(repo, pr_number, commit_id, path, line_number, message)

def parse_black_output(repo, pr_number, commit_id, linter_output):
    for line in linter_output.splitlines():
        if line.startswith("would reformat"):
            parts = line.split()
            path = parts[2]
            message = "Black would reformat this file."
            post_comment(repo, pr_number, commit_id, path, 1, message)

if __name__ == "__main__":
    repo = os.getenv("GITHUB_REPOSITORY")
    pr_number = os.getenv("PR_NUMBER")
    commit_id = os.getenv("GITHUB_SHA")
    
    with open("flake8_output.txt") as f:
        flake8_output = f.read()
    parse_flake8_output(repo, pr_number, commit_id, flake8_output)
    
    with open("black_output.txt") as f:
        black_output = f.read()
    parse_black_output(repo, pr_number, commit_id, black_output)