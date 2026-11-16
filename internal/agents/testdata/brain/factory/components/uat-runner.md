# uat-runner (placeholder)

Will run an epic's final UAT once a UAT command exists. Session, one per epic. Not built yet: the
program refuses `uat: required` while this file is a placeholder, so this session never starts.

- **Input:** the epic folder and `uat/plan.md`.
- **Output:** `uat/result.md` with `## Result` (`pass` or `fail` on its first line) and
  `## Evidence`.
