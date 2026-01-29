
## Immediate

- [ ] To the root of this repo, need to add a Readme.md that describes the project, a well-crafted CLAUDE.md to guide future claude sessions with skills (https://code.claude.com/docs/en/skills#extend-claude-with-skills)
- [ ] commit changes to feature/saga-parsl-integration for both saga and parsl submodules. for parsl, move all commits for this (starting with da7492d9ddcd510f4e572e19e49f3ba290adb8e1) to this branch and push.
  - [ ] create a review skill to look at all commits on the feature/saga-parsl-integration and make sure everything is implemented in the simplest possible way, adding/changing as little code as possible, easy to understand/read, and overwrite the existing summary of the project with a new fresh summary that captures everything we did.

## Next

- [ ] SAGA schedulers depend on task graphs and networks having weights. How do we integrate this naturally with Parsl? In practice, we could do some kind of benchmarking to figure out the task graph weights, but maybe also if there was a way for users to specify them?