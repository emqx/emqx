Fixes <issue-or-jira-number>

<!--
5.8.7
5.9.1
6.0.0-M1.202507
6.0.0-M2.202508
6.0.0
-->
Release version:

## Summary

<!--
Please compose a nontrivial summary in case of significant changes.
* Point out the crucial changes in logic
* Point out the most relevant files and modules for the change
* Provide some reasoning for the decisions taken
-->

## PR Checklist
<!--
Please convert the PR to a draft if any of the following conditions are not met.
-->
- [ ] For internal contributor: there is a jira ticket to track this change
- [ ] The changes are covered with new or existing tests
- [ ] Change log for changes visible by users has been added to `changes/ee/(feat|perf|fix|breaking)-<PR-id>.en.md` files
- [ ] Schema changes are backward compatible or intentionally breaking (describe the changes and the reasoning in the summary)

<!--
Please, take in account the following guidelines while working on PR:
* Try to achieve reasonable coverage of the new code
* Add property-based tests for code that performs complex user input validation or implements a complex algorithm
* Create a PR to [emqx-docs](https://github.com/emqx/emqx-docs) if documentation update is required, or make a follow-up jira ticket
* Do not squash large PRs into a single commit, try to keep comprehensive history of incremental changes
* Do not squash any significant amount of review fixes into the previous commits
-->

<!--
## Checklist for CI (.github/workflows) changes
- [ ] If changed package build workflow, pass [this action](https://github.com/emqx/emqx/actions/workflows/build_packages.yaml) (manual trigger)
- [ ] Change log has been added to `changes/` dir for user-facing artifacts update
-->
