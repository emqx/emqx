Fixes <issue-or-jira-number>

<!-- Make sure to target release-51 branch if this PR is intended to fix the issues for the release candidate. -->

## Summary
copilot:summary

## PR Checklist
Please convert it to a draft if any of the following conditions are not met. Reviewers may skip over until all the items are checked:

- [ ] Added tests for the changes
- [ ] Added property-based tests for code which performs user input validation
- [ ] Changed lines covered in coverage report
- [ ] Change log has been added to `changes/(ce|ee)/(feat|perf|fix)-<PR-id>.en.md` files
- [ ] For internal contributor: there is a jira ticket to track this change
- [ ] Created PR to [emqx-docs](https://github.com/emqx/emqx-docs) if documentation update is required, or link to a follow-up jira ticket
- [ ] Schema changes are backward compatible

## Checklist for CI (.github/workflows) changes

- [ ] If changed package build workflow, pass [this action](https://github.com/emqx/emqx/actions/workflows/build_packages.yaml) (manual trigger)
- [ ] Change log has been added to `changes/` dir for user-facing artifacts update
