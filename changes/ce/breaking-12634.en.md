Triple-quote string values in HOCON config files no longer support escape sequence.

The detailed information can be found in [this pull request](https://github.com/emqx/hocon/pull/290).
Here is a summary for the impact on EMQX users:

- EMQX 5.6 is the first version to generate triple-quote strings in `cluster.hocon`,
  meaning for generated configs, there is no compatibility issue.
- For user hand-crafted configs (such as `emqx.conf`) a thorough review is needed
  to inspect if escape sequences are used (such as `\n`, `\r`, `\t` and `\\`), if yes,
  such strings should be changed to regular quotes (one pair of `"`) instead of triple-quotes.
