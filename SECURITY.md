# Security Policy

## Supported Versions

| Version | Supported          |
| ------- | ------------------ |
| 5.1.x   | :white_check_mark: |
| 5.0.x   | :white_check_mark: |
| 4.4.x   | :white_check_mark: |
| < 4.4   | :x:                |

## Qualifying Vulnerabilities

Any design or implementation issue that substantially affects the confidentiality or integrity of user data is likely to be in scope for the program. Common examples including:

* Cross-site scripting
* Cross-site request forgery
* Mixed-content scripts
* Authentication or authorization flaws
* Server-side code execution bugs

Out of concern for the availability of our services to all users, please do not attempt to carry out DoS attacks, leverage black hat SEO techniques, spam people, brute force authentication, or do other similarly questionable things. We also discourage the use of any vulnerability testing tools that automatically generate very significant volumes of traffic.

## Non-qualifying Vulnerabilities

Depending on their impacts, some of the reported issues may not qualify.
Although we review them on a case-by-case basis, here are some of the issues that typically do not earn a monetary reward:

* Bugs requiring exceedingly unlikely user interaction Brute forcing
* User enumeration
* Non security related bugs
* Abuse

## Reporting a Vulnerability

1. When investigating a vulnerability, please, only ever target your own accounts. Never attempt to access anyone else's data and do not engage in any activity that would be disruptive or damaging to other users.
2. In the case the same vulnerability is present on multiple products, please combine and send one report.
3. If you have found a vulnerability, please contact us at security@emqx.io.
4. Note that we are only able to answer technical vulnerability reports. Duplicate reports will not be rewarded, first report on the specific vulnerability will be rewarded.
5. The report should include steps in plain text how to reproduce the vulnerability (not only video or images).
