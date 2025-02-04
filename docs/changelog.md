# Changelog

The following contains all major, minor, and patch version release notes.

## Version 0.1.4

<span class="md-h2-subheader">Release Date: 2025-02-05</span>

-   Workspace ID replacement improved
-   Increased error handling in activate script

## Version 0.1.3

<span class="md-h2-subheader">Release Date: 2025-01-29</span>

-   Update documentation & examples
-   Resolves [#75](https://github.com/microsoft/fabric-cicd/issues/75) - Add PyPI check version to encourage version bumps
-   Fixes [#77](https://github.com/microsoft/fabric-cicd/issues/77) - Items in subfolders are skipped
-   Fixes [#76](https://github.com/microsoft/fabric-cicd/issues/76) - Default item types fail to unpublish
-   Fixes [#61](https://github.com/microsoft/fabric-cicd/issues/61) - Semantic model initial publish results in None Url error
-   Fixes [#63](https://github.com/microsoft/fabric-cicd/issues/63) - Integer parsed as float failing in handle_retry for <3.12 python

## Version 0.1.2

<span class="md-h2-subheader">Release Date: 2025-01-27</span>

-   Resolves [#27](https://github.com/microsoft/fabric-cicd/issues/27) - Introduces max retry and backoff for long running / throttled calls
-   Fixes [#50](https://github.com/microsoft/fabric-cicd/issues/50) - Environment publish uses arbitrary wait time
-   Fixes [#56](https://github.com/microsoft/fabric-cicd/issues/56) - Environment publish doesn't wait for success
-   Fixes [#58](https://github.com/microsoft/fabric-cicd/issues/58) - Long running operation steps out early for notebook publish

## Version 0.1.1

<span class="md-h2-subheader">Release Date: 2025-01-23</span>

-   Fixes [#51](https://github.com/microsoft/fabric-cicd/issues/51) - Environment stuck in publish

## Version 0.1.0

<span class="md-h2-subheader">Release Date: 2025-01-23</span>

-   Initial public preview release
-   Supports Notebook, Pipeline, Semantic Model, Report, and Environment deployments
-   Supports User and System Identity authentication
-   Released to PyPi
-   Onboarded to Github Pages
