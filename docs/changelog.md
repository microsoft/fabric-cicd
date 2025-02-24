# Changelog

The following contains all major, minor, and patch version release notes.

-   ✨ New Functionality
-   🔧 Bug Fix
-   📝 Documentation Update
-   ⚡ Internal Optimization

## Version 0.1.6

<span class="md-h2-subheader">Release Date: 2025-02-24</span>

-   ✨ Resolves [#116](https://github.com/microsoft/fabric-cicd/issues/116) - Onboard Lakehouse item type
-   📝 Resolves [#25](https://github.com/microsoft/fabric-cicd/issues/25) - Refine documentation
-   📝 Resolves [#110](https://github.com/microsoft/fabric-cicd/issues/110) - Update find_replace docs
-   ⚡ Standardized docstrings to Google format
-   ⚡ Onboard file objects
-   ⚡ Resolves [#28](https://github.com/microsoft/fabric-cicd/issues/28) - Leverage UpdateDefinition Flag
-   ⚡ Resolves [#45](https://github.com/microsoft/fabric-cicd/issues/28) - Convert repo and workspace dictionaries

## Version 0.1.5

<span class="md-h2-subheader">Release Date: 2025-02-18</span>

-   🔧 Fixes [#103](https://github.com/microsoft/fabric-cicd/issues/103) - Environment Failure without Public Library
-   ⚡ Resolves [#100](https://github.com/microsoft/fabric-cicd/issues/100) - Introduces pytest check for PRs

## Version 0.1.4

<span class="md-h2-subheader">Release Date: 2025-02-12</span>

-   ✨ Resolves [#96](https://github.com/microsoft/fabric-cicd/issues/96)- Support Feature Flagging
-   🔧 Fixes [#88](https://github.com/microsoft/fabric-cicd/issues/88) - Image support in report deployment
-   🔧 Fixes [#92](https://github.com/microsoft/fabric-cicd/issues/92) - Broken README link
-   ⚡ Workspace ID replacement improved
-   ⚡ Increased error handling in activate script
-   ⚡ Onboard pytest and coverage
-   ⚡ Resolves [#37](https://github.com/microsoft/fabric-cicd/issues/37) - Improvements to nested dictionaries
-   ⚡ Resolves [#87](https://github.com/microsoft/fabric-cicd/issues/87) - Support Python Installed From Windows Store

## Version 0.1.3

<span class="md-h2-subheader">Release Date: 2025-01-29</span>

-   ✨ Resolves [#75](https://github.com/microsoft/fabric-cicd/issues/75) - Add PyPI check version to encourage version bumps
-   🔧 Fixes [#61](https://github.com/microsoft/fabric-cicd/issues/61) - Semantic model initial publish results in None Url error
-   🔧 Fixes [#63](https://github.com/microsoft/fabric-cicd/issues/63) - Integer parsed as float failing in handle_retry for <3.12 python
-   🔧 Fixes [#76](https://github.com/microsoft/fabric-cicd/issues/76) - Default item types fail to unpublish
-   🔧 Fixes [#77](https://github.com/microsoft/fabric-cicd/issues/77) - Items in subfolders are skipped
-   📝 Update documentation & examples

## Version 0.1.2

<span class="md-h2-subheader">Release Date: 2025-01-27</span>

-   ✨ Resolves [#27](https://github.com/microsoft/fabric-cicd/issues/27) - Introduces max retry and backoff for long running / throttled calls
-   🔧 Fixes [#50](https://github.com/microsoft/fabric-cicd/issues/50) - Environment publish uses arbitrary wait time
-   🔧 Fixes [#56](https://github.com/microsoft/fabric-cicd/issues/56) - Environment publish doesn't wait for success
-   🔧 Fixes [#58](https://github.com/microsoft/fabric-cicd/issues/58) - Long running operation steps out early for notebook publish

## Version 0.1.1

<span class="md-h2-subheader">Release Date: 2025-01-23</span>

-   🔧 Fixes [#51](https://github.com/microsoft/fabric-cicd/issues/51) - Environment stuck in publish

## Version 0.1.0

<span class="md-h2-subheader">Release Date: 2025-01-23</span>

-   ✨ Initial public preview release
-   ✨ Supports Notebook, Pipeline, Semantic Model, Report, and Environment deployments
-   ✨ Supports User and System Identity authentication
-   ✨ Released to PyPi
-   ✨ Onboarded to Github Pages
