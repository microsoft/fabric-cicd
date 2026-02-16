# Changelog

## [v0.2.0](https://pypi.org/project/fabric-cicd/0.2.0) - February 16, 2026

### ✨ New Functionality

- Support parallelize deployments within a given item type by [mdrakiburrahman](https://github.com/mdrakiburrahman) ([#719](https://github.com/microsoft/fabric-cicd/issues/719))
- Add a black-box REST API testing harness by [mdrakiburrahman](https://github.com/mdrakiburrahman) ([#738](https://github.com/microsoft/fabric-cicd/issues/738))
- Change header print messages to info log by [mwc360](https://github.com/mwc360) ([#771](https://github.com/microsoft/fabric-cicd/issues/771))
- Add support for semantic model binding per environment by [shirasassoon](https://github.com/shirasassoon) ([#689](https://github.com/microsoft/fabric-cicd/issues/689))

### 🔧 Bug Fix

- Remove OrgApp item type support by [shirasassoon](https://github.com/shirasassoon) ([#758](https://github.com/microsoft/fabric-cicd/issues/758))
- Improve environment-mapping behavior in optional config fields by [shirasassoon](https://github.com/shirasassoon) ([#716](https://github.com/microsoft/fabric-cicd/issues/716))
- Fix duplicate YAML key detection in parameter validation by [shirasassoon](https://github.com/shirasassoon) ([#752](https://github.com/microsoft/fabric-cicd/issues/752))
- Add caching for item attribute lookups by [MiSchroe](https://github.com/MiSchroe) ([#704](https://github.com/microsoft/fabric-cicd/issues/704))

### ⚡ Additional Optimizations

- Enable configuration-based deployment without feature flags by [shirasassoon](https://github.com/shirasassoon) ([#805](https://github.com/microsoft/fabric-cicd/issues/805))

### 📝 Documentation Update

- Fix troubleshooting docs by [shirasassoon](https://github.com/shirasassoon) ([#747](https://github.com/microsoft/fabric-cicd/issues/747))

## [v0.1.34](https://pypi.org/project/fabric-cicd/0.1.34) - January 20, 2026

### ✨ New Functionality

- Enable dynamic replacement of SQL endpoint values from SQL Database items ([#720](https://github.com/microsoft/fabric-cicd/issues/720))
- Support Fabric Notebook Authentication ([#707](https://github.com/microsoft/fabric-cicd/issues/707))

### 🆕 New Items Support

- Onboard Spark Job Definition item type ([#115](https://github.com/microsoft/fabric-cicd/issues/115))

### 📝 Documentation Update

- Add `CONTRIBUTING.md` file to repository ([#723](https://github.com/microsoft/fabric-cicd/issues/723))
- Add comprehensive troubleshooting guide to documentation ([#705](https://github.com/microsoft/fabric-cicd/issues/705))
- Add parameterization documentation for Report items using ByConnection binding to Semantic Models ([#637](https://github.com/microsoft/fabric-cicd/issues/637))

### ⚡ Additional Optimizations

- Add debug file for local Fabric REST API testing ([#714](https://github.com/microsoft/fabric-cicd/issues/714))

## [v0.1.33](https://pypi.org/project/fabric-cicd/0.1.33) - December 16, 2025

### ✨ New Functionality

- Add key_value_replace parameter support for YAML files ([#649](https://github.com/microsoft/fabric-cicd/issues/649))
- Support selective shortcut publishing with regex exclusion ([#624](https://github.com/microsoft/fabric-cicd/issues/624))

### ⚡ Additional Optimizations

- Add Linux development environment bootstrapping script ([#680](https://github.com/microsoft/fabric-cicd/issues/680))
- Update item types in scope to be an optional parameter in validate parameter file function ([#669](https://github.com/microsoft/fabric-cicd/issues/669))

### 🔧 Bug Fix

- Fix publish order for Notebook and Eventhouse dependent items ([#685](https://github.com/microsoft/fabric-cicd/issues/685))
- Enable parameterizing multiple connections in the same Semantic Model item ([#674](https://github.com/microsoft/fabric-cicd/issues/674))
- Fix missing description metadata in item payload for shell-only item deployments ([#672](https://github.com/microsoft/fabric-cicd/issues/672))
- Resolve API long running operation handling when publishing Environment items ([#668](https://github.com/microsoft/fabric-cicd/issues/668))

## [v0.1.32](https://pypi.org/project/fabric-cicd/0.1.32) - December 03, 2025

### 🔧 Bug Fix

- Fix publish bug for Environment items that contain only spark settings ([#664](https://github.com/microsoft/fabric-cicd/issues/664))

## [v0.1.31](https://pypi.org/project/fabric-cicd/0.1.31) - December 01, 2025

### ⚠️ Breaking Change

- Migrate to the latest Fabric Environment item APIs to simplify deployment and improve compatibility ([#173](https://github.com/microsoft/fabric-cicd/issues/173))

### ✨ New Functionality

- Enable dynamic replacement of Lakehouse SQL Endpoint IDs ([#616](https://github.com/microsoft/fabric-cicd/issues/616))
- Enable linking of Semantic Models to both cloud and gateway connections ([#602](https://github.com/microsoft/fabric-cicd/issues/602))
- Allow use of the dynamic replacement variables within the key_value_replace parameter ([#567](https://github.com/microsoft/fabric-cicd/issues/567))
- Add support for parameter file templates ([#499](https://github.com/microsoft/fabric-cicd/issues/499))

### 🆕 New Items Support

- Add support for the ML Experiment item type ([#600](https://github.com/microsoft/fabric-cicd/issues/600))
- Add support for the User Data Function item type ([#588](https://github.com/microsoft/fabric-cicd/issues/588))

### 📝 Documentation Update

- Update the advanced Dataflow parameterization example with the correct file_path value ([#633](https://github.com/microsoft/fabric-cicd/issues/633))

### 🔧 Bug Fix

- Fix publishing issues for KQL Database items in folders ([#657](https://github.com/microsoft/fabric-cicd/issues/657))
- Separate logic for 'items to include' feature between publish and unpublish operations ([#650](https://github.com/microsoft/fabric-cicd/issues/650))
- Fix parameterization logic to properly handle find_value regex patterns and replacements ([#639](https://github.com/microsoft/fabric-cicd/issues/639))
- Correct the publish order of Data Agent and Semantic Model items ([#628](https://github.com/microsoft/fabric-cicd/issues/628))
- Fix Lakehouse item publishing errors when shortcuts refer to the default Lakehouse ID ([#610](https://github.com/microsoft/fabric-cicd/issues/610))

## [v0.1.30](https://pypi.org/project/fabric-cicd/0.1.30) - October 20, 2025

### ✨ New Functionality

- Add support for binding semantic models to on-premise gateways in Fabric workspaces ([#569](https://github.com/microsoft/fabric-cicd/issues/569))

### 🆕 New Items Support

- Add support for publishing and managing Data Agent items ([#556](https://github.com/microsoft/fabric-cicd/issues/556))
- Add OrgApp item type support ([#586](https://github.com/microsoft/fabric-cicd/issues/586))

### ⚡ Additional Optimizations

- Enhance cross-workspace variable support to allow referencing other attributes ([#583](https://github.com/microsoft/fabric-cicd/issues/583))

### 🔧 Bug Fix

- Fix workspace name extraction bug for non-ID attrs using ITEM_ATTR_LOOKUP ([#583](https://github.com/microsoft/fabric-cicd/issues/583))
- Fix capacity requirement check ([#593](https://github.com/microsoft/fabric-cicd/issues/593))

## [v0.1.29](https://pypi.org/project/fabric-cicd/0.1.29) - October 01, 2025

### ✨ New Functionality

- Support dynamic replacement for cross-workspace item IDs ([#558](https://github.com/microsoft/fabric-cicd/issues/558))
- Add option to return API response for publish operations in publish_all_items ([#497](https://github.com/microsoft/fabric-cicd/issues/497))

### 🆕 New Items Support

- Onboard Apache Airflow Job item type ([#565](https://github.com/microsoft/fabric-cicd/issues/565))
- Onboard Mounted Data Factory item type ([#406](https://github.com/microsoft/fabric-cicd/issues/406))

### 🔧 Bug Fix

- Fix publish order of Eventhouses and Semantic Models ([#566](https://github.com/microsoft/fabric-cicd/issues/566))

## [v0.1.28](https://pypi.org/project/fabric-cicd/0.1.28) - September 15, 2025

### ✨ New Functionality

- Add folder exclusion feature for publish operations ([#427](https://github.com/microsoft/fabric-cicd/issues/427))
- Expand workspace ID dynamic replacement capabilities in parameterization ([#408](https://github.com/microsoft/fabric-cicd/issues/408))

### 🔧 Bug Fix

- Fix unexpected behavior with file_path parameter filter ([#545](https://github.com/microsoft/fabric-cicd/issues/545))
- Fix unpublish exclude_regex bug in configuration file-based deployment ([#544](https://github.com/microsoft/fabric-cicd/issues/544))

## [v0.1.27](https://pypi.org/project/fabric-cicd/0.1.27) - September 05, 2025

### 🔧 Bug Fix

- Fix trailing comma in report schema ([#534](https://github.com/microsoft/fabric-cicd/issues/534))

## [v0.1.26](https://pypi.org/project/fabric-cicd/0.1.26) - September 05, 2025

### ⚠️ Breaking Change

- Deprecate Base API URL kwarg in Fabric Workspace ([#529](https://github.com/microsoft/fabric-cicd/issues/529))

### ✨ New Functionality

- Support Schedules parameterization ([#508](https://github.com/microsoft/fabric-cicd/issues/508))
- Support YAML configuration file-based deployment ([#470](https://github.com/microsoft/fabric-cicd/issues/470))

### 📝 Documentation Update

- Add dynamically generated Python version requirements to documentation ([#520](https://github.com/microsoft/fabric-cicd/issues/520))

### ⚡ Additional Optimizations

- Enhance pytest output to limit console verbosity ([#514](https://github.com/microsoft/fabric-cicd/issues/514))

### 🔧 Bug Fix

- Fix Report item schema handling ([#518](https://github.com/microsoft/fabric-cicd/issues/518))
- Fix deployment order to publish Mirrored Database before Lakehouse ([#482](https://github.com/microsoft/fabric-cicd/issues/482))

## [v0.1.25](https://pypi.org/project/fabric-cicd/0.1.25) - August 19, 2025

### ⚠️ Breaking Change

- Modify the default for item_types_in_scope and add thorough validation ([#464](https://github.com/microsoft/fabric-cicd/issues/464))

### ✨ New Functionality

- Add new experimental feature flag to enable selective deployment ([#384](https://github.com/microsoft/fabric-cicd/issues/384))
- Support "ALL" environment concept in parameterization ([#320](https://github.com/microsoft/fabric-cicd/issues/320))

### 📝 Documentation Update

- Enhance Overview section in Parameterization docs ([#495](https://github.com/microsoft/fabric-cicd/issues/495))

### ⚡ Additional Optimizations

- Eliminate ACCEPTED_ITEM_TYPES_NON_UPN constant and unify with ACCEPTED_ITEM_TYPES ([#477](https://github.com/microsoft/fabric-cicd/issues/477))
- Add comprehensive GitHub Copilot instructions for effective codebase development ([#468](https://github.com/microsoft/fabric-cicd/issues/468))

### 🔧 Bug Fix

- Add feature flags and warnings for Warehouse, SQL Database, and Eventhouse unpublish operations ([#483](https://github.com/microsoft/fabric-cicd/issues/483))
- Fix code formatting inconsistencies in fabric_workspace unit test ([#474](https://github.com/microsoft/fabric-cicd/issues/474))
- Fix KeyError when deploying Reports with Semantic Model dependencies in Report-only scope case ([#278](https://github.com/microsoft/fabric-cicd/issues/278))

## [v0.1.24](https://pypi.org/project/fabric-cicd/0.1.24) - August 04, 2025

### ⚠️ Breaking Change

- Require parameterization for Dataflow and Semantic Model references in Data Pipeline activities
- Require specific parameterization for deploying a Dataflow that depends on another in the same workspace (see Parameterization docs)

### 📝 Documentation Update

- Improve Parameterization documentation ([#415](https://github.com/microsoft/fabric-cicd/issues/415))

### ⚡ Additional Optimizations

- Support for Eventhouse query URI parameterization ([#414](https://github.com/microsoft/fabric-cicd/issues/414))
- Support for Warehouse SQL endpoint parameterization ([#392](https://github.com/microsoft/fabric-cicd/issues/392))

### 🔧 Bug Fix

- Fix Dataflow/Data Pipeline deployment failures caused by workspace permissions ([#419](https://github.com/microsoft/fabric-cicd/issues/419))
- Prevent duplicate logical ID issue in Report and Semantic Model deployment ([#405](https://github.com/microsoft/fabric-cicd/issues/405))
- Fix deployment of items without assigned capacity ([#402](https://github.com/microsoft/fabric-cicd/issues/402))

## [v0.1.23](https://pypi.org/project/fabric-cicd/0.1.23) - July 08, 2025

### ✨ New Functionality

- New functionalities for GitHub Copilot Agent and PR-to-Issue linking

### 📝 Documentation Update

- Fix formatting and examples in the How to and Examples pages

### 🔧 Bug Fix

- Fix issue with lakehouse shortcuts publishing ([#379](https://github.com/microsoft/fabric-cicd/issues/379))
- Add validation for empty logical IDs to prevent deployment corruption ([#86](https://github.com/microsoft/fabric-cicd/issues/86))
- Fix SQL provision print statement ([#329](https://github.com/microsoft/fabric-cicd/issues/329))
- Rename the error code for reserved item name per updated Microsoft Fabric API ([#388](https://github.com/microsoft/fabric-cicd/issues/388))
- Fix lakehouse exclude_regex to exclude shortcut publishing ([#385](https://github.com/microsoft/fabric-cicd/issues/385))
- Remove max retry limit to handle large deployments ([#299](https://github.com/microsoft/fabric-cicd/issues/299))

## [v0.1.22](https://pypi.org/project/fabric-cicd/0.1.22) - June 25, 2025

### 🆕 New Items Support

- Onboard API for GraphQL item type ([#287](https://github.com/microsoft/fabric-cicd/issues/287))

### 🔧 Bug Fix

- Fix Fabric API call error during dataflow publish ([#352](https://github.com/microsoft/fabric-cicd/issues/352))

### ⚡ Additional Optimizations

- Expanded test coverage to handle folder edge cases ([#358](https://github.com/microsoft/fabric-cicd/issues/358))

## [v0.1.21](https://pypi.org/project/fabric-cicd/0.1.21) - June 18, 2025

### 🔧 Bug Fix

- Fix bug with workspace ID replacement in JSON files for pipeline deployments ([#345](https://github.com/microsoft/fabric-cicd/issues/345))

### ⚡ Additional Optimizations

- Increased max retry for Warehouses and Dataflows

## [v0.1.20](https://pypi.org/project/fabric-cicd/0.1.20) - June 12, 2025

### ✨ New Functionality

- Parameterization support for find_value regex and replace_value variables ([#326](https://github.com/microsoft/fabric-cicd/issues/326))

### 🆕 New Items Support

- Onboard KQL Dashboard item type ([#329](https://github.com/microsoft/fabric-cicd/issues/329))
- Onboard Dataflow Gen2 item type ([#111](https://github.com/microsoft/fabric-cicd/issues/111))

### 🔧 Bug Fix

- Fix bug with deploying environment libraries with special chars ([#336](https://github.com/microsoft/fabric-cicd/issues/336))

### ⚡ Additional Optimizations

- Improved test coverage for subfolder creation/modification ([#211](https://github.com/microsoft/fabric-cicd/issues/211))

## [v0.1.19](https://pypi.org/project/fabric-cicd/0.1.19) - May 21, 2025

### 🆕 New Items Support

- Onboard SQL Database item type (shell-only deployment) ([#301](https://github.com/microsoft/fabric-cicd/issues/301))
- Onboard Warehouse item type (shell-only deployment) ([#204](https://github.com/microsoft/fabric-cicd/issues/204))

### 🔧 Bug Fix

- Fix bug with unpublish workspace folders ([#273](https://github.com/microsoft/fabric-cicd/issues/273))

## [v0.1.18](https://pypi.org/project/fabric-cicd/0.1.18) - May 14, 2025

### 🔧 Bug Fix

- Fix bug with check environment publish state ([#295](https://github.com/microsoft/fabric-cicd/issues/295))

## [v0.1.17](https://pypi.org/project/fabric-cicd/0.1.17) - May 13, 2025

### ⚠️ Breaking Change

- Deprecate old parameter file structure ([#283](https://github.com/microsoft/fabric-cicd/issues/283))

### 🆕 New Items Support

- Onboard CopyJob item type ([#122](https://github.com/microsoft/fabric-cicd/issues/122))
- Onboard Eventstream item type ([#170](https://github.com/microsoft/fabric-cicd/issues/170))
- Onboard Eventhouse/KQL Database item type ([#169](https://github.com/microsoft/fabric-cicd/issues/169))
- Onboard Data Activator item type ([#291](https://github.com/microsoft/fabric-cicd/issues/291))
- Onboard KQL Queryset item type ([#292](https://github.com/microsoft/fabric-cicd/issues/292))

### 🔧 Bug Fix

- Fix post publish operations for skipped items ([#277](https://github.com/microsoft/fabric-cicd/issues/277))

### ⚡ Additional Optimizations

- New function `key_value_replace` for key-based replacement operations in JSON and YAML

### 📝 Documentation Update

- Add publish regex example to demonstrate how to use the `publish_all_items` with regex for excluding item names

## [v0.1.16](https://pypi.org/project/fabric-cicd/0.1.16) - April 25, 2025

### 🔧 Bug Fix

- Fix bug with folder deployment to root ([#255](https://github.com/microsoft/fabric-cicd/issues/255))

### ⚡ Additional Optimizations

- Add Workspace Name in FabricWorkspaceObject ([#200](https://github.com/microsoft/fabric-cicd/issues/200))
- New function to check SQL endpoint provision status ([#226](https://github.com/microsoft/fabric-cicd/issues/226))

### 📝 Documentation Update

- Updated Authentication docs + menu sort order

## [v0.1.15](https://pypi.org/project/fabric-cicd/0.1.15) - April 21, 2025

### 🔧 Bug Fix

- Fix folders moving with every publish ([#236](https://github.com/microsoft/fabric-cicd/issues/236))

### ⚡ Additional Optimizations

- Introduce parallel deployments to reduce publish times ([#237](https://github.com/microsoft/fabric-cicd/issues/237))
- Improvements to check version logic

### 📝 Documentation Update

- Updated Examples section in docs

## [v0.1.14](https://pypi.org/project/fabric-cicd/0.1.14) - April 09, 2025

### ✨ New Functionality

- Optimized & beautified terminal output
- Added changelog to output of old version check

### 🔧 Bug Fix

- Fix workspace folder deployments in root folder ([#221](https://github.com/microsoft/fabric-cicd/issues/221))
- Fix unpublish of workspace folders without publish ([#222](https://github.com/microsoft/fabric-cicd/issues/222))

### ⚡ Additional Optimizations

- Removed Colorama and Colorlog Dependency

## [v0.1.13](https://pypi.org/project/fabric-cicd/0.1.13) - April 07, 2025

### ✨ New Functionality

- Added support for Lakehouse Shortcuts
- New `enable_environment_variable_replacement` feature flag ([#160](https://github.com/microsoft/fabric-cicd/issues/160))

### 🆕 New Items Support

- Onboard Workspace Folders ([#81](https://github.com/microsoft/fabric-cicd/issues/81))
- Onboard Variable Library item type ([#206](https://github.com/microsoft/fabric-cicd/issues/206))

### ⚡ Additional Optimizations

- User-agent now available in API headers ([#207](https://github.com/microsoft/fabric-cicd/issues/207))
- Fixed error log typo in fabric_endpoint

### 🔧 Bug Fix

- Fix break with invalid optional parameters ([#192](https://github.com/microsoft/fabric-cicd/issues/192))
- Fix bug where all workspace ids were not being replaced by parameterization ([#186](https://github.com/microsoft/fabric-cicd/issues/186))

## [v0.1.12](https://pypi.org/project/fabric-cicd/0.1.12) - March 27, 2025

### 🔧 Bug Fix

- Fix constant overwrite failures ([#190](https://github.com/microsoft/fabric-cicd/issues/190))
- Fix bug where all workspace ids were not being replaced ([#186](https://github.com/microsoft/fabric-cicd/issues/186))
- Fix type hints for older versions of Python ([#156](https://github.com/microsoft/fabric-cicd/issues/156))
- Fix accepted item types constant in pre-build

## [v0.1.11](https://pypi.org/project/fabric-cicd/0.1.11) - March 25, 2025

### ⚠️ Breaking Change

- Parameterization refactor introducing a new parameter file structure and parameter file validation functionality ([#113](https://github.com/microsoft/fabric-cicd/issues/113))

### ✨ New Functionality

- Support regex for publish exclusion ([#121](https://github.com/microsoft/fabric-cicd/issues/121))
- Override max retries via constants ([#146](https://github.com/microsoft/fabric-cicd/issues/146))

### 📝 Documentation Update

- Update to [parameterization](https://microsoft.github.io/fabric-cicd/latest/how_to/parameterization/) docs

## [v0.1.10](https://pypi.org/project/fabric-cicd/0.1.10) - March 19, 2025

### ✨ New Functionality

- DataPipeline SPN Support ([#133](https://github.com/microsoft/fabric-cicd/issues/133))

### 🔧 Bug Fix

- Workspace ID replacement in data pipelines ([#164](https://github.com/microsoft/fabric-cicd/issues/164))

### 📝 Documentation Update

- Sample for passing in arguments from Azure DevOps Pipelines

## [v0.1.9](https://pypi.org/project/fabric-cicd/0.1.9) - March 11, 2025

### 🆕 New Items Support

- Support for Mirrored Database item type ([#145](https://github.com/microsoft/fabric-cicd/issues/145))

### ⚡ Additional Optimizations

- Increase reserved name wait time ([#135](https://github.com/microsoft/fabric-cicd/issues/135))

## [v0.1.8](https://pypi.org/project/fabric-cicd/0.1.8) - March 04, 2025

### 🔧 Bug Fix

- Handle null byPath object in report definition file ([#143](https://github.com/microsoft/fabric-cicd/issues/143))
- Support relative directories ([#136](https://github.com/microsoft/fabric-cicd/issues/136)) ([#132](https://github.com/microsoft/fabric-cicd/issues/132))
- Increase special character support ([#134](https://github.com/microsoft/fabric-cicd/issues/134))

### ⚡ Additional Optimizations

- Changelog now available with version check ([#127](https://github.com/microsoft/fabric-cicd/issues/127))

## [v0.1.7](https://pypi.org/project/fabric-cicd/0.1.7) - February 26, 2025

### 🔧 Bug Fix

- Fix special character support in files ([#129](https://github.com/microsoft/fabric-cicd/issues/129))

## [v0.1.6](https://pypi.org/project/fabric-cicd/0.1.6) - February 24, 2025

### 🆕 New Items Support

- Onboard Lakehouse item type ([#116](https://github.com/microsoft/fabric-cicd/issues/116))

### 📝 Documentation Update

- Update example docs ([#25](https://github.com/microsoft/fabric-cicd/issues/25))
- Update find_replace docs ([#110](https://github.com/microsoft/fabric-cicd/issues/110))

### ⚡ Additional Optimizations

- Standardized docstrings to Google format
- Onboard file objects ([#46](https://github.com/microsoft/fabric-cicd/issues/46))
- Leverage UpdateDefinition Flag ([#28](https://github.com/microsoft/fabric-cicd/issues/28))
- Convert repo and workspace dictionaries ([#45](https://github.com/microsoft/fabric-cicd/issues/45))

## [v0.1.5](https://pypi.org/project/fabric-cicd/0.1.5) - February 18, 2025

### 🔧 Bug Fix

- Fix Environment Failure without Public Library ([#103](https://github.com/microsoft/fabric-cicd/issues/103))

### ⚡ Additional Optimizations

- Introduces pytest check for PRs ([#100](https://github.com/microsoft/fabric-cicd/issues/100))

## [v0.1.4](https://pypi.org/project/fabric-cicd/0.1.4) - February 12, 2025

### ✨ New Functionality

- Support Feature Flagging ([#96](https://github.com/microsoft/fabric-cicd/issues/96))

### 🔧 Bug Fix

- Fix Image support in report deployment ([#88](https://github.com/microsoft/fabric-cicd/issues/88))
- Fix Broken README link ([#92](https://github.com/microsoft/fabric-cicd/issues/92))

### ⚡ Additional Optimizations

- Workspace ID replacement improved
- Increased error handling in activate script
- Onboard pytest and coverage
- Improvements to nested dictionaries ([#37](https://github.com/microsoft/fabric-cicd/issues/37))
- Support Python Installed From Windows Store ([#87](https://github.com/microsoft/fabric-cicd/issues/87))

## [v0.1.3](https://pypi.org/project/fabric-cicd/0.1.3) - January 29, 2025

### ✨ New Functionality

- Add PyPI check version to encourage version bumps ([#75](https://github.com/microsoft/fabric-cicd/issues/75))

### 🔧 Bug Fix

- Fix Semantic model initial publish results in None Url error ([#61](https://github.com/microsoft/fabric-cicd/issues/61))
- Fix Integer parsed as float failing in handle_retry for <3.12 python ([#63](https://github.com/microsoft/fabric-cicd/issues/63))
- Fix Default item types fail to unpublish ([#76](https://github.com/microsoft/fabric-cicd/issues/76))
- Fix Items in subfolders are skipped ([#77](https://github.com/microsoft/fabric-cicd/issues/77))

### 📝 Documentation Update

- Update documentation & examples

## [v0.1.2](https://pypi.org/project/fabric-cicd/0.1.2) - January 27, 2025

### ✨ New Functionality

- Introduces max retry and backoff for long running / throttled calls ([#27](https://github.com/microsoft/fabric-cicd/issues/27))

### 🔧 Bug Fix

- Fix Environment publish uses arbitrary wait time ([#50](https://github.com/microsoft/fabric-cicd/issues/50))
- Fix Environment publish doesn't wait for success ([#56](https://github.com/microsoft/fabric-cicd/issues/56))
- Fix Long running operation steps out early for notebook publish ([#58](https://github.com/microsoft/fabric-cicd/issues/58))

## [v0.1.1](https://pypi.org/project/fabric-cicd/0.1.1) - January 23, 2025

### 🔧 Bug Fix

- Fix Environment stuck in publish ([#51](https://github.com/microsoft/fabric-cicd/issues/51))

## [v0.1.0](https://pypi.org/project/fabric-cicd/0.1.0) - January 23, 2025

### ✨ New Functionality

- Initial public preview release
- Supports Notebook, Pipeline, Semantic Model, Report, and Environment deployments
- Supports User and System Identity authentication
- Released to PyPi
- Onboarded to Github Pages
