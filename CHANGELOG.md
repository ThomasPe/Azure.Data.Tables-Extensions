# Change Log
All notable changes to this project will be documented in this file.
 
The format is based on [Keep a Changelog](http://keepachangelog.com/)
and this project adheres to [Semantic Versioning](http://semver.org/).
 
## [2.0.1](https://github.com/ThomasPe/Azure.Data.Tables-Extensions/compare/Medienstudio.Azure.Data.Tables.Extensions-v2.0.0...Medienstudio.Azure.Data.Tables.Extensions-v2.0.1) (2026-08-08)


### Bug Fixes

* publish job was silently skipped on workflow_dispatch ([15cbca0](https://github.com/ThomasPe/Azure.Data.Tables-Extensions/commit/15cbca0e54d17f3d0f4219bf1caef5f36debe16f))
* publish job was silently skipped on workflow_dispatch ([e582f1a](https://github.com/ThomasPe/Azure.Data.Tables-Extensions/commit/e582f1a8abc944d1c2aafe80f30b9c5d6ead974a))

## [2.0.0] - 2026-08-08

### Added
- CSV export schema support.
- DI-friendly logging for CSV and table operations.

### Changed
- Updated dependencies to their latest stable versions.
- Optimized table extension batching and fixed filter escaping.
- Switched release automation to [release-please](https://github.com/googleapis/release-please) for version bumping and changelog generation.
- Fixed CI to run reliably against Azurite (`--skipApiVersionCheck`).

### Fixed
- Fixed CSV round-trip values.
- CSV import duplicate-column detection now actually throws instead of silently succeeding.
- Invalid `Timestamp` values encountered during CSV import are now logged and wrapped instead of failing ungracefully.

### Security
- Removed PartitionKey values from log messages to avoid potential PII exposure.

## [1.5.0] - 2025-11-23

### Changed
- Migrated the solution to .NET 10 (SDK, build, and publish workflows).
- Updated CsvHelper and Tables.Extensions package versions.
- Enhanced test project dependencies.
- Added documentation to helper classes.

## [1.4.2] - 2025-11-23

### Changed
- Updated CSV package dependencies.
- General project cleanup.

## [1.4.1] - 2025-04-02

### Changed
- Minor CSV package project cleanup and dependency reference update.

## [1.4.0] - 2025-04-02
  
  Dependencies updated for libraries and test projects.
  Updated for .NET 8.0, dropped support for netstandard2.1.

### Changed
- Changed IList to List in all methods that return a list of entities.
- Renamed CountEntitiesAsync, added PartitionKey overload.
 
## [1.3.1] - 2024-03-13
  
  Dependencies updated for libraries and test projects.
 

### Changed

- Updated Azure.Data.Tables to version 12.8.3  
- Updated CsvHelper to version 31.0.2
