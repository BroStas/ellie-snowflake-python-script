# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0] - 2025-03-28

### Added
- Initial release of Snowflake to Ellie Transfer Tool
- Support for transferring Snowflake schema information to Ellie
- Options to include or exclude views
- Standard and PrivateLink connection modes for Snowflake
- Support for direct URL linking to created models in Ellie
- Debug mode for API response inspection
- Comprehensive error handling and user guidance

### Known Issues
- No automatic relationship detection for tables without foreign key constraints
- Large schemas with many tables may take considerable time to transfer 