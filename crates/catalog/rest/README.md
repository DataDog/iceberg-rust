<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
-->

# Apache Iceberg Rest Catalog Official Native Rust Implementation

[![crates.io](https://img.shields.io/crates/v/iceberg.svg)](https://crates.io/crates/iceberg-catalog-rest)
[![docs.rs](https://img.shields.io/docsrs/iceberg.svg)](https://docs.rs/iceberg/latest/iceberg-catalog-rest/)

This crate contains the official Native Rust implementation of Apache Iceberg Rest Catalog.

See the [API documentation](https://docs.rs/iceberg-catalog-rest/latest) for examples and the full API.

## Metrics reporting

Tables returned by the REST catalog inherit a metrics reporter when the server
supports the report-metrics endpoint. Reports are delivered asynchronously after
scan planning finishes successfully. Delivery failures do not fail the scan.
Each table retains a reporter with the contextual authentication session used for
its catalog operation. Reporters share the catalog's HTTP transport and derive
the metrics endpoint from each report's table identifier. Authentication sessions
are responsible for refreshing their credentials; delivery does not derive a new
session from a scan context.

Filter literals are replaced with `(redacted)` before delivery. This uniform
redaction does not implement Java's numeric magnitude, relative date/time, and
string-hash representations.

Set the catalog property `rest-metrics-reporting-enabled` to `false` to disable
REST reporting (the default is `true`). A table or scan can override the inherited
reporter with `with_metrics_reporter`. Dropped or failed planning streams do not
produce reports. Runtime shutdown may cancel pending delivery.
