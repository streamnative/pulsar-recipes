# Pulsar Recipes

[![Build](https://github.com/streamnative/pulsar-recipes/actions/workflows/pr-build-and-test.yml/badge.svg)](https://github.com/streamnative/pulsar-recipes/actions/workflows/pr-build-and-test.yml)

A cookbook of varied use-cases built atop of Apache Pulsar. The purpose of this repository is to demonstrate how Apache
Pulsar primitives can be combined to produce useful and novel behaviours while benefiting from Pulsar's inherent
reliability and scalability.

## Recipes

* [Long Running Tasks](long-running-tasks) — A distributed work queue for long-running tasks.
* [RPC](rpc) — A distributed RPC framework.

## Build

Requirements:

* JDK 11
* Maven 3.8.6+

Common build actions:

|             Action              |                 Command                  |
|---------------------------------|------------------------------------------|
| Full build and test             | `mvn clean verify`                       |
| Skip tests                      | `mvn clean verify -DskipTests`           |
| Skip Jacoco test coverage check | `mvn clean verify -Djacoco.skip`         |
| Skip Checkstyle standards check | `mvn clean verify -Dcheckstyle.skip`     |
| Skip Spotless formatting check  | `mvn clean verify -Dspotless.check.skip` |
| Format code                     | `mvn spotless:apply`                     |
| Generate license headers        | `mvn license:format`                     |

## License

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
