# Getting Started

- Install Java 22 (Java 23 still have some issues with Lombok's @Builder and till Java includes record `withers`, `.toBuilder()` is our friend)
  - Install `sdkman` (https://sdkman.io/install/)
  - Run `sdk install java 22.0.2-oracle` to install Java
- Install latest Docker Desktop
- Run `./mvnw clean test` from project root directory