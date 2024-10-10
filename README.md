# Getting Started

- Install Java 22 (Java 23 still have some issues with Lombok's @Builder and till Java includes record `withers`, `.toBuilder()` is our friend)
  - Install `sdkman` (https://sdkman.io/install/)
  - Run `sdk install java 22.0.2-oracle` to install Java
- Install latest Docker Desktop
- Run `./mvnw clean test` from project root directory

# What it does

Calculates in a streaming fashion the current top 10 rated movies, with a minimum of 500 votes, based on the following rating function:

`(numVotes/averageNumberOfVotes) * averageRating`

- Source topic: titlevote (where individual votes are published)
- Sink topic: titleranking (where the current top10Movies are published)