# Distributed Data Processing

Course project for **Big Data Processing and Analytics** at **Politecnico di Torino**.

Java lab exercises using Hadoop MapReduce and Apache Spark. The original code structure is preserved, with small correctness fixes and updated dependencies.

- **Lab1 / Lab1Bonus:** word counts and bigram counts.
- **Lab2:** word-frequency filtering and counters.
- **Lab3:** product co-occurrence and top-100 pairs.
- **Lab4:** user-centered rating normalization.
- **Lab5:** Spark word-frequency analysis.
- **Lab6:** Spark product co-occurrence and top-10 pairs.
- **Lab7:** bike-sharing station criticality using RDDs, with KML placemark output.
- **Lab8_DataFrame / Lab8_Dataset / Lab8_SQL:** bike-sharing analysis using structured Spark APIs.

Exercises are based on course templates and utilities. Lab1Bonus and the three Lab8 variants are copied from the solution folders in the original course materials.

Build with Java 17 and Maven:

    mvn package

Run a Spark exercise locally with Spark 3.5.3 installed:

    ./Lab6/runLocal.sh

Hadoop exercises require Hadoop 3.3.4. Each lab has a local launcher (runLocal.sh) and a YARN/HDFS launcher (run.sh). Pass custom input/output arguments to either script; cluster inputs must already be on HDFS. Output directories must not already exist.
