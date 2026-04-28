# Declaration on the Use of Generative AI

**Project:** Epidemiological Data Warehouse and Analytics Platform
**Author:** Anwar Debes

## Purpose of this Declaration

In the spirit of academic honesty and full transparency, this document sets out exactly how, and to what extent, generative artificial intelligence tools were consulted during the preparation of this project. The work submitted, including the architectural design, the database schemas, the ETL and streaming pipelines, the FastAPI service, the React dashboard, the benchmarking harness, and the accompanying written report, is my own. AI was used as a study aid and a sounding board, never as a substitute for the engineering and analytical effort that the project required.

## How AI Was Used

### 1. Concept clarification and intuition building
Several topics covered in the module were unfamiliar to me when I began. Whenever I encountered a concept that I struggled to internalise from the lecture notes or textbooks alone, I asked a generative model to re explain it in the simplest possible terms, often requesting an explanation pitched at the level of a six year old, followed by a more formal restatement. This proved to be an extremely effective study technique for ideas such as the star schema versus the snowflake schema, the document bucket pattern in MongoDB, the property graph model in Neo4j, the role of KRaft mode in Apache Kafka, and the distinction between roll up, drill down, slice and dice in OLAP. Once I understood the intuition, I returned to the primary sources to confirm the technical details and only then committed anything to the project. The benefit of this approach was twofold. First, it shortened the path from confusion to comprehension. Second, it gave me the vocabulary to explain the same ideas in plain English to a non technical reader, which directly improved the clarity of my final report.

### 2. Discussion of design trade offs
During the design phase I used AI as a conversational partner to stress test my own ideas. Typical exchanges included weighing the merits of a single unified backend against the polyglot persistence approach I eventually adopted, considering whether to model time as a separate dimension table or as an embedded attribute, and debating where stream validation logic should live between Kafka producers, ksqlDB, and the API layer. The decisions recorded in the report and visible in the codebase are my own. AI offered alternative perspectives that I then evaluated, accepted, modified, or rejected based on the requirements of the brief and my own judgement.

### 3. Reflection on future improvements
Toward the end of the project I used AI to brainstorm directions for future work, including richer anomaly detection on the streaming layer, the addition of a semantic search layer over disease metadata, the possibility of integrating a vector store for narrative epidemiological reports, and extending the platform to work with the Project Tycho Level 2 dataset to broaden the scope of supported diseases and reporting jurisdictions.

### 4. Proofreading
I asked an AI tool to flag awkward phrasing and obvious typographical errors in drafts of the report. All editorial decisions, rewrites, and final wording were performed by me.

## How AI Was Not Used

AI was not used to write the project for me. The data ingestion pipeline against the Project Tycho Level 1 dataset, the cleaning rules that produced the 473,189 weekly observations, the relational star schema, the MongoDB bucket layout, the Neo4j graph projection, the Kafka and ksqlDB topology, the FastAPI endpoints, the React dashboard, the benchmarking harness, and the written analysis in the report were designed, implemented, debugged, and documented by me. No AI generated code was pasted verbatim into the repository without first being read, understood, adapted to fit the surrounding architecture, and tested. Any code that I could not have written and explained myself was not included.

## Closing Statement

I treated generative AI in the same way I would treat a knowledgeable peer or a well written textbook. It was a resource for learning and for refining my thinking, not a shortcut around the work. The understanding I gained through this project, and my ability to defend every design decision in it, is genuine and my own.

