# Jazz Explorer Discogs App

*(Repository: `TFM-DiscogsApp` – Final Master's Project)*

A data engineering project that extracts jazz album metadata from the Discogs API, processes it with Spark & Delta Lake, and builds a graph database in Neo4j for exploration and visualization.

The goal is to provide an open dataset of jazz albums, musicians, leaders, and labels between 1953 and 1967, with tools to query and visualize musical connections.

## 1. Architecture and Microservices

The project follows a layered architecture. Each microservice has a clear role in the data flow:

- **Source Layer**  
  - **Discogs API**: external source of jazz album data.

- **Ingestion Layer**  
  - **discogs-downloader**: downloads raw data from Discogs API and saves it in the *landing* area of the data lake.

- **Storage Layer (Data Lakehouse)**  
  - Data is organized in four zones: *landing*, *raw*, *bronze*, and *gold*.  
  - We use Delta Lake tables to add ACID transactions and schema management on top of the lake.

- **Processing Layer**  
  - **pipeline**: transforms data from landing to bronze and gold tables, creating clean and structured entities (albums, artists, leaders, labels, works).  
  - **neo4j-loader**: reads the gold tables, builds a graph model, and sends it to Neo4j.

- **Service Layer**  
  - **Neo4j**: graph database that stores the processed jazz data and relationships.

- **Consumption Layer**  
  - **jazz-queries**: REST API to query the graph with predefined endpoints.  
  - **gateway**: reverse proxy to expose the API.  
  - **Neodash**: dashboard tool to visualize the graph and explore connections.  

In this design, the API (`jazz-queries`), the proxy (`gateway`), and the dashboards (`Neodash`) are part of the **consumption layer**, because they are used directly by end users. The **service layer** only contains Neo4j, which serves the data to those tools.