# OpenAIRE Graph with Apache Spark: a Distributed Computing case study

This repository contains a Jupyter notebook used in the **Distributed Computing** course at the University of Pisa.
It is a hands-on case study that shows how to analyse the [OpenAIRE Graph](https://graph.openaire.eu/) with the [Apache Spark](https://spark.apache.org/) framework (through `PySpark`).

## What is the OpenAIRE Graph?

The OpenAIRE Graph is an Open Access dataset of metadata about research products (publications, datasets, software and other research products) linked to the other entities of the research ecosystem: organisations, projects/grants, data sources and communities.
The relationships between these entities (e.g. *Cites*, *IsSupplementTo*, *HasAuthorInstitution*) make it a large knowledge graph.

The full graph takes several terabytes and needs a cluster to process.
This course uses the **OpenAIRE Beginner's Kit** instead. It is a subset with the research products published between `2024-06-01` and `2024-12-31`, plus every entity and relation connected to them.
The subset is small enough to run on a laptop but still too big to load in memory with plain Python, which is exactly the situation where a distributed framework pays off.

## Why this notebook is useful for the course

The notebook moves from a single-machine approach to a distributed one and makes the reason concrete:

1. **The memory wall.** It loads one part file with `pandas`, then shows that loading the whole publication set makes the kernel run out of memory. Watch it happen with `htop`.
2. **Creating a Spark session.** It runs Spark in local mode (`local[8]`), explains the main configuration parameters and how the JVM heap is split (reserved, unified execution/storage and user memory), and estimates the memory available to each task. The Spark UI at <http://localhost:4040> lets you inspect jobs, stages and tasks while the cells run.
3. **The RDD API (MapReduce style).** It reads the raw JSON lines and computes statistics with `map`, `flatMap` and `reduceByKey`:
   - publications per month
   - total authors per month
   - publications per type
4. **The DataFrame / Spark SQL API.** It loads every entity of the graph and answers research questions with declarative queries:
   - counting the entities of each type and plotting them with Plotly
   - inspecting the data model of a publication
   - keyword search in titles
   - publications per month and per language
   - the distribution of relation types
5. **Graph-style queries with joins.** It works on the `relation` table:
   - the most cited products, with joins between publications and relations
   - the top journals by number of publications
   - Open Access status by year, using a `union` of all research product types
6. **A data engineering task.** It extracts all the authors, writes them back to disk and starts a naive author deduplication. This leads into the shuffle cost of `groupBy`.

Along the way you see the core ideas of distributed computing in practice: data partitioning, lazy evaluation, transformations vs. actions, shuffles, and the trade-off between the low-level RDD API and the optimised DataFrame API.

## Repository layout

```
.
├── Notebook.ipynb      # the case-study notebook
├── pyproject.toml      # project dependencies
├── uv.lock             # exact, reproducible dependency versions
├── .python-version     # Python version used by uv (3.12)
└── data/               # OpenAIRE Beginner's Kit (not versioned, see below)
```

## Getting started

### 1. Prerequisites

- **Java 17 or newer.** Spark runs on the JVM, and PySpark 4.x needs Java 17+. Check with:

  ```bash
  java -version
  ```

  If it's missing, install a JDK, for example:
  - macOS: `brew install openjdk@17`
  - Ubuntu/Debian: `sudo apt install openjdk-17-jdk`
  - Windows: install [Eclipse Temurin](https://adoptium.net/) 17 or 21

  Make sure `JAVA_HOME` points to the installation.

- **At least 8 GB of RAM** is recommended.

### 2. Install uv

[uv](https://docs.astral.sh/uv/) is a fast Python package and project manager. It installs the right Python version and every dependency for you.

macOS / Linux:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

Windows (PowerShell):

```powershell
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
```

Alternatives: `brew install uv` (macOS) or `pipx install uv`.

Check the installation (you may need to open a new terminal):

```bash
uv --version
```

### 3. Clone the repository and install the dependencies

```bash
git clone <repository-url>
cd corso_openaire_unipi
uv sync
```

`uv sync` reads `.python-version`, `pyproject.toml` and `uv.lock`. It downloads Python 3.12 if needed, creates a virtual environment in `.venv/` and installs the exact package versions from the lock file (PySpark, pandas, Plotly, Jupyter kernel, ...).

### 4. Download the data

Download the **OpenAIRE Graph Beginner's Kit** from [Zenodo](https://zenodo.org/search?q=openaire%20beginner%27s%20kit) (or use the link given during the course) and extract it into the `data/` folder. The result should look like this:

```
data/
├── communities_infrastructures/
├── dataset/
├── datasource/
├── organization/
├── otherresearchproduct/
├── project/
├── publication/
├── relation/
└── software/
```

Each folder holds gzip-compressed JSON-lines part files (`part-*.json.gz`).
The `data/` folder is listed in `.gitignore`, so it is never committed.

### 5. Run the notebook

Start Jupyter Lab inside the project environment:

```bash
uv run --with jupyter jupyter lab
```

then open `Notebook.ipynb`.

You can also open the folder in **VS Code** (or PyCharm) and select the `.venv` interpreter as the notebook kernel.

Run the cells one at a time with `Shift+Enter`. While Spark jobs run, open the Spark UI at <http://localhost:4040> to see how the work is split into stages and tasks.

> **Tip:** if your machine has less memory or fewer cores, adjust `.master("local[8]")` and `spark.driver.memory` in the Spark session cell.

## Useful links

- OpenAIRE Graph documentation: <https://graph.openaire.eu/docs/>
- OpenAIRE Graph data model: <https://graph.openaire.eu/docs/data-model/>
- PySpark documentation: <https://spark.apache.org/docs/latest/api/python/>
- uv documentation: <https://docs.astral.sh/uv/>
