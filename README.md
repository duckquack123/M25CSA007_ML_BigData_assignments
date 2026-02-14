# CSL7110 — ML/DL/OPS Assignment

## Overview

This project implements large-scale text analysis on **Project Gutenberg** books using **Hadoop MapReduce** and **Apache Spark (PySpark)**. It demonstrates distributed data processing pipelines for word counting, metadata extraction, TF-IDF computation, document similarity, and author influence graph analysis.

---

## Repository Structure

```
.
├── README.md
├── WordCount.java            # Hadoop MapReduce WordCount program
├── question10-12.ipynb       # PySpark notebook (Questions 10–12)
├── sample.txt                # Sample input text for testing
├── 200.txt                   # Example Project Gutenberg book
├── output.txt                # Word count output from MapReduce
└── dataset/                  # 425 Project Gutenberg book files (.txt)
```

---

## Part 1 — Hadoop MapReduce WordCount (`WordCount.java`)

A classic Hadoop MapReduce program that counts word occurrences across the dataset.

### Features
- Converts all text to **lowercase** for case-insensitive counting
- Strips **non-alphanumeric characters** to normalize tokens
- Uses a **Combiner** for local aggregation to reduce shuffle overhead
- Configures a max input split size of 10 KB (`mapreduce.input.fileinputformat.split.maxsize`) for fine-grained parallelism
- Logs total **execution time** upon completion

### How to Run

```bash
# Compile
javac -classpath $(hadoop classpath) -d wordcount_classes WordCount.java
jar -cvf wordcount.jar -C wordcount_classes/ .

# Upload data to HDFS
hdfs dfs -put dataset /user/hadoop/assignment/dataset

# Execute
hadoop jar wordcount.jar WordCount /user/hadoop/assignment/dataset /user/hadoop/assignment/output

# View results
hdfs dfs -cat /user/hadoop/assignment/output/part-r-00000 | head -50
```

---

## Part 2 — PySpark Text Analysis (`question10-12.ipynb`)

An end-to-end PySpark notebook that performs metadata extraction, NLP feature engineering, document similarity computation, and author influence network analysis on the Gutenberg corpus.

### Question 10 — Metadata Extraction & Exploration

| Step | Description |
|------|-------------|
| **Load** | Reads all `.txt` files from HDFS using `sc.wholeTextFiles()` |
| **Extract** | Parses **Title**, **Author**, **Release Date**, **Language**, and **Character Encoding** via regex |
| **Analyze** | Counts books per release year, finds the most common language, and computes average title length |

### Question 11 — TF-IDF & Document Similarity

| Step | Description |
|------|-------------|
| **Clean** | Lowercases text and removes non-alphabetic characters |
| **Tokenize** | Splits text into tokens using `RegexTokenizer` |
| **Stop-word Removal** | Filters out common English stop words with `StopWordsRemover` |
| **TF-IDF** | Computes term frequencies (`HashingTF`, 500 features) and inverse document frequencies (`IDF`, `minDocFreq=4`) |
| **Cosine Similarity** | Calculates pairwise cosine similarity between all book pairs and ranks the most similar documents |
| **Query** | Finds the top-5 most similar books to a specific book (e.g., `10.txt`) |

### Question 12 — Author Influence Graph

| Step | Description |
|------|-------------|
| **Build Graph** | Constructs a directed edge from Author A → Author B if B published within **X years** after A |
| **Experiment** | Tests with window sizes **X = 5** and **X = 2** |
| **In-Degree** | Ranks authors by how many predecessors could have influenced them |
| **Out-Degree** | Ranks authors by how many successors they could have influenced |

---

## Dataset

The `dataset/` directory contains **425 plain-text books** sourced from [Project Gutenberg](https://www.gutenberg.org/). Each file includes standard Gutenberg headers with metadata (title, author, release date, language, encoding) followed by the full book text.

---

## Prerequisites

| Component | Version / Details |
|-----------|-------------------|
| **Java** | JDK 8+ |
| **Hadoop** | 3.x (HDFS + MapReduce) |
| **Apache Spark** | 3.x with PySpark |
| **Python** | 3.8+ |
| **Python Packages** | `pyspark`, `numpy` |

### Spark Configuration (used in notebook)

```python
SparkSession.builder \
    .appName("CSL7110_Assignment") \
    .master("local[*]") \
    .config("spark.driver.memory", "8g") \
    .config("spark.executor.memory", "8g") \
    .config("spark.sql.shuffle.partitions", "20") \
    .getOrCreate()
```

---

## Quick Start

```bash
# 1. Clone the repository
git clone https://github.com/duckquack123/M25CSA007_MLDLOPS_assignment.git
cd M25CSA007_MLDLOPS_assignment

# 2. Start Hadoop services
start-dfs.sh && start-yarn.sh

# 3. Upload dataset to HDFS
hdfs dfs -mkdir -p /user/hadoop/assignment
hdfs dfs -put dataset /user/hadoop/assignment/dataset

# 4. Run MapReduce WordCount (see Part 1 above)

# 5. Open and run the PySpark notebook
jupyter notebook question10-12.ipynb
```

---

## Output

- **`output.txt`** — Tab-separated word frequency counts from the MapReduce job (65,500+ lines)
- **Notebook outputs** — Inline tables and DataFrames showing metadata stats, similarity rankings, and influence graph metrics
