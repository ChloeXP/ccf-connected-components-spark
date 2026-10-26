# CCF — Connected Components at Scale with Apache Spark

Re-implementation of the **CCF (Connected Component Finder)** MapReduce algorithm from
Kardes et al., *"CCF: Fast and Scalable Connected Component Computation in MapReduce"*,
in **Apache Spark**, in both **PySpark and Scala**, using both **RDDs and DataFrames**,
plus an experimental comparison on graphs of increasing size.

> Course project: *Distributed processing of massive data with Hadoop, Spark and Kafka*.
> Executive Master in AI & Data Science, Université Paris Dauphine-PSL (Promo 7, 2025–2026).
> Authors: **Laure Boizeau** · **Xuan Peng**

---

## Goals

1. Understand the CCF MapReduce algorithm and implement it in Spark with **RDDs** and **DataFrames**.
2. Provide **Python** and **Scala** implementations.
3. Benchmark RDD vs DataFrame on graphs of growing size (10 → 5.1 M edges).
4. Study the effect of **parallelism** (number of partitions) and Spark memory settings.
5. Check the paper's claims about **CCF-Dedup** and **secondary sorting**.

## The algorithm in a nutshell

**Input:** a list of edges `(u, v)`. **Output:** for each node, the ID of its connected component
(the smallest node ID in that component).

Two jobs run in a loop until no new pair is emitted:

| Job | Map | Reduce |
|---|---|---|
| **CCF-Iterate** | emit `(u, v)` and `(v, u)` | group neighbours by key, find `min`; if `min < key`, emit `(key, min)` and `(v, min)` for every other neighbour, incrementing a global `NewPair` counter |
| **CCF-Dedup** | identity | drop duplicate pairs |

When `NewPair == 0` after an iteration, every node points directly to its component's minimum node.
The number of components is the number of distinct targets.

Three variants are benchmarked:

| Variant | Description |
|---|---|
| `iterate_avec_duplicat` | CCF-Iterate only (no dedup) |
| `iterate_sans_duplicat` | CCF-Iterate + CCF-Dedup ("standard") |
| `iterate_sort_sans_duplicat` | sort the neighbour list, take the first as min, + dedup ("secondary sort") |

## Repository layout

```
.
├── Pipeline_1_python_spark.ipynb      # Python pipeline 1: pandas baseline, PySpark DataFrame (UDF) and RDD, scaling & parallelism tests
├── Pipeline_2_python_spark.ipynb      # Python pipeline 2: PySpark RDD & DataFrame, standard vs secondary sort, full Google / NotreDame runs
├── Pipeline_3_scala_spark.ipynb       # Scala pipeline: DataFrame & RDD, 3 variants, parallelism sweep 1–30, full run
├── Projet_analyse.ipynb               # Loads all CSVs and produces the figures
├── Projet_Python_comp.csv             # Python timings by method / size / parallelism
├── Projet_Scala_Comp.csv              # Scala timings by variant / structure / size
├── Projet_Scala_RDD_test_parall.csv   # Scala RDD timings vs number of partitions (100k edges)
├── Projet_nb_connect_by_group_5M.csv  # Size of each connected component (full Google graph)
├── Synthese_test.csv                  # Full-dataset timings across machines / configs / pipelines
└── Projet_synthese.pdf                # Full project report (French)
```

## Data

From the [Stanford SNAP collection](https://snap.stanford.edu/data/):

| Dataset | Nodes | Edges |
|---|---|---|
| [web-Google](https://snap.stanford.edu/data/web-Google.html) | 875,713 | 5,105,039 |
| [web-NotreDame](https://snap.stanford.edu/data/web-NotreDame.html) | 325,729 | 1,497,134 |

The data files are **not** included. Download `web-Google.txt.gz` / `web-NotreDame.txt.gz`
and place them next to the notebooks (adjust the `path` variables in each notebook if needed).
Scaling tests use the first *N* edges of web-Google, *N* ∈ {10, 10², 10³, 10⁴, 10⁵, 5·10⁵, 10⁶, 2·10⁶, all}.

## Environment

| Setup | Hardware | Tooling | Spark config |
|---|---|---|---|
| Machine 1 | 8 GB RAM, 4 cores (container capped at 6 GB / 3 CPUs) | Docker `jupyter/all-spark-notebook:x86_64-ubuntu-22.04` + Apache Toree (Scala kernel) | `local[*]`, then *Config 1*: `local[6]`, 1 GB driver/executor, `shuffle.partitions=4` |
| Machine 2 | 16 GB RAM, 8 cores | Kaggle Notebook (Python), IntelliJ IDEA + sbt (Scala) | *Config 2*: 12 GB driver/executor, AQE on, `shuffle.partitions=50`, 4 GB off-heap, `network.timeout=1200s` |

Versions: Spark 3.5.0, Scala 2.13.12, Java 11 target. The Scala `build.sbt` needs extra JVM flags
(`--add-opens java.base/...`, `-Xss4m`, `-Xmx8g`) to run Spark on recent JDKs.

## How to run

```bash
# Option A – Docker (Python + Scala notebooks)
docker run -it --rm -p 8888:8888 --memory=6g --cpus=3 \
  -v "$PWD":/home/jovyan/work jupyter/all-spark-notebook:x86_64-ubuntu-22.04
# then install Apache Toree inside the container for the Scala notebook

# Option B – local PySpark
pip install pyspark==3.5.0 pandas matplotlib
jupyter notebook Pipeline_2_python_spark.ipynb
```

Run the pipeline notebooks first (they write the CSVs), then `Projet_analyse.ipynb` for the figures.

## Results

**Correctness (web-Google):** 2,746 connected components found by every implementation.
After CCF, 872,967 edges remain, and 872,967 + 2,746 = 875,713 = number of nodes, as expected
(each non-root node keeps exactly one edge to its root). The largest component holds
855,802 nodes (~97.7%); convergence takes 8–9 iterations.

**Full Google graph, Python, Machine 2 / Pipeline 2:**

| Method | NotreDame (1.5 M edges) | Google (5.1 M edges) |
|---|---|---|
| DataFrame, standard | 2.84 min | **8.81 min** |
| DataFrame, secondary sort | **2.82 min** | 9.42 min |
| RDD, standard | 9.57 min | 27.89 min |
| RDD, secondary sort | 8.77 min | 27.92 min |

### Key findings

- **DataFrame beats RDD in every configuration**: ~3× faster on Machine 2, 4.6–8.5× on Machine 1.
  Catalyst/Tungsten optimisation plus no Python ↔ JVM serialisation for built-in functions.
- **Spark vs plain pandas:** pandas is fastest below ~1k edges (no Spark overhead) but takes about 40 min
  for 100k edges, while PySpark RDD needs about 7 s.
- **Parallelism:** time is lowest when the number of partitions matches the available cores (≈3–5 on Machine 1)
  and rises steadily beyond that, as scheduling and shuffle-merge overhead takes over.
- **CCF-Dedup works:** at 100k edges it halves DataFrame time and cuts RDD time to about a third (Scala).
- **Secondary sort:** no measurable gain at this graph size, consistent with the paper.

## Limitations & lessons learned

- The first Scala "DataFrame" version `collect()`s the data and iterates on the driver with Scala collections.
  It is fast on small inputs but **not distributed**, so its comparison with the Scala RDD version isn't
  like-for-like. A distributed rewrite exists but is slower and did not finish on the full graph.
- The Scala RDD full run (~80 min) is ~10× slower than PySpark. A likely cause is appending to an
  **immutable `Array` (`:+=`) inside the reducer**, which is O(n²) on the giant component; switching to
  `ArrayBuffer`/iterators should help. This has not been checked by a re-run.
- Iterations do not `persist`/`checkpoint` the RDD, so lineage grows each round and accumulators updated inside
  transformations can over-count on recomputation.
- "Secondary sort" is implemented as an in-memory `sorted()` after `groupByKey`, not a true shuffle-level
  secondary sort (`repartitionAndSortWithinPartitions`).
- All runs used local mode on one machine, not a multi-node cluster. Full-dataset runs were executed once,
  and environments differ across machines, so cross-machine numbers should be read with care.

## Possible next steps

- Validate against GraphFrames `connectedComponents()` / NetworkX on small graphs.
- Add `persist()` + periodic `checkpoint()` per iteration.
- Fix the Scala reducer and redo a fair Python vs Scala comparison on identical settings.
- Run on a real cluster (Databricks / course cluster) and larger graphs (e.g. LiveJournal, Friendster).
- Investigate skew from the giant component (e.g. key salting).

## Reference

H. Kardes, S. Agrawal, X. Wang, A. Sun. *CCF: Fast and Scalable Connected Component Computation in MapReduce.*
inome Inc. — https://www.cse.unr.edu/~hkardes/pdfs/ccf.pdf
