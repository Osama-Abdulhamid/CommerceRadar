# HDFS Local Prototype

## Deployment

Apache Hadoop 3.4.1 runs as two Docker Compose services:
- namenode: filesystem metadata and block locations.
- datanode: file blocks.

Both services use the commerceradar network.
Replication is 1 for this single-DataNode prototype.
There is no node redundancy or high availability.

Services run as root for compatibility with the tested
Windows-drive bind mounts. This is a local development setup.
HDFS authentication is simple, without Kerberos.

## Configuration and Persistent Storage

Configuration:
- infra/hdfs/core-site.xml
- infra/hdfs/hdfs-site.xml

Environment variables in .env:
- HADOOP_IMAGE=apache/hadoop:3.4.1
- COMMERCE_DATA_ROOT=/mnt/e/CommerceRadarData
- HDFS_STORAGE_ROOT=/mnt/e/CommerceRadarData/hdfs
- HDFS_UI_PORT=9870

Each teammate must set paths appropriate to their machine
and create the namenode and datanode storage directories.

NameNode storage is mounted at /data/name.
DataNode storage is mounted at /data/data.
Both are persisted on E in the verified environment.

The NameNode is formatted only if current/VERSION is absent
and its storage directory is empty. Unexpected existing
contents cause startup to fail rather than being overwritten.

The NameNode also sees COMMERCE_DATA_ROOT at /external,
read-only, to import existing files.

## Start and Inspect

From the repository root:

```bash
docker compose config -q
docker compose up -d namenode datanode
docker compose exec namenode hdfs dfsadmin -report
```

Expected: Live datanodes (1).
Windows UI: http://localhost:9870
Internal endpoint: hdfs://namenode:8020

Spark containers must join the Compose network and load
core-site.xml and hdfs-site.xml.

## Data Lake Layout

- /commerceradar/raw/wdc: original source data.
- /commerceradar/cleaned/wdc: cleaned Parquet.
- /commerceradar/curated/wdc: future analytical outputs.
- /commerceradar/tests: verification files.

## Verified Results

- HDFS file write and read succeeded.
- The same file was readable after restarting both services.
- Spark read 1,000 raw records from HDFS, cleaned them,
  wrote Parquet to HDFS and read it back.
- Full raw archive imported: 3,793,679,127 bytes.
- Full cleaned output imported: 66 Parquet files and _SUCCESS.
- Cleaned directory size: 8,083,144,496 bytes.
- Spark counted 16,451,499 records in the HDFS Parquet output.
- fsck /commerceradar: HEALTHY.
- Validated blocks: 98.
- Missing blocks: 0.
- Corrupt blocks: 0.

The full cleaning job ran on local bind mounts before import.
Cleaning directly through HDFS was tested on the small sample.
The full embedded JSON output passed schema validation before
import. The HDFS record count was verified after import.
A cryptographic comparison of source and destination was not
performed.

## Import Existing Data

Only run if destinations do not already exist:

```bash
docker compose exec namenode hdfs dfs -put /external/raw/wdc/offers_corpus_english_v2_non_norm.json.gz /commerceradar/raw/wdc/
docker compose exec namenode hdfs dfs -put /external/processed/wdc_full_cleaned_v1 /commerceradar/cleaned/wdc/
```

Import retains the local source files.
Access HDFS files through hdfs dfs commands.

## Verify

```bash
docker compose exec namenode hdfs dfsadmin -report
docker compose exec namenode hdfs dfs -stat '%b bytes | %n' /commerceradar/raw/wdc/offers_corpus_english_v2_non_norm.json.gz
docker compose exec namenode hdfs dfs -count /commerceradar/cleaned/wdc/wdc_full_cleaned_v1
docker compose exec namenode hdfs fsck /commerceradar
```

## Safe Shutdown

Finish active transfers and Spark jobs, then run:

```bash
docker compose stop -t 60
```

Quit Docker Desktop after services stop.
Do not delete the persistent storage directories.
Git stores configuration and code, not HDFS data.
Copies on the same disk do not protect against disk failure.

## Next Session

- Configure the streaming data source.
- Prepare team handoff and review the branch for merging.
- Continue with ClickHouse integration and analytical features.

Historical aggregations, product matching and production
security remain future work.
