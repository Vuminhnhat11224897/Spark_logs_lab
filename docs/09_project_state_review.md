# Tổng Hợp Hiện Trạng Dự Án

Ngày: 2026-06-22

Tài liệu này tổng hợp hiện trạng của dự án `spark_training` sau các phần việc Raw, Bronze,
Silver, profiling và centralized error registry. Mục tiêu là tạo một mốc review để bạn xem xét
trước khi bước sang giai đoạn cleanup hoặc tái cấu trúc.

## Tóm Tắt Nhanh

Dự án không còn chỉ là scaffold. Hiện tại đã có luồng batch Spark local chạy được từ Raw CSV sang
Bronze Parquet và Silver Parquet, kèm Raw/Bronze profiling, Silver quarantine output, unit tests
tập trung, Docker Compose cho Spark standalone cluster, và một error registry tập trung để quản lý
các lỗi validation có cấu trúc.

Phần mạnh nhất hiện nay là batch pipeline đến Silver. Phần yếu nhất là các module dự kiến cho
tương lai vẫn đang tồn tại như placeholder: Gold, quality orchestration, benchmarks, serving,
streaming, Trino, Flink và warehouse validation. Các placeholder này không nguy hiểm nếu chỉ dùng
để định hướng, nhưng một số job wrapper hiện chỉ in TODO mà vẫn return success. Điều này có thể tạo
cảm giác pipeline đã chạy thành công trong automation trong khi thực tế chưa làm gì.

Hướng cleanup nên làm: giữ code batch đã implement trong main source tree, di chuyển hoặc xóa các
runtime module placeholder cho tới khi chúng được implement, bắt các job chưa làm gì phải fail rõ
ràng, chuẩn hóa config/job bootstrapping, rồi tách module Silver đang lớn thành các đơn vị nhỏ hơn.

## Cấu Trúc Repository Hiện Tại

Cấu trúc source tracked ở mức cao:

```text
spark_training/
├── configs/                # YAML config theo convention, hầu như chưa được load runtime
├── data/                   # marker folders; raw/sample data thật bị gitignore
├── docker/                 # Dockerfile/config placeholder, trừ docker-compose.yml ở root
├── docs/                   # architecture, contracts, runbook, roadmap, security, reviews
├── jobs/                   # job entrypoints
├── scripts/                # submit helpers, cleanup, sample generation
├── sql/                    # SQL warehouse/trino placeholder hoặc contract sớm
├── src/spark_log_lab/      # Python package
├── tests/                  # unit, integration, smoke tests
├── warehouse/              # marker + README; Parquet outputs thật bị gitignore
├── docker-compose.yml      # Spark standalone cluster đang dùng thật
├── pyproject.toml
└── requirements.txt
```

Cấu trúc package hiện tại:

```text
src/spark_log_lab/
├── common/       # paths, config/env loading, Spark session, logging, errors/exceptions, CLI args
├── io/           # Spark read/write helpers
├── schemas/      # Raw, Bronze, Silver schemas; Gold vẫn là placeholder
├── pipelines/    # Bronze và Silver đã implement; Gold placeholder
├── quality/      # profiler, basic quality checks, quality result writer; orchestration chưa xong
├── metadata/     # run context và audit writer; lineage placeholder
├── benchmarks/   # placeholder modules
├── serving/      # placeholder modules
└── streaming/    # placeholder modules
```

Quy mô code ước tính từ workspace hiện tại:

- `src/spark_log_lab`: khoảng 1,667 dòng Python.
- `jobs`, `scripts`, và `tests`: khoảng 1,235 dòng Python.
- Các file tracked của project trước các thay đổi untracked review/spec: khoảng 3,626 dòng.
- Module source lớn nhất là `src/spark_log_lab/pipelines/silver_clean_parquet.py`, khoảng
  415 dòng.

## Những Gì Đã Implement

### Foundation

Đã có:

- Path helpers trong `spark_log_lab.common.paths`.
- Load `.env` và `ProjectConfig` đơn giản trong `spark_log_lab.common.config`.
- Spark session factory trong `spark_log_lab.common.spark`, gồm UTC timezone, AQE, dynamic
  partition overwrite và Spark resource settings từ env.
- Logging helper trong `spark_log_lab.common.logging`.
- Spark CSV/Parquet readers trong `spark_log_lab.io.readers`.
- Spark CSV/Parquet writers trong `spark_log_lab.io.writers`.
- Centralized structured error registry trong `spark_log_lab.common.errors`.
- Structured exception hierarchy trong `spark_log_lab.common.exceptions`.

Ghi chú:

- Các file YAML trong `configs/` hiện mới đóng vai trò convention/documentation. Runtime code chưa
  parse các file này; chủ yếu đang dùng env vars và `.env`.
- `spark_log_lab.common.cli.add_common_pipeline_args` đã có nhưng chưa được các job hiện tại dùng.

### Raw Layer

Đã có:

- Raw schemas cho:
  - `01-log-tracking.csv`
  - `02-purchase-behavior.csv`
- Raw files được đọc dạng string để các giá trị lỗi có thể được inspect ở tầng sau, thay vì bị ép
  kiểu thành null ngay lúc CSV ingestion.
- `jobs/00_1_check_raw_files.py` check header, schema, sample rows, null counts và optional full
  counts.
- `jobs/00_2_profile_raw.py` ghi per-column Raw profile snapshots.

Runtime artifacts đang có local:

- `data/raw/01-log-tracking.csv`
- `data/raw/02-purchase-behavior.csv`
- `results/data_profiles/raw_log_tracking_profile.csv`
- `results/data_profiles/raw_purchase_behavior_profile.csv`

Các file CSV/profile thật đang bị `.gitignore`.

### Bronze Layer

Đã có:

- `spark_log_lab.pipelines.bronze_csv_to_parquet.build_bronze_pipeline`.
- Chuyển Raw CSV sang Bronze Parquet cho cả hai source datasets.
- Parse `event_timestamp`, `event_date`, các purchase date fields, source file, ingest time và
  `batch_id`.
- Bronze schemas cho log tracking và purchase behavior.
- `jobs/01_build_bronze.py`.
- `jobs/01_1_check_bronze.py`.
- `jobs/01_2_profile_bronze.py`.
- Docker submit wrappers cho build/check/profile.

Runtime artifacts đang có local:

- `warehouse/bronze/log_tracking/`
- `warehouse/bronze/purchase_behavior/`
- `results/data_profiles/bronze_log_tracking_profile.csv`
- `results/data_profiles/bronze_purchase_behavior_profile.csv`

Các output này đang bị `.gitignore`.

### Silver Layer

Đã có:

- `spark_log_lab.pipelines.silver_clean_parquet.build_silver_pipeline`.
- Cleaning functions:
  - `clean_log_tracking_to_silver`
  - `clean_purchase_behavior_to_silver`
- Trim string và normalize empty string.
- Parse timestamp/date.
- Cast ID và price.
- Validate event type.
- Tách `category_code` thành `category_l1`, `category_l2`, `category_l3`.
- Warning flags:
  - category code bị thiếu
  - brand bị thiếu
  - cast category ID thất bại
  - price bằng 0 hoặc âm
  - source cohort week mismatch
  - event date mismatch với timestamp
- Hard quarantine rules:
  - thiếu required field
  - timestamp parse failed
  - invalid event type
  - invalid required ID cast
  - invalid purchase price
  - event date parse failed
  - duplicate record
- Deduplication theo shared Silver event key.
- Silver outputs partition theo `event_date`.
- Quarantine output giữ raw fields dạng string để dễ inspect.

Runtime artifacts đang có local:

- `warehouse/silver/log_tracking/`
- `warehouse/silver/purchase_behavior/`
- `warehouse/silver/quarantine/`

Các output này đang bị `.gitignore`.

### Quality Và Profiling

Đã có:

- Data profiler trong `spark_log_lab.quality.profiler`.
- Quality result writer trong `spark_log_lab.quality.result_writer`.
- Basic quality checks:
  - row count greater than zero
  - null-rate check
  - duplicate-count check
- Centralized managed errors đã được dùng cho missing columns và writer validation.

Chưa hoàn thiện:

- `jobs/04_run_quality_checks.py` mới chỉ print TODO và return success.
- Basic quality check modules đã có nhưng chưa được orchestrate trên Silver outputs.
- `quality/business_rules.py` và `quality/warehouse_validation.py` vẫn là placeholders.

### Metadata

Đã có:

- Run context helper với UTC run IDs và timestamps.
- Audit CSV writer.

Chưa hoàn thiện:

- Pipeline jobs chưa ghi audit records một cách nhất quán.
- `metadata/lineage.py` mới là placeholder.

### Docker Và Runtime

Đã có:

- Root `docker-compose.yml` chạy Spark master và hai workers bằng image
  `apache/spark:3.5.1-python3`.
- Submit scripts cho Raw và Bronze jobs.
- Project được volume mount vào `/opt/spark-log-lab`.

Chưa implement:

- Custom Spark image.
- Flink runtime.
- Trino runtime.

Các file trong `docker/spark/`, `docker/flink/`, và `docker/trino/` hiện đang là placeholders.

### Tests

Kết quả verification trong lần review này:

```bash
.venv/bin/python -m pytest -q
25 passed
```

Tests mạnh nhất:

- Silver cleaning behavior.
- Error registry behavior.
- Profile writer behavior.
- Quality result writer behavior.
- Audit writer behavior.
- Path helpers.

Tests yếu:

- Một số tests chỉ verify placeholder modules/files tồn tại.
- `tests/unit/test_transformations.py` import `gold_marts_parquet` dù module này vẫn là
  placeholder.
- `tests/unit/test_sql_contracts.py` check SQL file tồn tại, chưa check behavior của SQL.
- `tests/smoke/test_jobs_smoke.py` check job entrypoint file tồn tại, chưa chạy job behavior.

## Dead Code, Placeholder Code Và Cleanup Candidates

Phần này tách riêng giữa code cần cleanup thật sự và future work đang được đặt sẵn có chủ đích.

### Cleanup Ưu Tiên Cao

| Khu vực | Hiện trạng | Rủi ro | Khuyến nghị |
| --- | --- | --- | --- |
| `jobs/03_build_gold.py` | Print TODO và return `0` | False success | Return non-zero đến khi implement, hoặc bỏ khỏi active smoke expectations |
| `jobs/04_run_quality_checks.py` | Print TODO và return `0` | False success | Wire existing quality checks vào Silver hoặc return non-zero |
| `jobs/05_run_spark_benchmarks.py` | Print TODO và return `0` | False success | Return non-zero đến khi benchmark runner có thật |
| `jobs/06_run_trino_benchmarks.py` | Print TODO và return `0` | False success | Chỉ giữ trong docs/roadmap hoặc return non-zero |
| `jobs/07_start_streaming_demo.py` | Print TODO và return `0` | False success | Chỉ giữ trong docs/roadmap hoặc return non-zero |
| Tests cho TODO jobs | Chỉ check file tồn tại | Tín hiệu yếu | Đổi thành tests assert unfinished jobs fail rõ ràng |

### Placeholder Modules

Các module sau về cơ bản đang là dead source code ở thời điểm hiện tại vì chỉ có docstring hoặc
placeholder text:

- `src/spark_log_lab/pipelines/gold_marts_parquet.py`
- `src/spark_log_lab/schemas/gold.py`
- `src/spark_log_lab/quality/business_rules.py`
- `src/spark_log_lab/quality/warehouse_validation.py`
- `src/spark_log_lab/metadata/lineage.py`
- `src/spark_log_lab/benchmarks/*.py`
- `src/spark_log_lab/serving/*.py`
- `src/spark_log_lab/streaming/*.py`
- `docker/spark/Dockerfile`
- `docker/flink/Dockerfile`
- `docker/trino/config.properties`

Khuyến nghị:

- Nếu phase tiếp theo vẫn là Gold/Silver batch work, hãy bỏ các placeholder này khỏi active source
  và ghi rõ trong `docs/07_roadmap.md`.
- Nếu muốn giữ lại hình dạng tương lai, chuyển chúng vào một khu vực planning rõ ràng như
  `docs/planned/` thay vì để chúng thành runtime modules import được.
- Không nên giữ placeholder modules trong tests như thể chúng đã được implement.

### Helpers Đã Có Nhưng Chưa Được Wire In

| Helper | Trạng thái hiện tại | Khuyến nghị |
| --- | --- | --- |
| `common/cli.py` | Define common args, chưa job nào dùng | Dùng chung cho jobs hoặc xóa đến khi cần |
| `metadata/audit.py` | Writer đã có test, chưa integrate vào pipeline jobs | Wire vào Raw/Bronze/Silver jobs sau khi chốt run-context design |
| `quality/row_count.py` | Basic check đã có, chưa orchestrate | Dùng trong `04_run_quality_checks.py` hoặc giữ private đến khi wire |
| `quality/null_check.py` | Basic check đã có, chưa orchestrate | Dùng trong `04_run_quality_checks.py` hoặc giữ private đến khi wire |
| `quality/duplicate_check.py` | Basic check đã có, chưa orchestrate | Dùng trong `04_run_quality_checks.py` hoặc giữ private đến khi wire |
| `configs/*.yaml` | Runtime code chưa parse | Load YAML config tập trung hoặc ghi rõ là docs/examples |

### Comment Hoặc Naming Đã Cũ

- `src/spark_log_lab/io/readers.py` nói readers "will be implemented" dù các functions đã được
  implement.
- `src/spark_log_lab/pipelines/bronze_csv_to_parquet.py` có module docstring là placeholder nhưng
  bên trong đã có Bronze logic thật.
- Một số jobs tự `sys.path.append("src")`; một số jobs lại phụ thuộc package/PYTHONPATH. Nên chỉ
  giữ một convention.

### Generated Hoặc Local Runtime Artifacts

Các generated files đang tồn tại local và đã bị ignore:

- `__pycache__/` directories trong source, jobs, scripts và tests.
- 87 file `.pyc` được tìm thấy trong source/jobs/scripts/tests lúc review.
- Parquet output trong `warehouse/bronze/` và `warehouse/silver/`.
- CSV profiles trong `results/data_profiles/`.
- Report files trong `results/reports/`.
- Sample files trong `data/samples/`.
- Raw CSV files trong `data/raw/`.

Đây không phải source dead code. Chúng là local runtime artifacts. Nên tiếp tục ignore. Chỉ clean
khi cần workspace sạch.

## Cấu Trúc Mục Tiêu Đề Xuất

Cấu trúc top-level hiện tại ổn. Cleanup chính nên nằm trong `src/spark_log_lab` và `jobs`.

Cấu trúc source nên hướng tới cho stable batch phase tiếp theo:

```text
src/spark_log_lab/
├── common/
│   ├── config.py
│   ├── errors.py
│   ├── exceptions.py
│   ├── logging.py
│   ├── paths.py
│   └── spark.py
├── io/
│   ├── readers.py
│   └── writers.py
├── schemas/
│   ├── raw.py
│   ├── bronze.py
│   └── silver.py
├── pipelines/
│   ├── bronze.py
│   └── silver/
│       ├── clean.py
│       ├── quarantine.py
│       ├── deduplicate.py
│       └── pipeline.py
├── quality/
│   ├── checks.py
│   ├── profiler.py
│   └── result_writer.py
└── metadata/
    ├── audit.py
    └── run_context.py
```

Chỉ thêm các phần sau khi chúng trở thành code thật:

```text
src/spark_log_lab/pipelines/gold/
src/spark_log_lab/benchmarks/
src/spark_log_lab/serving/
src/spark_log_lab/streaming/
```

Cấu trúc job nên hướng tới:

```text
jobs/
├── 00_check_environment.py
├── 00_check_raw_files.py
├── 00_profile_raw.py
├── 01_build_bronze.py
├── 01_check_bronze.py
├── 01_profile_bronze.py
├── 02_build_silver.py
└── 02_check_silver.py
```

Không nên giữ các job `03+` trong active smoke tests cho tới khi chúng làm việc thật.

## Cleanup Plan Đề Xuất

### Phase 1: Làm Repository Trung Thực Hơn

1. Đổi các TODO job wrappers sang return non-zero, hoặc bỏ chúng khỏi active smoke tests.
2. Xóa các import/existence tests cho placeholder modules.
3. Cập nhật stale docstrings trong `io/readers.py` và `pipelines/bronze_csv_to_parquet.py`.
4. Chuẩn hóa job bootstrapping: hoặc package install/PYTHONPATH ở mọi nơi, hoặc một helper pattern
   duy nhất, không trộn lẫn `sys.path.append`.
5. Chạy `scripts/clean_runtime_outputs.sh` khi cần xóa cache directories; tách riêng cleanup data
   và warehouse vì các artifact đó có thể mất công regenerate.

### Phase 2: Thắt Chặt Batch Quality

1. Implement `jobs/04_run_quality_checks.py` thành `02_check_silver.py`.
2. Chạy row count, null-rate, duplicate checks trên Silver outputs.
3. Ghi quality results với centralized error handling.
4. Thêm behavior tests cho quality checks, không chỉ import tests.
5. Quyết định `quality/business_rules.py` sẽ thành real Silver business checks hay sẽ bị xóa.

### Phase 3: Tách Silver Pipeline

1. Chuyển common normalization/casting helpers vào `pipelines/silver/clean.py`.
2. Chuyển hard quarantine construction vào `pipelines/silver/quarantine.py`.
3. Chuyển dedup logic vào `pipelines/silver/deduplicate.py`.
4. Giữ orchestration trong `pipelines/silver/pipeline.py`.
5. Giữ lại public functions hiện tại hoặc update jobs/tests trong cùng một thay đổi.

### Phase 4: Chốt Gold Trước Khi Thêm Platform

1. Implement Gold marts chỉ sau khi Silver quality checks đã ổn định.
2. Không mở rộng Trino/Flink/streaming cho tới khi Gold có tested data contract.
3. Xóa hoặc quarantine platform placeholders cho tới khi runtime path của chúng là thật.

## Kết Luận Review

Dự án đang ở trạng thái tốt cho một batch-engineering training project đến tầng Silver. Đã có đủ
real pipeline code để tiếp tục, nhưng cũng đang mang quá nhiều placeholder cho các phase tương lai.
Bước kỹ thuật tốt nhất tiếp theo không phải thêm nhiều feature mới, mà là làm repository nhỏ và
trung thực hơn: module đã implement thì giữ importable, module chỉ mới dự định thì để trong
docs/roadmap, và job entrypoint không return success cho tới khi nó thực sự làm việc.

