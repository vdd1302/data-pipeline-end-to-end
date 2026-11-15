# Hướng dẫn chạy và kiểm thử lập lịch end-to-end trên Windows

Áp dụng cho bản mã nguồn ngày 08/11/2026, PowerShell, Python 3.12, Docker Desktop và Airflow 3.1.8 trong repository này. Đây là hướng dẫn nghiệm thu live có giám sát trước khi vận hành production. Các lệnh dưới đây chưa được thực thi live trong phiên hỗ trợ này.

Thực hiện lần lượt từ thư mục gốc dự án; nếu một lệnh báo lỗi hoặc trả exit code khác 0, xử lý lỗi trước khi sang bước tiếp theo. Các lệnh Python gọi trực tiếp `.\.venv312\Scripts\python.exe`. Các biến `$DC`, `$RunId`, `$BI` chỉ tồn tại trong cửa sổ PowerShell đang dùng.

Luồng cần chứng minh:

```mermaid
flowchart LR
  A[Airflow tự kích hoạt lúc 19:00 UTC+7] --> C[Crawl 10 vị trí x 2 thành phố]
  C --> R[(topcv_raw_storage / raw)]
  R --> S[(market / staging)]
  S --> W[(market / market_dw)]
  W --> Q[Kiểm tra chất lượng và đủ 20 scope]
  W --> B[Superset nếu nghiệm thu cả BI]
```

| Thành phần | Địa chỉ khi chạy trên máy Windows |
| --- | --- |
| Raw PostgreSQL do bạn cung cấp | Theo `.env`, ví dụ `127.0.0.1:5432/topcv_raw_storage` |
| PostgreSQL do Compose tạo, chứa DWH và Airflow metadata | `127.0.0.1:55440/market` |
| Airflow UI | `http://localhost:8096` |
| Superset, triển khai riêng nếu cần | `http://localhost:8090` |

**1. Chọn môi trường kiểm thử và kiểm tra công cụ**

Lần đầu nên dùng database/volume riêng dành cho kiểm thử live. Nếu đã có dữ liệu ở database `market`, Raw hoặc volume `topcv-market`, sao lưu và thử phục hồi sang môi trường khác trước; không coi chúng là môi trường mới. Khi có schema cũ, kiểm tra contract trên bản sao trước khi chạy `initialize`. Lệnh `initialize` tạo/bổ sung schema, không phải công cụ migration tự động cho mọi phiên bản cũ.

Mở Docker Desktop, dùng Linux containers. Dành tối thiểu 4 GB RAM cho Docker; nên có thêm dung lượng cho Chromium. Mở PowerShell trong thư mục dự án:

```powershell
Set-Location 'C:\Users\Admin\Downloads\Project1_VuDinhDong_Hardened_2026-11-08\Project1_VuDinhDong'
py -3.12 --version
docker version
docker compose version
psql --version
```

Kết quả cần thấy: Python 3.12; `docker version` có cả Client và Server. Nếu thiếu Server hoặc báo không kết nối được Engine, xử lý Docker Desktop/quyền truy cập trước. `psql` dùng cho bước 3; có thể dùng pgAdmin để thực hiện thao tác tương đương.

**2. Tạo môi trường Python và chạy lại kiểm thử offline**

```powershell
py -3.12 -m venv .venv312
.\.venv312\Scripts\python.exe --version
.\.venv312\Scripts\python.exe -m pip install -r requirements.txt

if ($env:MARKET_TEST_DSN) {
    throw 'MARKET_TEST_DSN đang được đặt. Dùng cửa sổ PowerShell chưa cấu hình biến này cho bước offline.'
}
.\.venv312\Scripts\python.exe -m unittest discover -s tests -p 'test_*.py' -v
.\.venv312\Scripts\python.exe -m scripts.preflight_release --report artifacts/e2e/preflight-static.json
```

Kết quả cần thấy: không có `FAIL`/`ERROR`; các bài PostgreSQL có thể `skipped` ở bước này. Preflight có `issues: []`, `scope_count: 20`. Trạng thái `PASS_STATIC_ONLY` chưa chứng minh Docker, kết nối DB hoặc live crawl.

Nếu đã dùng bản hướng dẫn trước và lệnh `& $Py ...` báo đường dẫn chứa `\=\Join-Path\...`, biến `$Py` trong terminal đã bị gán sai. Chạy trực tiếp `.\.venv312\Scripts\python.exe -m pip install -r requirements.txt` như trên; không cần xóa hoặc tạo lại môi trường chỉ vì lỗi biến này.

**3. Tạo Raw PostgreSQL và cấu hình kết nối**

Compose Airflow **không tạo Raw database**. Nếu đã có Raw đúng hợp đồng, dùng database đó sau khi xác minh/sao lưu; không chạy lại phần tạo database dưới đây. Nếu cài mới, ví dụ PostgreSQL của bạn nghe ở cổng 5432:

```powershell
psql -h 127.0.0.1 -p 5432 -U postgres -d postgres
```

Trong phiên `psql`, nhập lần lượt; `\password` sẽ hỏi mật khẩu riêng:

```text
CREATE ROLE topcv_raw_app LOGIN;
\password topcv_raw_app
CREATE DATABASE topcv_raw_storage OWNER topcv_raw_app;
\q
```

Trở lại PowerShell, cài schema vào **Raw mới**, khi chưa có crawler/ETL chạy:

```powershell
psql -h 127.0.0.1 -p 5432 -U topcv_raw_app -d topcv_raw_storage -v ON_ERROR_STOP=1 -f .\sql\create_topcv_raw_storage_schema.sql

if (-not (Test-Path .env)) { Copy-Item .env.example .env }
notepad .env
```

Điền dòng sau bằng thông tin thật, chỉ lưu ở máy bạn:

```dotenv
TOPCV_RAW_DATABASE_URL=postgresql://topcv_raw_app:MAT_KHAU_DA_URL_ENCODE@127.0.0.1:5432/topcv_raw_storage
```

Nếu mật khẩu có ký tự đặc biệt, percent-encode phần mật khẩu trong URL. Kiểm tra bằng Python host:

```powershell
.\.venv312\Scripts\python.exe -c "from src.raw_storage.db import get_connection; c=get_connection(read_only=True); print(c.execute('SELECT current_database() AS db, to_regclass(%s) AS runs', ('raw.crawl_runs',)).fetchone()); c.close()"
```

Phải thấy database `topcv_raw_storage` và `runs` không phải `None`. Raw có 6 bảng: `crawl_runs`, `search_tasks`, `search_pages`, `job_discoveries`, `job_observations`, `crawl_errors`.

**4. Sinh cấu hình riêng và khởi động DWH**

```powershell
.\.venv312\Scripts\python.exe -m scripts.prepare_market_runtime
$DC = @('--env-file', '.env.market', '-f', 'airflow/docker-compose.market.yml')
docker compose @DC config --quiet
docker compose @DC build market-scheduler
docker compose @DC up -d --wait market-db
```

`prepare_market_runtime` sinh `.env.market` với mật khẩu ngẫu nhiên và đổi địa chỉ Raw localhost thành `host.docker.internal` cho container. Script giữ nguyên file nếu đã tồn tại: sửa `.env` sau đó sẽ **không tự cập nhật** `TOPCV_RAW_CONTAINER_URL` trong `.env.market`. Kiểm tra/sửa riêng dòng đó nếu bạn đổi kết nối Raw. Không xóa `.env.market` để sinh mật khẩu mới khi volume DB vẫn dùng mật khẩu cũ.

Chỉ với DWH mới đã xác minh, cài schema qua container dùng một lần:

```powershell
docker compose @DC run --rm --no-deps market-scheduler python -m scripts.run_market_pipeline initialize
.\.venv312\Scripts\python.exe -m scripts.preflight_release --database --report artifacts/e2e/preflight-database.json
```

Chạy `initialize` theo cách này để container nhận cả `MARKET_READER_PASSWORD` và tạo role đọc BI. Bản thân lệnh `initialize` không crawl TopCV. Cần thấy `status: initialized`, `null_values: 0`; trong preflight cần `postgres_database.status: PASS`, `tables_verified: 10`, `issues: []`. Trường status ngoài cùng của preflight vẫn có thể là `PASS_STATIC_ONLY`; xem cả kết quả PostgreSQL bên trong.

Nếu báo `schema drift`, dừng triển khai vào DB đó và xử lý migration trên bản sao. Không dùng `--reprocess` để sửa cấu trúc bảng.

**5. Chạy kiểm thử PostgreSQL thật trên database dùng riêng**

```powershell
.\.venv312\Scripts\python.exe -m scripts.run_market_checks --postgres
```

Script kết nối runtime `market`, tạo hoặc dùng database riêng tên **`market_integration_test`**, rồi chạy bộ kiểm thử core và PostgreSQL. Chỉ dùng tên này cho DB có thể xóa dữ liệu: kiểm thử sẽ `TRUNCATE` bảng trong DB test. Trình duyệt vẫn được mô phỏng trong các bài này, chưa phải live E2E.

Kết quả cần thấy `passed: true`, `failures: 0`, `errors: 0`, `postgres_included: true`, đồng thời các bài PostgreSQL không bị bỏ qua vì thiếu DSN. Báo cáo ở `artifacts/market/tests.json`. Bước 2 đã chạy toàn bộ discovery offline; helper này chạy tập core cùng các bài PostgreSQL.

**6. Khởi động Airflow và kiểm tra kết nối từ container**

```powershell
docker compose @DC up -d
docker compose @DC ps -a
docker compose @DC exec -T market-scheduler airflow dags list-import-errors
docker compose @DC exec -T market-scheduler airflow dags list
```

Cần thấy `market-db` và `market-api` healthy; scheduler và dag-processor running. Hai service `market-init`, `market-permissions` kết thúc exit 0 là bình thường. Nếu DAG chưa xuất hiện, chờ một chu kỳ parse rồi kiểm tra lại. Không có lỗi import và thấy đủ 3 DAG; giữ cả 3 paused ở bước này.

Kiểm tra Raw qua đúng đường mạng container sẽ sử dụng:

```powershell
docker compose @DC exec -T market-scheduler python -c "from src.raw_storage.db import get_connection; c=get_connection(read_only=True); print(c.execute('SELECT current_database() AS db, to_regclass(%s) AS runs', ('raw.crawl_runs',)).fetchone()); c.close()"
```

Nếu host kết nối được nhưng container không được: kiểm tra `TOPCV_RAW_CONTAINER_URL`, cấu hình PostgreSQL `listen_addresses`, `pg_hba.conf` và firewall cho đúng mạng Docker. Chỉ cấp truy cập cho mạng cần dùng. Bên trong container, `127.0.0.1:5432` không trỏ về Raw trên Windows.

Mở `http://localhost:8096`, user **`admin`**. Đọc mật khẩu tự sinh tại máy riêng:

```powershell
docker compose @DC exec -T market-api cat /opt/airflow/config/market_auth.json
```

`admin:admin` trong Compose có nghĩa **username:role**, không phải cặp username/password. Không đưa file mật khẩu hoặc `.env.market` vào log/chụp ảnh chia sẻ.

Có thể kiểm tra Chromium trước khi truy cập nguồn, hoàn toàn bằng trang `data:` cục bộ:

```powershell
@'
import os
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from scripts.run_market_pipeline import runtime_config
cfg = runtime_config('config/crawl.airflow.yaml')
o = cfg.selenium.build_chrome_options()
o.set_capability('goog:loggingPrefs', {'performance': 'ALL'})
d = webdriver.Chrome(service=Service(os.environ['CHROMEDRIVER_PATH']), options=o)
try:
    d.get('data:text/html,<title>browser-ok</title>')
    print(d.title)
finally:
    d.quit()
'@ | docker compose @DC exec -T market-scheduler python
```

Kết quả cần thấy `browser-ok`. Phép thử dùng Chrome options/profile/sandbox của collector thật; nếu lỗi ở đây, xử lý môi trường browser trước khi bật live.

**7. Bật quyền chạy live sau khi nguồn đã cho phép**

Mã nguồn yêu cầu đồng thời hai biến bên dưới. Chỉ cấu hình chúng khi bạn đã có căn cứ cho phép thu thập tự động; nhập một mã tham chiếu không bí mật tới căn cứ đó. Hai biến là cổng kỹ thuật, không thay thế sự cho phép từ nguồn.

Mở `.env.market` và thêm/sửa, tránh để nhiều dòng trùng tên:

```powershell
notepad .env.market
```

```dotenv
TOPCV_LIVE_COLLECTION_APPROVED=YES
TOPCV_SOURCE_AUTHORIZATION_REFERENCE=THAY_BANG_THAM_CHIEU_CHO_PHEP_THAT
```

`.env` của dự án chỉ được đọc cho Raw DSN; đặt các biến live ở đó không tự bật được collector. PowerShell environment có thể ghi đè giá trị Compose lấy từ env-file, vì vậy kiểm tra các biến cùng tên nếu giá trị thực tế không đúng.

Áp dụng cấu hình mới bằng recreate; `restart` đơn thuần không nạp env mới:

```powershell
docker compose @DC up -d --force-recreate market-api market-scheduler market-dag-processor
docker compose @DC exec -T market-scheduler python -c "from src.market_pipeline.safety import require_source_approval; require_source_approval(); print('Live gate OK')"
```

Cần thấy `Live gate OK`. Các DAG vẫn paused. Nếu chưa có quyền nguồn, dừng kiểm thử crawl tại đây; bạn vẫn có thể kiểm thử `refresh` trên Raw sẵn có.

**8. Smoke test live một scope trước**

```powershell
docker compose @DC exec -T market-scheduler python -m scripts.run_market_pipeline plan
docker compose @DC exec -T market-scheduler python -m scripts.run_market_pipeline e2e --max-scopes 1
.\.venv312\Scripts\python.exe -m scripts.run_market_pipeline status --report artifacts/e2e/status-after-smoke.json
```

`e2e` chạy crawl rồi refresh, nhưng **không tự chạy task coverage như DAG**. `--max-scopes 1` vẫn tạo 20 dòng queue cho hôm nay và chỉ xử lý tối đa 1 scope đến hạn. Không phải giới hạn một HTTP request hay một tin: cấu hình hiện tại cho phép tối đa 5 trang listing/scope và 15 detail/scope/lượt. Theo dõi kết quả và dữ liệu trước khi chuyển bước.

Nếu nguồn trả 403, 429 hoặc challenge, đọc trạng thái queue/cooldown trước; không trigger liên tục để ép chạy. Cách xử lý có trong bước 13.

**9. Kiểm thử toàn bộ DAG bằng manual trigger**

Kết thúc smoke trước khi chạy bước này. Nên làm xa thời điểm 19:00 để dễ phân biệt run thủ công và run theo lịch. Bật DAG rồi trigger ngay có thể tạo thêm scheduled run đã đến hạn; xem danh sách run trước để tránh nhầm.

```powershell
$RunId = 'live_e2e_' + (Get-Date -Format 'yyyyMMdd_HHmmss')
docker compose @DC exec -T market-scheduler airflow dags unpause topcv_market_pipeline
docker compose @DC exec -T market-scheduler airflow dags trigger -r $RunId topcv_market_pipeline
docker compose @DC exec -T market-scheduler airflow dags list-runs topcv_market_pipeline
docker compose @DC exec -T market-scheduler airflow tasks states-for-dag-run topcv_market_pipeline $RunId
```

Trong UI, chọn đúng run ID, xem Graph/Grid và Logs của 4 task:

```text
crawl_scopes -> refresh_warehouse -> check_nonnull -> check_scope_coverage
```

Trigger một DAG đang paused chỉ để run ở queued; phải unpause để scheduler thực thi. Lệnh `airflow dags test` không thay thế bằng chứng scheduler chạy thật. Task `refresh_warehouse` dùng `all_done`, nên có thể xanh dù crawl lỗi.

Nếu thiếu detail do ngân sách 15 detail/scope/lượt và scope còn `pending`/`retry`, bật **scope recovery** để tiếp tục mỗi 30 phút:

```powershell
docker compose @DC exec -T market-scheduler airflow dags unpause topcv_scopes_recovery
.\.venv312\Scripts\python.exe -m scripts.run_market_pipeline status
```

Giữ `topcv_warehouse_recovery` paused khi đang dùng scope recovery, vì scope recovery đã bao gồm refresh và quality. Nếu chỉ cần nạp Raw có sẵn mà không crawl, dùng warehouse recovery thay thế.

`check_scope_coverage` có thể fail trong lượt đầu khi chưa xong 20 scope. Chỉ khi recovery hoàn tất **trong cùng ngày Việt Nam**, clear riêng task coverage đã fail trong UI để nó thực thi lại kiểm tra. Không chọn Mark success. Recovery DAG xanh riêng nó chưa đủ bằng chứng 20/20 vì không có task coverage.

**10. Đối chiếu dữ liệu từng tầng và lưu bằng chứng**

```powershell
.\.venv312\Scripts\python.exe -m scripts.run_market_pipeline status --report artifacts/e2e/status.json
.\.venv312\Scripts\python.exe -m scripts.run_market_pipeline quality --report artifacts/e2e/quality.json
.\.venv312\Scripts\python.exe -m scripts.run_market_pipeline coverage --report artifacts/e2e/coverage.json
$LASTEXITCODE
```

Coverage cần `scope_count: 20`, `scope_coverage_complete: true`, toàn bộ scope `success`, exit code **0**. Exit code **3** nghĩa chưa đủ coverage. Quality cần `null_values: 0`; kết quả này vẫn có thể đạt trên bảng rỗng.

Mở SQL console trong DW:

```powershell
docker compose @DC exec market-db psql -U market_owner -d market
```

Chạy SQL sau trong `psql`. Ngày dùng UTC+7 rõ ràng, không phụ thuộc timezone máy/session:

```sql
SELECT snapshot_date, status, count(*) AS scopes
FROM market_ops.scope_queue
WHERE snapshot_date = (now() AT TIME ZONE 'Asia/Ho_Chi_Minh')::date
GROUP BY snapshot_date, status ORDER BY status;

SELECT position_id, location_id, status, attempts, failure_count,
       next_retry_at, last_error
FROM market_ops.scope_queue
WHERE snapshot_date = (now() AT TIME ZONE 'Asia/Ho_Chi_Minh')::date
ORDER BY position_id, location_id;

SELECT * FROM market_ops.host_backoff;
SELECT * FROM market_ops.refresh_checkpoint;
SELECT * FROM market_ops.refresh_log ORDER BY refresh_id DESC LIMIT 10;
SELECT * FROM market_ops.mapping_errors ORDER BY raw_job_id DESC LIMIT 20;

SELECT count(*) AS staging_rows FROM staging.job_observations_clean;
SELECT snapshot_date, count(*) AS fact_rows,
       count(DISTINCT posting_natural_key) AS distinct_postings
FROM market_dw.fct_job_snapshot
GROUP BY snapshot_date ORDER BY snapshot_date DESC;

SELECT * FROM market_dw.overview;
SELECT * FROM market_dw.data_quality;

WITH todays AS (
    SELECT raw_run_id FROM market_ops.scope_queue
    WHERE snapshot_date = (now() AT TIME ZONE 'Asia/Ho_Chi_Minh')::date
)
SELECT
    (SELECT count(*) FROM staging.job_observations_clean s
     WHERE s.crawl_run_id IN (SELECT raw_run_id FROM todays)) AS staged,
    (SELECT count(*) FROM market_dw.fct_job_snapshot f
     WHERE f.crawl_run_id IN (SELECT raw_run_id FROM todays)) AS facts,
    (SELECT count(*) FROM staging.job_observations_clean s
     LEFT JOIN market_dw.fct_job_snapshot f ON f.raw_job_id = s.raw_job_id
     WHERE s.crawl_run_id IN (SELECT raw_run_id FROM todays)
       AND f.raw_job_id IS NULL) AS staging_without_fact;

SELECT posting_natural_key, crawl_run_id, count(*)
FROM market_dw.fct_job_snapshot
GROUP BY posting_natural_key, crawl_run_id HAVING count(*) > 1;
```

Kỳ vọng: 20 scope success cho ngày nghiệm thu; không có duplicate theo khóa fact; refresh không có rejected chưa giải thích; mapping errors được điều tra; có fact cho các detail đủ điều kiện đã crawl. Trên dữ liệu mới hợp lệ, `staged = facts`, `staging_without_fact = 0`. Các scope có kết quả tìm kiếm rỗng hợp lệ có thể success mà không tạo fact; cần kiểm tra bằng chứng trang rỗng, không suy từ quality xanh. Nghiệm thu luồng dữ liệu có record cần truy vết được ít nhất một detail mới xuyên suốt các tầng.

Thoát `\q`. Kết nối Raw bằng `psql`/pgAdmin với thông tin ở `.env`, kiểm tra riêng:

```sql
SELECT snapshot_date, run_status, count(*) AS runs
FROM raw.crawl_runs
WHERE snapshot_date = (now() AT TIME ZONE 'Asia/Ho_Chi_Minh')::date
  AND crawler_version = 'topcv-selenium-market-v2'
GROUP BY snapshot_date, run_status;

SELECT r.snapshot_date, count(*) AS current_run_observations
FROM raw.job_observations o JOIN raw.crawl_runs r USING (crawl_run_id)
WHERE r.snapshot_date = (now() AT TIME ZONE 'Asia/Ho_Chi_Minh')::date
  AND r.crawler_version = 'topcv-selenium-market-v2'
GROUP BY r.snapshot_date;
```

Chọn một `raw_job_id` mới để truy vết detail trong Raw, dòng cùng ID trong `staging.job_observations_clean`, và trong `market_dw.fct_job_snapshot`. Không yêu cầu tổng discovery = observation = fact: một tin có thể xuất hiện ở nhiều scope; refresh còn lọc record không đủ điều kiện và ghi rejected. Fact có grain `(posting_natural_key, crawl_run_id)`.

Project được mount read-only trong container. Với `--report`, dùng Python **host** như trên; không yêu cầu container ghi `artifacts/...` bên trong project. Lưu thêm run ID, thời gian, ảnh Grid/Graph và log task đã loại bỏ bí mật.

Khi crawler và các lượt refresh khác đã dừng, kiểm tra nạp lặp an toàn:

```powershell
.\.venv312\Scripts\python.exe -m scripts.run_market_pipeline refresh --report artifacts/e2e/refresh-replay.json
```

Nếu không có Raw mới kể từ lần refresh cuối, kỳ vọng `selected: 0`, `loaded: 0` và số fact không tăng. Không dùng `--reprocess` cho phép thử này.

**11. Chứng minh tự kích hoạt lúc 19:00, không chỉ manual run**

Giữ DAG chính unpaused, Docker Desktop và Raw PostgreSQL chạy, máy không sleep. Trước giờ nghiệm thu:

```powershell
docker compose @DC exec -T market-scheduler airflow dags next-execution -n 3 topcv_market_pipeline
docker compose @DC exec -T market-scheduler airflow config get-value scheduler create_cron_data_intervals
```

Kiểm tra schedule thực tế là `0 19 * * *`, timezone `Asia/Ho_Chi_Minh`. Nếu timestamp hiển thị UTC thì **12:00 UTC = 19:00 Việt Nam**. `next-execution` giúp xem mốc lịch, không phải bằng chứng task đã chạy. Bản này `catchup=False`; không giả định có backfill tự động sau thời gian máy tắt.

Sau 19:00, vào UI hoặc dùng:

```powershell
docker compose @DC exec -T market-scheduler airflow dags list-runs topcv_market_pipeline
```

Phải có run mới có **run_type = scheduled**. Chọn đúng run, lấy ID và kiểm tra 4 task bằng `states-for-dag-run` như bước 9. Ghi thời gian scheduler tạo run, task bắt đầu và kết thúc; có thể có độ trễ xử lý sau 19:00.

Nếu hôm đó đã smoke/manual đủ 20 scope trước 19:00, scheduled crawl có thể không lấy thêm do queue ngày đó đã xong. Lượt này chỉ chứng minh timer/orchestration. Để chứng minh **scheduled live E2E đầy đủ**, quan sát thêm một ngày mới mà queue chưa được xử lý trước 19:00; chỉ bật scope recovery sau khi DAG chính đã bắt đầu nếu cần.

Scope recovery chạy mỗi 30 phút suốt ngày. Nếu để bật qua đêm, nó có thể seed và crawl ngày mới từ trước 19:00. Khi mục tiêu là bắt đầu thu thập đúng 19:00, giữ scope recovery paused trước giờ đó và chỉ bật khi cần hoàn tất queue trong buổi tối. Nếu muốn lịch recovery tự giới hạn khung giờ, cần thay đổi DAG và kiểm thử riêng; bản hiện tại chưa làm việc này.

Mã crawl/status/coverage lấy ngày hiện tại ở Việt Nam, **không lấy ngày từ Airflow logical_date hay dag_run.conf**. Không dùng backfill hoặc clear coverage hôm trước vào hôm sau để xác nhận dữ liệu quá khứ. Công việc chưa xong qua nửa đêm được đóng/expired theo quy tắc snapshot.

Sau một ngày thành công, theo dõi thêm ít nhất một ngày mới để kiểm tra lịch, snapshot mới và sự ổn định. Số ngày này là khuyến nghị nghiệm thu, không phải chứng nhận production tự động.

**12. Nếu cần nghiệm thu cả dashboard Superset**

Đây là phần tùy chọn của luồng BI, tách khỏi Airflow. Compose BI hiện dùng image `apache/superset:6.1.0-dev` và `superset run`; cần nghiệm thu/chuẩn hóa deployment riêng trước khi đưa BI vào production.

```powershell
$BI = @('--env-file', 'bi/superset_isolated/.env', '-f', 'bi/superset_isolated/docker-compose.yml')
.\.venv312\Scripts\python.exe -c "from pathlib import Path; import secrets; p=Path('bi/superset_isolated/.env'); print('Giữ nguyên cấu hình BI' if p.exists() else (p.write_text('SUPERSET_SECRET_KEY='+secrets.token_hex(32)+'\n',encoding='utf-8') and 'Đã tạo cấu hình BI'))"
docker compose @BI up -d
docker compose @BI exec superset superset db upgrade
docker compose @BI exec superset superset fab create-admin --username admin
docker compose @BI exec superset superset init
.\.venv312\Scripts\python.exe -m scripts.publish_market_superset
```

Lệnh tạo admin hỏi các thông tin còn lại trực tiếp; username phải là `admin` vì script publish tìm tài khoản này. Không tạo lại admin nếu đã có. Mở `http://localhost:8090`, đối chiếu dashboard với view `market_dw` và kiểm tra `artifacts/market/superset.json` có `all_charts_passed: true`. Nếu thiếu driver PostgreSQL, xử lý dependency trong image BI rồi kiểm thử lại; image BI này chưa được xác nhận runtime trong bản bàn giao.

Kiểm tra tài khoản BI là `market_reader`, SELECT-only; không dùng `market_owner` cho dashboard. Xem cả render UI, số liệu lương thiếu/đã biết, tin nhiều địa điểm/kỹ năng và ngày đăng. Báo cáo API thành công riêng nó chưa chứng minh mọi biểu đồ hiển thị đúng.

**13. Xử lý lỗi và dừng kiểm thử**

| Hiện tượng | Kiểm tra / xử lý |
| --- | --- |
| `Live TopCV collection is disabled` | Thiếu một trong hai biến live trong environment container; sửa `.env.market` rồi recreate. |
| DAG không xuất hiện / import lỗi | Xem `list-import-errors`, log `market-dag-processor`, build image và dependency. |
| Run chỉ `queued` | DAG còn paused, scheduler/API lỗi hoặc đã có active run; kiểm tra UI và `ps`. |
| `host_cooldown`, HTTP 429/challenge | Xem `market_ops.host_backoff.next_retry_at`; chờ cooldown/Retry-After, không lặp trigger để ép nguồn. |
| `blocked` / `access_denied` | Code có thể đặt host stop tới năm 9999; đây là dừng cứng, không phải retry ngắn. Điều tra quyền truy cập với nguồn trước khi xử lý trạng thái. |
| `pending` sau một lượt | Thường còn detail do ngân sách 15/scope; scope recovery tiếp tục các mục đến hạn. |
| `partial` | Có thể chạm giới hạn 5 trang hoặc pagination chưa xác minh. Recovery không tự chọn lại trạng thái này; đọc `last_error` và Raw stop_reason, xử lý nguyên nhân trước. |
| `exhausted` / `expired` | Recovery không tự chọn lại; xem lỗi và ngày snapshot, không đổi trạng thái hàng loạt sang success. |
| DWH xanh nhưng coverage fail | Kiểm tra từng scope; refresh có thể chạy dù crawl lỗi, quality có thể pass khi không có dữ liệu. |
| `Warehouse refresh already running` | Có refresh khác giữ advisory lock; chờ nó xong, tránh bật hai recovery DAG cùng lúc. |
| Checkpoint lớn hơn Raw max ID | Kiểm tra nhầm nguồn/restore; không xóa checkpoint để che lỗi. |
| `schema drift` | Dừng trên DB hiện tại; kiểm tra contract và migration trên bản sao. |

Lấy log vận hành khi cần:

```powershell
docker compose @DC logs --tail 150 market-init market-api market-scheduler market-dag-processor
```

Một số log Airflow có thể chứa mật khẩu tự sinh; đọc cục bộ và loại bỏ bí mật trước khi chia sẻ. Lỗi chi tiết từng task xem ở UI Logs của đúng run/task.

Tạm dừng tạo công việc mới:

```powershell
docker compose @DC exec -T market-scheduler airflow dags pause topcv_market_pipeline
docker compose @DC exec -T market-scheduler airflow dags pause topcv_scopes_recovery
docker compose @DC exec -T market-scheduler airflow dags pause topcv_warehouse_recovery
```

Pause không hủy task đang chạy. Nếu cần dừng ngay việc thực thi LocalExecutor:

```powershell
docker compose @DC stop market-scheduler
```

Task có thể bị gián đoạn; kiểm tra Raw/queue/checkpoint khi khởi động lại. Muốn dừng toàn bộ stack và giữ volumes:

```powershell
docker compose @DC down
```

Không thêm `-v` khi cần giữ DB, metadata, log và browser state. Để tắt cổng live cho lần khởi động sau, sửa `.env.market` về `TOPCV_LIVE_COLLECTION_APPROVED=NO`, rồi recreate service khi bật lại.

**14. Điều kiện ghi nhận kiểm thử live E2E đạt**

- Offline, PostgreSQL integration, DAG import, kết nối Raw từ container và Chromium đều đạt.
- Có run `scheduled` thực sự sau mốc 19:00; lưu run ID và trạng thái/thời gian của 4 task.
- Ngày đó có 20/20 scope success; nếu kết quả rỗng, có bằng chứng nguồn rỗng hợp lệ.
- Detail của đúng ngày/run đi qua Raw → staging → fact; rejected/mapping errors được giải thích; không có NULL hoặc duplicate vi phạm contract.
- Chạy lại refresh khi không có Raw mới không tạo thêm fact; checkpoint không bị xóa/sửa thủ công.
- Quan sát ngày mới, thử khởi động lại và kiểm tra queue/checkpoint trên môi trường test; có backup và thử restore riêng trước khi vận hành dữ liệu thật lâu dài.
- Nếu nghiệm thu BI, số liệu/API/UI/quyền đọc đều được kiểm tra.

Repository hiện chưa có bằng chứng live 20/20; tài liệu bàn giao ghi 7 route đã xác minh lịch sử và 13 candidate chưa xác minh. Nếu route thực tế hoặc parser lỗi, cần sửa và kiểm thử lại, không thể bảo đảm đạt bằng cách chỉ bật scheduler. Hoàn thành runbook là bằng chứng cho môi trường đã kiểm thử; production còn cần xác nhận vận hành, quản lý truy cập, backup/restore và cảnh báo thực tế.

Cú pháp CLI và cơ chế mật khẩu đã đối chiếu tài liệu chính thức [Airflow 3.1.8 CLI](https://airflow.apache.org/docs/apache-airflow/3.1.8/cli-and-env-variables-ref.html), [Simple auth manager](https://airflow.apache.org/docs/apache-airflow/3.1.8/core-concepts/auth-manager/simple/index.html). Simple auth manager được Airflow dành cho development/testing; khi dùng production cần kiểm soát truy cập bổ sung. Yêu cầu bộ nhớ tham khảo [Airflow Docker](https://airflow.apache.org/docs/apache-airflow/3.1.8/howto/docker-compose/).
