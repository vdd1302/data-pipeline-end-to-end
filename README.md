# data-pipeline-end-to-end
## Cấu trúc phần crawl->raw-storage (tạm thời)
```text
Project1_VuDinhDong/
│
├── config/
│   └── crawl.yaml                  # Cấu hình toàn bộ crawl
│
├── src/
│   ├── db.py                       # Kết nối PostgreSQL
│   ├── models.py                   # RawJobPayload, content hash
│   │
│   ├── crawler/
│   │   ├── topcv.py                # Selenium discovery + detail
│   │   └── parser.py               # HTML → RawJobPayload
│   │
│   ├── raw_storage/
│   │   └── repository.py           # Toàn bộ SQL cho raw.*
│   │
│   ├── pipeline/
│   │   ├── config.py               # Load/validate crawl.yaml
│   │   └── runner.py               # Điều phối và resume
│   │
│   └── cleaning/                   # Giữ nguyên code hiện tại
│       └── ...
│
├── scripts/
│   ├── crawl.py                    # Một CLI chính
│   └── run_topcv_cleaning.py       # Giữ nguyên
│
├── migrations/
│   └── raw/
│       ├── 001_baseline.sql
│       └── 002_crawl_reliability.sql
│
├── tests/
│   ├── test_crawler.py
│   ├── test_raw_storage.py
│   ├── test_crawl_pipeline.py
│   └── ... existing tests
│
├── docs/
│   ├── RAW_STORAGE.md
│   └── CRAWLER.md
│
├── archive/
│   └── legacy_scripts/            # Demo cũ (tạm giữ)
│
├── .env.example
├── .gitignore
├── requirements.txt
└── README.md
```
