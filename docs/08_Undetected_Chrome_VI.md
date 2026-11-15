# 08 — Transport `undetected-chromedriver` (tùy chọn, từ 13/11/2026)

Đây là quyết định của người vận hành ngày 13/11/2026, đi ngược nguyên tắc "không che giấu automation" ở
`README.md` và `README_TopCV_Crawl_Recovery_Partial_Success.md`. Mặc định của dự án **không đổi**: mọi file
YAML vẫn chạy Selenium thường. Transport này chỉ chạy khi được chọn rõ ràng.

Lưu ý trước khi dùng: TopCV đã trả 403 và trang "Sorry, you have been blocked" cho crawler này nhiều lần.
Dùng transport này là tiếp tục thu thập dù nguồn đã từ chối trình duyệt tự động; người vận hành tự chịu trách
nhiệm với điều khoản sử dụng của TopCV. Giữ khối lượng nhỏ và nhịp chậm như cấu hình hiện tại.

## 1. Nó khác gì so với hai cách cũ

Đo trên Chrome 153 bằng một trang kiểm tra cục bộ:

| Cách chạy | `navigator.webdriver` | Thuộc tính `cdc_…` trong trang |
|---|---|---|
| Selenium mở Chrome (cách gốc) | `true` | có (7 thuộc tính, từ lúc trang bắt đầu tải) |
| Selenium gắn vào Chrome cổng 9222 (`crawl_1_scope.ps1`) | `false` | **vẫn có** |
| `undetected-chromedriver` | `false` | không |

Ngoài ra transport này tải trang khi **không có driver nào gắn vào Chrome**: lên lịch điều hướng trong trang,
tắt chromedriver, để trang tự tải, rồi mới gắn lại để đọc DOM. Trang chi tiết mở ngay trong tab hiện tại
(không mở tab mới), nên request mang `Referer` của trang trước như khi duyệt bình thường.

Chưa có gì bảo đảm cách này qua được Cloudflare của TopCV: chưa chạy thử trên trang thật.

## 2. Cài đặt (một lần)

```powershell
& "<python.exe đang dùng cho dự án>" -m pip install -r requirements.txt
```

`requirements.txt` đã thêm `undetected-chromedriver` và `setuptools` (Python 3.12+ cần `setuptools` vì
thư viện còn dùng `distutils`). Lần chạy đầu thư viện tự tải chromedriver khớp phiên bản Chrome trên máy.

## 3. Chạy thủ công trên Windows

```powershell
.\crawl_uc.ps1 -DetailOnly -MaxNavigations 1    # phép thử nhỏ nhất: mở đúng 1 job detail đang chờ
.\crawl_uc.ps1 -DetailOnly -MaxNavigations 5    # nếu bước trên qua
.\crawl_uc.ps1                                  # 1 scope: 1 trang listing + các job detail của nó
.\crawl_uc.ps1 -Sessions 10 -PauseSeconds 120   # nhiều phiên liên tiếp
```

- Không mở Chrome cổng 9222 trước; script tự bỏ biến `TOPCV_CHROME_DEBUGGER_ADDRESS`.
- Chrome dùng profile riêng: `browser_profile/topcv_windows_pilot/uc-profile`. Không dùng Chrome đó cho việc khác
  và đóng nó trước khi chạy lại.
- Script **không tự mở khóa host**. Nếu kết quả là `SOURCE_BLOCKED`, mở topcv.vn bằng Chrome thường để kiểm tra,
  rồi chạy lại với `-ReviewHost`.
- Gặp trang xác minh: crawler chờ tối đa 20 giây cho trình duyệt tự qua; nếu không qua và có
  `--allow-manual-verification` (script luôn bật) thì terminal đổ chuông để bạn tự xác minh trong cửa sổ Chrome.
  Trong lúc chờ, driver được tháo ra và chỉ gắn lại vài giây một lần để kiểm tra.
- Gặp trang chặn cứng (403 "Sorry, you have been blocked"): dừng, khóa host, chờ review — như trước.

Lệnh tương đương không qua script:

```powershell
python -u -m scripts.run_market_pipeline crawl --config config/crawl.windows-pilot.yaml `
  --browser-driver undetected --allow-manual-verification --max-scopes 1
```

## 4. Tham số

| Cách đặt | Ý nghĩa | Mặc định |
|---|---|---|
| `--browser-driver undetected` hoặc `TOPCV_BROWSER_DRIVER=undetected` | chọn transport | `selenium` |
| `TOPCV_UC_SETTLE_SECONDS` (0–30) | số giây để trang tự tải khi không gắn driver; `0` = dùng `driver.get` thường | `6` |
| `TOPCV_UC_CHALLENGE_GRACE_SECONDS` (0–120) | chờ trang xác minh tự qua trước khi coi là bị chặn | `20` |

Nhịp crawl (`delay_between_jobs_seconds`, `min_navigation_interval_seconds`, …) vẫn lấy từ YAML và không đổi.
Mỗi lần điều hướng mất thêm khoảng 8 giây so với trước.

## 5. Bật trong Airflow/Docker (chỉ sau khi chạy thủ công ổn)

1. Thêm vào `.env.market`:
   ```
   TOPCV_AIRFLOW_CRAWL_ENABLED=YES
   TOPCV_BROWSER_DRIVER=undetected
   TOPCV_AIRFLOW_CRAWL_ARGS=--detail-first
   ```
   `config/crawl.airflow.yaml` chỉ thu listing; `--detail-first` bật thêm job detail. Có thể thêm
   `--max-navigations N` vào cùng biến này.
2. Build lại image (requirements đã đổi) và khởi động lại stack:
   `docker compose --env-file .env.market -f airflow/docker-compose.market.yml up -d --build`
3. Unpause `topcv_scopes_recovery` (chạy phút :10 và :40) hoặc để `topcv_market_pipeline` chạy lúc 19:00.

Trong container transport này cần Xvfb (`market-scheduler` đã chạy `Xvfb :99`); không có màn hình ảo thì nó
từ chối khởi động thay vì chạy headless. Chromedriver hệ thống được sao chép sang `/tmp/topcv-uc-driver` để vá,
file gốc không bị sửa. `undetected-chromedriver` luôn chạy Chromium với `--no-sandbox`.

Phần Docker chưa được build và chạy thử; Chromium trên Linux dưới Xvfb có dấu vân tay khác Chrome trên Windows
nên khả năng bị chặn có thể cao hơn chạy thủ công.

## 6. Đã kiểm tra gì

- `tests/test_undetected_transport.py` (17 test, không cần Chrome hay mạng).
- Chạy thật với Chrome 153 trên Linux + Xvfb, đọc các trang listing/detail TopCV mà crawler đã lưu trong Raw
  qua một máy chủ cục bộ: listing 50/50 thẻ, parse detail, 403 chặn cứng, 404, trang xác minh tự qua, chờ xác minh
  thủ công, Chrome bị đóng giữa chừng.
- Chưa kiểm tra: trang TopCV thật, Windows, image Docker.
