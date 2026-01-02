# [CO5173] Group 11 TikTrend: Nắm bắt trending cho nhà sáng tạo nội dung
<p align="center">
  <img src="images/logotik.png" alt="TendTrick Logo" width="200"/>
</p>

Ứng dụng **Streamlit Dashboard** giúp Creator/Marketer ra quyết định nội dung dựa trên log hashtag TikTok:

- Phân tích **Top Hashtag**, **Momentum**, **Retention**, **Ngành**, **Quốc gia**  
- **Tab 9:** AI Phân tích Kênh & Gợi ý kịch bản (gọi Gemini qua HTTP)  
- **Tab 10:** Phân tích **Promote/Quảng cáo trả phí** (tỷ lệ hashtag được gắn Promote)
---

## 1. Yêu cầu hệ thống

### 1.1. Hệ điều hành

- Windows 10/11 (đã test)
- (Có thể chạy trên macOS / Linux nhưng hướng dẫn dưới đây tập trung vào Windows)

### 1.2. Phần mềm cần cài

1. **Python 3.10+**
   - Tải từ: https://www.python.org/downloads/
   - Khi cài **nhớ tick**:
     - `Add Python to PATH`
2. (Tuỳ chọn nhưng nên có) **Git**
   - Tải từ: https://git-scm.com/downloads

3. Trình duyệt:
   - Chrome / Edge / Firefox đều được (Streamlit sẽ mở trên trình duyệt)

---

## 2. Tải source code về máy

Giả sử bạn lưu project ở Desktop.

### Cách 1 — Clone bằng Git (nếu có Git)

```bash
cd %USERPROFILE%\Desktop
git clone <link-repo-cua-ban> tiktok_trending_webapp
cd tiktok_trending_webapp
```

### Cách 2 — Tải ZIP (nếu không dùng Git)

1. Tải file `.zip` của project từ GitHub/Drive/…  
2. Giải nén vào: `C:\Users\<TênUser>\Desktop\tiktok_trending_webapp`  
3. Mở **Command Prompt / PowerShell** và chạy:

```bash
cd %USERPROFILE%\Desktop\tiktok_trending_webapp
```

Đảm bảo trong thư mục này có file:

- `app.py`
- folder `util/` (chứa `db.py`, `filters.py`, …)
- (các file phụ khác nếu có)

---

## 3. Tạo virtualenv và cài thư viện

> Mục tiêu: không làm bẩn Python global, tất cả chạy trong **.venv** riêng của project.

### 3.1. Tạo virtualenv

Trong thư mục project (`tiktok_trending_webapp`), chạy:

```bash
python -m venv .venv
```

Sau đó **kích hoạt**:

```bash
.\.venv\Scripts\activate
```

Nếu thành công, trước dòng lệnh sẽ có `(venv)` hoặc `(.venv)`.

>  Nếu báo lỗi `activate.ps1 bị chặn` trên PowerShell, chạy:
> ```powershell
> Set-ExecutionPolicy -Scope CurrentUser RemoteSigned
> ```
> rồi thử lại lệnh `.\.venv\Scripts\activate`.

### 3.2. Cài các thư viện Python cần thiết

Nếu bạn đã có `requirements.txt` thì:

```bash
pip install -r requirements.txt
```

Nếu **chưa có** `requirements.txt`, có thể cài tay như sau:

```bash
pip install streamlit==1.39.0
pip install plotly==5.24.1
pip install pandas
pip install requests
pip install databricks-sql-connector
```

Tuỳ project của bạn, có thể cần thêm:

```bash
pip install pyarrow
pip install numpy
```

---

## 4. Kết nối Databricks (cho phần dữ liệu Bronze/Silver/Gold)

Ứng dụng đang dùng hàm `run_sql` trong `util/db.py` để chạy query tới Databricks.

Bạn có 2 cách cấu hình:

### 4.1. Cách 1 — Cấu hình qua biến môi trường (khuyến nghị)

Trong PowerShell/CMD (sau khi `activate` venv), đặt các biến:

```bash
setx DATABRICKS_SERVER_HOSTNAME "<tên workspace>.cloud.databricks.com"
setx DATABRICKS_HTTP_PATH "/sql/1.0/warehouses/<warehouse-id>"
setx DATABRICKS_TOKEN "<PAT-token-cua-ban>"
```

Rồi **mở lại** terminal mới, `cd` vào project, `activate` lại venv.

Sau đó, trong `util/db.py`, bạn có thể viết kiểu:

```python
# util/db.py (ví dụ)
import os
import pandas as pd
from databricks import sql

def run_sql(query: str) -> pd.DataFrame:
    conn = sql.connect(
        server_hostname=os.getenv("DATABRICKS_SERVER_HOSTNAME"),
        http_path=os.getenv("DATABRICKS_HTTP_PATH"),
        access_token=os.getenv("DATABRICKS_TOKEN"),
    )
    try:
        with conn.cursor() as c:
            c.execute(query)
            rows = c.fetchall()
            if not rows:
                return pd.DataFrame()
            cols = [d[0] for d in c.description]
            return pd.DataFrame.from_records(rows, columns=cols)
    finally:
        conn.close()
```

> Lưu ý: Bạn chỉ cần chỉnh `util/db.py` **một lần**, sau đó mọi tab sẽ dùng được.

### 4.2. Cách 2 — Hard-code trực tiếp trong `util/db.py` (dùng cho demo nhanh)

Không khuyến khích cho production, nhưng nếu chỉ demo:

```python
# util/db.py (cách đơn giản)
import pandas as pd
from databricks import sql

def run_sql(query: str) -> pd.DataFrame:
    conn = sql.connect(
        server_hostname="xxx.cloud.databricks.com",
        http_path="/sql/1.0/warehouses/xxxx",
        access_token="dapixxxxxxxx",
    )
    ...
```

---

## 5. Cấu hình Gemini API cho Tab 9 (AI Phân tích Kênh)

Tab 9 dùng Google **Gemini** (qua HTTP `requests`) để sinh text.

### 5.1. Lấy API key

1. Vào: https://aistudio.google.com/
2. Login bằng tài khoản Google.
3. Vào mục **API Keys** → tạo 1 API key mới.
4. Copy key dạng: `AIza...`

### 5.2. Tạo file `secrets.toml` cho Streamlit

Tạo folder `.streamlit` cùng cấp với `app.py`:

```bash
mkdir .streamlit
```

Tạo file: `.streamlit/secrets.toml` với nội dung:

```toml
[gemini]
api_key = "AIzaXXXXXXXXXXXXXXXXXXXXXXXX"
```

> **Quan trọng:**  
> - Không commit file này lên Git public.  
> - Không chia sẻ key cho người khác.

---

## 6. Chạy ứng dụng Streamlit

Tất cả bước sau đều thực hiện trong thư mục project:

1. Mở **Command Prompt / PowerShell**
2. `cd` tới project:

   ```bash
   cd %USERPROFILE%\Desktop\tiktok_trending_webapp
   ```

3. Kích hoạt venv:

   ```bash
   .\.venv\Scripts\activate
   ```

4. Chạy ứng dụng:

   ```bash
   streamlit run app.py
   ```

Nếu đúng, Streamlit sẽ:

- In ra 1 URL kiểu: `http://localhost:8501`
- Tự mở trong trình duyệt. Nếu không, copy đường link và dán vào Chrome/Edge.

---

## 7. Các chức năng chính trong UI

Sau khi mở app, bạn sẽ thấy 10 tab:

1. **🎯 Tìm Ngách (Niche Finder)**  
   - Scatter plot View vs Video  
   - Tìm hashtag **Demand cao – Competition thấp**

2. **🔥 Động lượng Trend (Momentum)**  
   - Tính `view_delta`, `rank_velocity`  
   - Top trend tăng/giảm view, tăng hạng nhanh

3. **⚡ Chiến lược Trend Nhanh (Short-term)**  
   - Phân bố **streak_days** (vòng đời trend)  
   - Số hashtag mới mỗi ngày

4. **🌳 Chiến lược Bền vững (Long-term)**  
   - Top hashtag “sống dai” nhất  
   - Ngành có vòng đời trend lâu

5. **📊 Phân tích Bão hòa Ngành**  
   - Thị phần view theo ngành  
   - Hiệu quả view/video từng ngành

6. **🌍 Phân tích Thị trường QG**  
   - View theo thời gian cho từng quốc gia (stacked area)

7. **🏆 Top 100 Đã Kiểm chứng (Proven Winners)**  
   - Danh sách hashtag top 100 mới nhất

8. **📅 Lập kế hoạch Tuần (Weekly Planner)**  
   - Weekly ranking / best_rank / avg_rank theo tuần

9. **🤖 AI Phân tích Kênh**  
   - Prompt Builder (Mục tiêu, Audience, KPI, Tone, …)  
   - Tự chọn hashtag từ data (Hot/Evergreen/Opportunity/Top100/Weekly)  
   - Gọi Gemini API → trả về phân tích kênh + ý tưởng + kịch bản

10. **📣 Phân tích Promote (Quảng bá trả phí)**  
    - Dùng `is_promoted` từ Silver hoặc `gold.trend_promoted_share`  
    - KPI: tỷ lệ promoted toàn kỳ + 7 ngày gần nhất  
    - Phân loại: Organic / Balanced / Ads-heavy  
    - Biểu đồ % promoted theo thời gian & theo quốc gia  
    - Gợi ý chiến lược phân bổ ngân sách Promote

---

## 8. Lỗi thường gặp & cách xử lý

### 8.1. `streamlit: command not found` / `'streamlit' is not recognized`

- Chưa kích hoạt venv, hoặc Streamlit cài vào Python khác.
- Giải pháp:
  ```bash
  cd %USERPROFILE%\Desktop\tiktok_trending_webapp
  .\.venv\Scripts\activate
  pip install streamlit
  streamlit run app.py
  ```

### 8.2. Lỗi `Please replace use_container_width with width`

- Đây chỉ là **warning** của phiên bản Streamlit mới.  
- Code đã dùng `use_container_width=True` → vẫn chạy bình thường.  
- Có thể bỏ qua khi demo. Khi rảnh có thể đổi theo gợi ý.

### 8.3. Lỗi `SQL error: [UNRESOLVED_COLUMN...]`

- Thường là do:
  - Bảng Gold/Silver chưa đủ cột
  - Sai tên cột so với schema hiện tại
- Giải pháp:
  - Kiểm tra lại bảng trong Databricks.
  - Nếu cần, bạn có thể tạm thời dùng Fallback từ Silver (trong code đã hỗ trợ ở nhiều tab).

### 8.4. Lỗi gọi Gemini: `api_key không hợp lệ`

- Kiểm tra file `.streamlit/secrets.toml`:
  - Đúng section `[gemini]`
  - API Key bắt đầu bằng `AIza`
- Kiểm tra xem Aistudio/Gemini có đang hạn chế country/tài khoản không.

---

## 9. Gợi ý cấu trúc project

Một cấu trúc tối thiểu:

```text
tiktok_trending_webapp/
├─ app.py
├─ util/
│  ├─ db.py          # hàm run_sql kết nối Databricks
│  └─ filters.py     # sidebar_filters
├─ .streamlit/
│  └─ secrets.toml   # chứa Gemini API key
├─ .venv/            # virtualenv (tự tạo, không cần commit)
└─ README.md         # file này
```

---

Nếu bạn muốn mình viết luôn file `requirements.txt` chuẩn theo app hiện tại, mình có thể soạn thêm để bạn chỉ cần `pip install -r requirements.txt` là xong.



