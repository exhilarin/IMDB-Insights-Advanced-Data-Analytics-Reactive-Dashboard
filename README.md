# 🎬 COE203 – Advanced Programming with Python: IMDb Analytics Suite

Robust IMDb veri boru hattı: **Selenium + requests/bs4** ile Top 250 film/TV show scraping, **pandas** temizleme/analiz, **IQR & regresyon** tabanlı anomali tespiti, **MongoDB** kayıtları ve **Recharts** destekli **React** dashboard. Kod okunabilirliği, OOP, robustluk ve görselleştirme rubric'lerini karşılar.

---

## ⭐ Özellikler (Features)

### ⚡ Scraping & API
- **Selenium** chart/search sayfası DOM parsing
- **Requests + BeautifulSoup** detay çekimi ve JSON-LD fallback
- **Paralel threading** hızlı toplu scraping (24 workers)
- **Otosave JSON** dev sırasında kısmi veri desteği
- Headless mode ve anti-bot detection

### 🗄️ Veritabanı
- **MongoDB Atlas** entegrasyonu (upsert, ping kontrolü, logging)
- `.env` güvenli kimlik bilgisi yönetimi
- `MongoDBManager` sınıfı ile bağlantı yönetimi

### 🧹 Analiz & Temizleme
- Medyan imputasyonu (tür-bazlı movie/tv ayrımı)
- Süre normalizasyonu (`2h 30m` → dakika)
- **IQR outlier** tespiti ($IQR = Q_3 - Q_1$)
- **Rating-votes regresyon** rezidüel kontrolü
- Duplicate deduplikasyon, genre standardizasyonu

### 📊 Görselleştirme
- **Recharts** tabanlı React dashboard
- Custom boxplot (rating distribution)
- Rating vs Metascore scatter chart
- Filtre (genre, type, anomaly-only) ve sortable tablo
- Opsiyonel **matplotlib/seaborn** boxplot PNG'leri

### 🧱 OOP Mimarisi
- **`MongoDBManager`** — bağlantı, ping, upsert (bknz [databasemanager.py](databasemanager.py))
- **`IMDbScraper`** — Selenium session, infinite scroll, Load More (bknz [new_scraper.py](new_scraper.py))
- **Dataclass `IMDbContent`** — film/TV varlığı (bknz [main.py](main.py))
- **Modüler pipeline fonksiyonları** — fallback katmanları, retry/backoff

### 🎁 Bonus
- CLI menü (watchlist, filtre, Mongo kayıt)
- Unit testleri (dataclass, scraper init, hata yakalama)
- Autosave dosyalar (dev sırasında)

---

## 📂 Proje Yapısı (Project Structure)

```
.
├── advanced_pipeline.py              # Tam boru hattı (scrape→clean→analyze→JSON/PNG)
├── data_processor.py                 # Top 250 film+TV birleşim, medyan impute, IQR anomalileri
├── fast_imdb_top250_scraper.py       # Hızlı Selenium + paralel requests scraper
├── movies_processor.py               # advanced_pipeline sarmalayıcısı
├── run_pipeline.py                   # Launcher (limit/fast/threads)
├── new_scraper.py                    # Genel IMDb Selenium scraper (scroll, Load More)
├── databasemanager.py                # MongoDB bağlantı, upsert, logging
├── main.py                           # CLI menü, scraping, watched list
├── test.py                           # Unit testleri (dataclass, init, hata yakalama)
├── requirements.txt                  # Python bağımlılıkları
│
└── frontend/
    ├── package.json                  # React + Recharts + lucide-react
    ├── src/
    │   ├── App.js                    # Dashboard, filtre/sıralama, anomali rozetleri
    │   ├── App.css                   # Tema ve responsive stil
    │   ├── index.js
    │   ├── index.css
    │   └── reportWebVitals.js
    └── public/
        ├── index.html
        ├── movies_final.json         # Nihai boru hattı çıktısı (React tarafından okunan)
        └── movies_final_autosave.json  # Dev sırasında ara kayıtlar
```

---

## 🔧 Kurulum (Installation)

### Ortak Gereksinimler
- **Python 3.10+** (3.11+ önerilir)
- **Node.js 18+** (React CLI için)
- **Chrome/Chromium** (Selenium; webdriver-manager otomatik kurar)
- **MongoDB URI** (`.env` dosyasında `MONGO_URI="mongodb+srv://..."`), MongoDB Atlas veya local

### Windows Kurulumu

1. **Repoyu klonlayın:**
   ```cmd
   git clone <repo-url>
   cd /path/to/project
   ```

2. **Python sanal ortamı (CMD):**
   ```cmd
   python -m venv venv
   venv\Scripts\activate
   pip install -r requirements.txt
   ```
   
   **veya PowerShell:**
   ```powershell
   python -m venv venv
   .\venv\Scripts\Activate.ps1
   pip install -r requirements.txt
   ```
   
   > PowerShell ExecutionPolicy hatası: `Set-ExecutionPolicy -Scope CurrentUser -ExecutionPolicy RemoteSigned -Force`

3. **`.env` dosyası oluşturun:**
   ```
   MONGO_URI=mongodb+srv://user:password@cluster.mongodb.net/db
   ```

4. **Veri pipeline'ını çalıştırın:**
   ```cmd
   python data_processor.py --limit 250 --threads 16
   ```

5. **Frontend'i ayrı terminalde başlatın:**
   ```cmd
   cd frontend
   npm install
   npm start
   ```
   
   Tarayıcı otomatik açılır: **http://localhost:3000**

### Linux / macOS Kurulumu

1. **Repoyu klonlayın:**
   ```bash
   git clone <repo-url>
   cd /path/to/project
   ```

2. **Python sanal ortamı:**
   ```bash
   python3 -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   ```

3. **`.env` dosyası:**
   ```bash
   echo 'MONGO_URI=mongodb+srv://user:password@cluster.mongodb.net/db' > .env
   ```

4. **Veri pipeline'ını çalıştırın:**
   ```bash
   python data_processor.py --limit 250 --threads 16
   ```

5. **Frontend'i başlatın:**
   ```bash
   cd frontend
   npm install
   npm start
   ```
   
   http://localhost:3000

### Olası Hatalar ve Çözümleri

| Hata | Çözüm |
|------|-------|
| ChromeDriver uyumsuzluğu | `pip install --upgrade webdriver-manager` veya `CHROME_DRIVER_PATH=/path/to/chromedriver` |
| 403 Forbidden (IMDb blokajı) | `--threads` küçült, `SLEEP_BETWEEN_REQUESTS` artır, user-agent doğrula |
| MongoDB bağlantı hatası | URI doğru mu? IP allowlist, TLS ayarlarını kontrol et |
| SSL/TLS uyarıları | Sistem CA sertifikalarını güncelle (`pip install --upgrade certifi`) |
| `npm install` başarısız | Node.js sürümünü kontrol et; `npm cache clean --force` ve yeniden dene |
| Permission denied (Linux) | `chmod +x advanced_pipeline.py` veya virtualenv'i yeniden etkinleştir |

---

## 📖 Kullanım (Usage)

### Gelişmiş Boru Hattı (Scrape + Clean + Analyze)

```bash
python run_pipeline.py --limit 50 --fast --threads 8
```

**Parametreler:**
- `--limit` (int, default=25): Kaç film/TV show çekilecek
- `--fast` (flag): Requests-tabanlı hızlı mod (Selenium yok)
- `--threads` (int, default=8): Paralel workers sayısı

**Çıktılar:**
```
movies_cleaned.json          # Temizlenen kayıtlar
movies_charts.json           # Histogram/scatter JSON'ları
movies_analysis.json         # İstatistiksel özet
movies_final.json            # Anomali bayrakları + summary
boxplot_rating.png           # Rating distribution (opsiyonel)
boxplot_metascore.png        # Metascore distribution (opsiyonel)
frontend/public/movies_final.json  # React dashboard tarafından okunacak
```

### Hızlı Top 250 Scraper

```bash
python fast_imdb_top250_scraper.py --limit 250 --threads 24 --autosave-every 25
```

### Birleşik Top 250 Film + TV Show

```bash
python data_processor.py --limit 250 --threads 16 --autosave-every 25
```

### CLI Menü + Mongo İş Akışı

```bash
python main.py --headless
```

**Menü seçenekleri:**
- Top 250 Movies / TV Shows / Popular scrape
- Watched list ekle/çıkar/filtrele
- Rating bazlı filtre
- Veritabanı temizle

### React Dashboard

```bash
cd frontend && npm start
```

- `movies_final.json` yüklenir
- Filtrele, sırala, anomali rozetleri gör
- Boxplot & scatter chart interact

### Testler Çalıştırın

```bash
python -m unittest test.py
```

Testler: dataclass alan kontrolü, scraper init, hatalı Mongo URI

---

## 📊 Veri Kaynağı & Temizleme (Dataset)

### Kaynaklar
- **IMDb Top 250 Movies** (https://www.imdb.com/chart/top/)
- **IMDb Top 250 TV Shows** (https://www.imdb.com/chart/toptv/)

### Scraping Yöntemi
1. **Selenium** → Chart sayfasından film/TV show linklerini DOM'dan çekme
2. **Requests/BeautifulSoup** → Detay sayfaları (metascore, votes, duration, genres)
3. **JSON-LD Fallback** → Dinamik içerik kaçırılmamışsa parsing
4. **Regex Fallback** → CSS değişiklikleri karşısında robustluk

### Temizleme Adımları

1. **Süre Normalizasyonu**
   - `2h 30m`, `150 min`, `PT2H22M` → dakika (integer)
   - Aşırı uzun süreler (>10 saat) elenir

2. **Sayısal Coercion**
   - Rating, metascore, votes, year → numeric types
   - NaN yönetimi

3. **Medyan İmputasyonu**
   - Tür-bazlı: movie ve tv show'lar ayrı impute
   - Median seçimi: çarpık dağılımda ortalamadan daha robust

4. **Genre Deduplikasyonu**
   - Tekrar eden genre'ler temizle
   - Case-insensitive standardizasyon

5. **Duplicate Linkler**
   - Aynı URL birden çekilmemiş

---

## 📈 Analiz & Görselleştirme

### Yapılan Analizler

**IQR-Tabanlı Outlier Tespiti**
- Her değişken için Q1, Q3 hesaplanır
- IQR = Q3 − Q1
- Alt sınır = Q1 − 1.5×IQR, Üst sınır = Q3 + 1.5×IQR
- Sınırların dışındaki gözlemler bayraklanır

**Rating-Votes Regresyon**
- Y = log(votes), X = rating
- Residual = gerçek − tahmin
- Büyük residual = tutarsızlık → anomali

**Yüksek Rating + Düşük Metascore Heuristic**
- rating ≥ 8.5 ∧ metascore < medyan − 10 → bayrak

**Tür-Bazlı Analiz**
- Movie ve TV show istatistikleri ayrı
- Anomali bayrakları tür başına

### Kullanılan Kütüphaneler

| Kütüphane | Amaç |
|-----------|------|
| **pandas** | DataFrame işlemleri, groupby, imputation |
| **numpy** | Sayısal hesaplamalar, NaN yönetimi |
| **scipy** | Regresyon (linregress), istatistikler |
| **matplotlib** | PNG boxplot export |
| **seaborn** | Stil ve hızlı visualizasyon |
| **recharts** (React) | Interactive chart dashboard |
| **requests** | HTTP scraping |
| **selenium** | Browser automation |
| **beautifulsoup4** | HTML parsing |
| **pymongo** | MongoDB bağlantısı |

### Extra Point Kısımlar

✓ **Görselleştirme:** React dashboard, custom boxplot, scatter chart  
✓ **Dataset:** 500+ kayıt (Top 250 film + TV)  
✓ **Analiz:** IQR, regresyon, medyan imputation, tür-bazlı istatistik  
✓ **OOP:** MongoDBManager, IMDbScraper, dataclass design patterns

---

## 🏗️ OOP & Mimari (Architecture)

### Sınıf Tasarımları

**`MongoDBManager` ([databasemanager.py](databasemanager.py))**
```python
class MongoDBManager:
    def __init__(self, uri, db_name, collection_name)
    def connect() -> bool
    def insert_data(data_dict, rank=None)
```
- Bağlantı yönetimi, ping, upsert
- Logging entegrasyonu, hata yakalama

**`IMDbScraper` ([new_scraper.py](new_scraper.py))**
```python
class IMDbScraper:
    def __init__(self, headless=False)
    def scrape_data(chart_url, limit=50) -> list[dict]
    def close()
```
- Selenium WebDriver session
- Infinite scroll, "Load More" button click
- Dynamic content handling

**Dataclass `IMDbContent` ([main.py](main.py))**
```python
@dataclass
class IMDbContent:
    title: str
    rating: float
    year: int
    category: str
    watched: bool = False
```
- Type hints, default değerler
- JSON serialization (`asdict()`)

### Modüler Pipeline Fonksiyonları

- `collect_top_links_via_requests()` — Link toplama
- `fetch_details_requests()` — Paralel detail çekimi
- `build_dataframe()` — Type coercion
- `impute_numeric_with_median()` — Eksik veri doldurma
- `detect_anomalies()` — Multi-method anomali
- `prepare_final_json()` — JSON export

### Tasarım İlkeleri

✓ **Fallback Katmanları:** Selenium başarısız → requests → regex  
✓ **Tür Ayrımı:** Movie vs TV show istatistikleri ayrı  
✓ **Retry/Backoff:** Ağ hataları otomatik retry  
✓ **Headless Mode:** Opsiyonel görsel tarayıcı  
✓ **Otosave:** Dev sırasında kısmi kurtarma

---

## ✅ Testler & Robustluk (Testing & Robustness)

### Unit Testleri ([test.py](test.py))

```python
test_01_data_class_integrity()      # IMDbContent field validation
test_02_scraper_initialization()    # IMDbScraper headless init
test_03_database_connection_failure_handling()  # Hatalı URI graceful fail
```

Çalıştırma:
```bash
python -m unittest test.py
```

### Hata Yönetimi

| Hata Türü | Stratejisi |
|-----------|-----------|
| Network timeout | Retry with backoff (2^n seconds) |
| HTML parsing | Fallback regex, null values |
| Selenium failure | Requests + BeautifulSoup |
| Mongo connection | Log + graceful skip |
| Missing values | Median imputation |

## 📜 Lisans & Akademik Not (License & Academic Use)

**Bu proje COE203 (Advanced Programming with Python) ders öğretim materyalidir.**

### Kullanım Koşulları

- IMDb'nin **Terms of Service** ve **robots.txt** kurallarına uyun
- **Yoğun scraping yapmayın** (rate limiting risk'i)
- Çıkartılan verileri **ticari amaçla kullanmayın**
- Kimlik bilgilerini (`.env`) **gizli tutun, commit'lemeyin**

### Atıf

```
IMDb Analytics Suite (COE203 Advanced Programming with Python)
Python 3.10+, Selenium, Requests, Pandas, MongoDB, React
```

---

## 📞 Destek & İletişim

- Issues/Questions: Lütfen GitHub Issues açın
- Hızlı test: `python -m unittest test.py`
- Log kontrol: `tail -f logs/data_processor.log`
- Frontend debug: Browser DevTools (F12)

---

**Yapılış Tarihi:** January 2026  
**Son Güncelleme:** COE203 Rubric Compliance  
**Status:** Production Ready ✓
