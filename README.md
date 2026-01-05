# IMDBScraperwSelenium — IMDB Data Science Pipeline + React Dashboard

Bu repo, IMDB’den film verisi toplayıp (Selenium + fallback), veriyi temizleyip (duration parse + median imputation) IQR istatistikleri ile analiz eden ve anomalileri işaretleyen **uçtan uca** bir veri bilimi projesidir. Sonuçlar `movies_final.json` olarak dışa aktarılır ve React dashboard tarafından görselleştirilir.

## Proje çıktıları (ödev hedefi)

**Python (ETL + analiz):**

- IMDB’den alanlar: `title`, `year`, `rating`, `metascore`, `duration_min`, `genres`, `votes`, `url`
- Veri temizleme:

  # IMDBScraperwSelenium — Hızlı Başlatma (copy-paste ready)

  Bu repo IMDB Top-250 Movies ve Top-250 TV Shows verilerini toplayan (Selenium + fallback), temizleyen ve anomali tespiti yaptıktan sonra bir React dashboard ile görselleştiren bir ETL + dashboard projesidir.

  Amaç: repoyu klonlayan birinin, terminale README'den kopyala-yapıştır ile aynı sonucu (250 film + 250 dizi → `frontend/public/movies_final.json`) alabilmesini sağlamaktır.

  ## Gereksinimler

  - Linux/macOS (önerilen)
  - Python 3.8+ ve `venv` modülü
  - Node.js + npm
  - Chrome veya Chromium yüklü (Selenium için; `webdriver-manager` projede mevcutsa sürücü otomatik indirilir)
  - Git

  Not: Bu README Linux / bash için hazır komutlar içerir.

  ## Hızlı kurulum ve çalıştırma (tam kopyala-yapıştır)

  1. Depoyu klonlayın ve dizine girin

  ```bash
  git clone https://github.com/<your-username>/IMDBScraperwSelenium.git
  cd IMDBScraperwSelenium
  ```

  2. Python sanal ortam oluşturun ve bağımlılıkları yükleyin

  ```bash
  python3 -m venv venv
  source venv/bin/activate
  pip install --upgrade pip
  pip install -r requirements.txt
  ```

  3. (Opsiyonel ama önerilir) `logs/` dizinini oluşturun

  ```bash
  mkdir -p logs
  ```

  4. Scraper + pipeline'i çalıştırın — 250 film ve 250 dizi (toplam ~500 kayıt)

  Bu tek satır komutu arka planda çalıştırır, logları `logs/data_processor.log`'a yazar ve PID'i `logs/data_processor.pid` içine kaydeder.

  ```bash
  ./venv/bin/python data_processor.py --limit 250 --threads 24 --autosave-every 25 > logs/data_processor.log 2>&1 & echo $! > logs/data_processor.pid
  ```

  Ne yapar: `data_processor.py` paralel olarak IMDB sayfalarını tarar, veriyi temizler, anomali bayraklarını atar ve sonuçları `frontend/public/movies_final.json` içine (kademeli olarak) yazar.

  5. Frontend (React) uygulamasını ayrı bir terminalde başlatın

  ```bash
  cd frontend
  npm install
  PORT=3001 npm start > ../logs/react.log 2>&1 & echo $! > ../logs/react.pid
  ```

  Tarayıcıda açın: http://127.0.0.1:3001

  6. Kısa doğrulama — JSON içindeki kayıt sayısını kontrol edin (pipeline tamamlandığında)

  ```bash
  python3 - <<'PY'
  import json
  print(len(json.load(open('frontend/public/movies_final.json'))['records']))
  PY
  ```

  Bu komut 500 (250 film + 250 dizi) civarı bir sayı döndürmelidir.

  ## Durum/Stop komutları

  - Scraper'ı durdurmak:

  ```bash
  kill $(cat logs/data_processor.pid) || true
  ```

  - React dev server'ı durdurmak:

  ```bash
  kill $(cat logs/react.pid) || true
  ```

  ## Hatalar / sık karşılaşılan durumlar

  - Eğer `frontend/public/movies_final.json` uygulama tarafından bulunamıyorsa, pipeline'in tamamlandığını ve dosyanın yazıldığını kontrol edin. (Yol proje köküne göre `frontend/public/...` olmalıdır.)
  - Eğer Selenium sürücü hatası alırsanız, Chrome/Chromium kurulu olduğundan emin olun. `webdriver-manager` kullanılıyorsa sürücü otomatik indirilir; aksi halde sistemde bir chromedriver olmalıdır.
  - Port 3001 meşgulse başka bir port seçin: `PORT=3002 npm start`.

  ## Notlar

  - Büyük/binary dosyalar `archive_unused/` içine taşındı ve `.gitignore` güncellendi; push etmeden önce bu klasörü dahil etmek istemezsiniz.
  - Eğer uzak repoya pushlayacaksanız, gizli/özel bilgileri (ör. `.env`) içermediğinizden emin olun.

  ***

  Eğer isterseniz, ben bu README'i repo URL'nizle otomatik güncelleyebilirim ve ayrıca bir `Makefile` veya `dev` scripti ekleyip tüm adımları tek komutla çalıştırılabilir hale getirebilirim.
