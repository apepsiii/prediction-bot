# Hermes & AI Agent Context - Chainlink CRE Go Prediction Bot

Dokumen ini adalah panduan operasional (*Context File*) utama untuk agen AI / Hermes di masa depan yang akan mengelola, mengembangkan, menguji, atau memperbaiki proyek ini.

---

## 1. Arsitektur Sistem & Lingkungan

- **Platform**: Chainlink Runtime Environment (CRE) CLI v1.37.0+ & Go HTTP Web Engine.
- **Bahasa**: Go 1.25.6+ (WASM target `wasip1` untuk workflow CRE; native Go untuk Web Server & Database).
- **Root Directory**: `E:\apep\WEB\cre-prediction-bot`.
- **Database**: `data/prediction.db` (SQLite murni dengan driver `modernc.org/sqlite`, mode WAL aktif).
- **Dashboard Web**: `http://localhost:8090` (dilayani oleh `web/server.go`).
- **AI Provider**: Endpoint OpenAI-compatible lokal 9router di `http://127.0.0.1:20128/v1` (Model default: `cbai/glm-5.2`).
- **Zona Waktu Resmi**: **WIB (UTC+7)** (`time.FixedZone("WIB", 7*3600)`).

---

## 2. Invarian Logika Inti (JANGAN DIUBAH SEMBARANGAN)

### A. Siklus Rantai 5 Menit Berkelanjutan (Continuous Chaining)
- Sistem berputar secara otomatis 24/7 tanpa perlu pemicu manual dari user.
- **Harga Patokan Acuan (*Benchmark*)**:
  - Harga patokan untuk ronde 5 menit yang sedang berjalan adalah **HARGA CUTOFF 5 MENIT SEBELUMNYA**.
  - Contoh: Ronde `23:30 - 23:35` berakhir di harga `$85,400.00`. Maka tepat di `23:35:00`, ronde `23:35 - 23:40` dibuat dengan batas acuan `$85,400.00`.
- **Resolusi Pemenang**:
  - Pada detik cutoff (misal `23:40:00`), harga spot riil Binance diambil (`final_price`).
  - Jika $\text{final\_price} > \text{threshold\_price}$: Hasil = `UP`.
  - Jika $\text{final\_price} < \text{threshold\_price}$: Hasil = `DOWN`.
  - Jika $\text{final\_price} == \text{threshold\_price}$: Hasil = `DRAW`.
  - Konsolidasi pemenang (Human vs AI) disimpan ke SQLite dengan status `CONSOLIDATED`.
  - Seketika ronde baru dibuka dengan patokan = $\text{final\_price}$ barusan.

### B. Analisis AI Kuantitatif Multi-Timeframe (Presisi 99.5%)
- AI tidak hanya menebak acak, tetapi menganalisis gabungan:
  1. **Jangka Pendek (1m - 5m Micro)**:
     - RSI 5m (14-period).
     - Status crossover EMA 9 vs EMA 21.
     - Micro-momentum candle 1m (higher-high / lower-low).
  2. **Jangka Panjang (15m - 1h - 24h Macro)**:
     - Tren harga 24 jam (% perubahan).
     - Rentang harga 24 jam (High - Low) dan volume 24 jam.
     - Support dinamis dan resistensi lokal.
- Prompt menghasilkan output JSON terstruktur: `choice` (UP/DOWN), `confidence` (85-99%), `short_term`, `long_term`, dan `reasoning`.
- **Fallback Deterministik**: Jika 9router offline/timeout, engine otomatis mengeksekusi kalkulasi quant matematis berbasis indikator teknikal riil di atas agar sistem tidak pernah macet (*zero hang*).

---

## 3. Database Schema (SQLite)

Tabel `rounds` di [`data/prediction.db`](file:///E:/apep/WEB/cre-prediction-bot/data/prediction.db):
```sql
CREATE TABLE IF NOT EXISTS rounds (
    id TEXT PRIMARY KEY,               -- Contoh: RND-BTC-2335
    symbol TEXT NOT NULL,             -- Contoh: BTCUSDT
    start_time_wib TEXT NOT NULL,     -- Format: 23:35:00 WIB
    cutoff_time_wib TEXT NOT NULL,    -- Format: 23:40:00 WIB
    created_at INTEGER NOT NULL,      -- Unix timestamp mulai
    cutoff_at INTEGER NOT NULL,       -- Unix timestamp cutoff (created_at + 300s)
    start_price REAL NOT NULL,        -- Patokan cutoff 5m sebelumnya
    threshold_price REAL NOT NULL,    -- Sama dengan start_price sebagai batas acuan
    final_price REAL DEFAULT 0,       -- Harga spot saat waktu cutoff tiba
    duration_minutes INTEGER DEFAULT 5, -- 5 menit
    user_choice TEXT NOT NULL,        -- 'UP', 'DOWN', atau 'PENDING'
    ai_choice TEXT NOT NULL,          -- 'UP' atau 'DOWN'
    ai_confidence INTEGER DEFAULT 95, -- 85 - 99%
    ai_reasoning TEXT,                -- Catatan kuantitatif multi-timeframe
    ai_model TEXT,                    -- Contoh: cbai/glm-5.2
    actual_outcome TEXT DEFAULT 'PENDING', -- 'UP', 'DOWN', 'DRAW', 'PENDING'
    user_won INTEGER DEFAULT 0,       -- 1 jika menang, 0 jika kalah
    ai_won INTEGER DEFAULT 0,         -- 1 jika menang, 0 jika kalah
    outcome_summary TEXT DEFAULT 'PENDING', -- 'HUMAN_WON', 'AI_WON', 'DRAW_WIN', 'DRAW_LOSS'
    status TEXT DEFAULT 'ACTIVE'      -- 'ACTIVE' atau 'CONSOLIDATED'
);
```

---

## 4. API Endpoints

- `GET /api/price?symbol=BTCUSDT`: Mengambil harga spot dan ticker 24 jam Binance secara real-time.
- `GET /api/rounds/active?symbol=BTCUSDT`: Mengambil ronde aktif yang sedang berjalan beserta sisa detik menuju cutoff, status live (di atas / di bawah patokan), selisih USD, dan pilihan AI.
- `POST /api/rounds/bet`: Mengunci tebakan user (`UP` atau `DOWN`) untuk ronde aktif yang sedang berjalan.
  - Body: `{"round_id": "RND-BTC-2335", "choice": "DOWN"}`.
- `GET /api/rounds`: Mengambil daftar riwayat ronde dan ringkasan metrik Leaderboard persisten.
- `POST /api/simulate-cre`: Menjalankan simulasi native Chainlink CRE CLI di latar belakang dan mengembalikan output stdout.

---

## 5. Perintah Operasional Penting

### Menjalankan Server Web & Engine 5-Menit
```powershell
cd E:\apep\WEB\cre-prediction-bot
go run web/server.go web/db.go
```

### Menjalankan Simulasi Workflow CRE WASM
```powershell
cd E:\apep\WEB\cre-prediction-bot
cre workflow simulate prediction-workflow --target staging-settings --non-interactive --trigger-index 0
```

### Menjalankan Unit Test Go
```powershell
cd E:\apep\WEB\cre-prediction-bot
go test ./...
```

### Memeriksa Akun & Sesi CRE
```powershell
cre whoami
```

---

## 6. Aturan Khusus Chainlink CRE WASM
1. **WASM Networking Capability**:
   - Di dalam guest environment `wasip1`, panggilan socket `net/http` langsung akan ditolak. Selalu gunakan `creHttp.SendRequest` dari SDK resmi: `github.com/smartcontractkit/cre-sdk-go/capabilities/networking/http`.
2. **Determinisme & Waktu**:
   - Di dalam alur kerja WASM CRE, gunakan `runtime.Now()` untuk timestamp, bukan `time.Now()`.
3. **Format Cron 6-Field**:
   - Pemicu Cron CRE harus memiliki format 6-field: `0 */5 * * * *`.
