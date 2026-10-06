# 🏆 Chainlink CRE x AI 5-Minute Prediction Arena (Go & WebAssembly)

Platform arena prediksi pasar kripto real-time berbasis **Chainlink Runtime Environment (CRE)** dalam bahasa **Go (WASM)**, digabungkan dengan **Continuous 5-Minute Chained Engine**, **Model Kuantitatif AI (9router / GLM-5.2)**, dan dashboard interaktif berlatar belakang database **SQLite (WAL Mode)**.

---

## ⚡ Ringkasan Logika & Aturan Permainan

Sistem beroperasi **secara otomatis 24/7** dengan siklus waktu presisi **5 menit (WIB / UTC+7)**.

```
       [Ronde N-1: 23:25 - 23:30]
                    │
           Cutoff Harga: $85,400.00
                    ▼
 ┌──────────────────────────────────────┐
 │ Ronde N (23:30 - 23:35 WIB)          │
 │ • Patokan Acuan  : $85,400.00 (Cutoff 23:30)
 │ • Tebakan User   : UP (▲) atau DOWN (▼)
 │ • Prediksi AI    : 9router Multi-Timeframe
 │ • Selesai 23:35  : Final Spot $85,550.00
 └──────────────────┬───────────────────┘
                    │
           Cutoff Harga: $85,550.00
                    ▼
 ┌──────────────────────────────────────┐
 │ Ronde N+1 (23:35 - 23:40 WIB)        │
 │ • Patokan Acuan  : $85,550.00 (Cutoff 23:35)
 │ • Bersambung Otomatis Tanpa Henti... │
 └──────────────────────────────────────┘
```

1. **Harga Patokan Berantai (*Chained Cutoff Benchmark*)**:
   - Harga yang dijadikan batas penentuan untuk 5 menit ke depan adalah **harga cutoff saat 5 menit sebelumnya**.
   - Contoh: Ronde 1 selesai di `23:30:00` pada harga `$85,400.00`. Maka untuk ronde berikutnya (`23:30 - 23:35`), batas acuan yang digunakan adalah tepat **`$85,400.00`**.
   - Pada detik cutoff `23:35:00`, harga spot akhir dicatat (misal `$85,550.00`), ronde dikonsolidasi, dan `$85,550.00` langsung menjadi patokan ronde `23:35 - 23:40`.
2. **Penentuan Hasil (*Outcome Resolution*)**:
   - Jika $\text{Harga Akhir} > \text{Harga Patokan}$: Hasil = **UP (▲)**.
   - Jika $\text{Harga Akhir} < \text{Harga Patokan}$: Hasil = **DOWN (▼)**.
   - Jika $\text{Harga Akhir} = \text{Harga Patokan}$: Hasil = **DRAW**.
3. **Analisis AI Multi-Timeframe (Target Presisi 99.5%)**:
   - **Jangka Pendek (1m - 5m Micro)**: Menghitung RSI 5m (14-period), status crossover EMA 9 vs EMA 21, dan micro-momentum candle 1m (higher-high / lower-low).
   - **Jangka Panjang (15m - 1h - 24h Macro)**: Menghitung struktur tren 24 jam, pergerakan rentang High/Low, volume 24 jam, serta level support dan resistensi lokal.
   - Dihubungkan ke endpoint 9router (`cbai/glm-5.2` atau model pilihan Anda) dengan tingkat keyakinan (*confidence*) **85% - 99%**.
4. **Leaderboard & Riwayat Terpusat**:
   - Seluruh pertandingan, tebakan user, prediksi AI, dan harga final tersimpan secara persisten di database SQLite (`data/prediction.db`).
   - Leaderboard otomatis menghitung skor kemenangan solo Anda, skor AI, kemenangan bersama (*draw wins*), dan persentase *win-rate*.

---

## 🛠️ Arsitektur Teknologi

| Komponen | Teknologi | Keterangan |
| :--- | :--- | :--- |
| **CRE Workflow Engine** | Go (`wasip1` WebAssembly) | Scheduled 5m Cron Trigger (`0 */5 * * * *`), consensus HTTP |
| **Backend Web Server** | Go HTTP + Background Worker | Engine 5-menit berantai, auto-rollover, live price feeder |
| **Database** | SQLite via `modernc.org/sqlite` | Pure Go driver (tanpa CGO/gcc), WAL Mode, thread-safe |
| **Frontend UI** | HTML5, Tailwind CSS, Chart.js, Lucide | Real-time tick chart, live clock WIB, interactive prediction |
| **AI Provider** | 9router (`http://127.0.0.1:20128/v1`) | OpenAI-compatible endpoint dengan fallback kuantitatif deterministik |

---

## 🚀 Panduan Memulai Cepat

### 1. Menjalankan Dashboard Web Interaktif
Dashboard mencakup seluruh engine 5-menit berantai otomatis, live chart Binance, input tebakan Anda, dan leaderboard:

```powershell
cd E:\apep\WEB\cre-prediction-bot
go run web/server.go web/db.go
```
Buka browser pada: **[http://localhost:8090](http://localhost:8090)**

### 2. Menjalankan Simulasi Native Chainlink CRE
Untuk memvalidasi alur kerja terdesentralisasi menggunakan CRE CLI:

```powershell
cd E:\apep\WEB\cre-prediction-bot
cre workflow simulate prediction-workflow --target staging-settings --non-interactive --trigger-index 0
```

### 3. Menjalankan Pengujian Unit (Unit Test)
```powershell
cd E:\apep\WEB\cre-prediction-bot
go test ./...
```

---

## ⚙️ Konfigurasi & Environment Variable

File `.env` terletak di root direktori proyek (`E:\apep\WEB\cre-prediction-bot/.env`):

```env
# AI Provider Configuration (9router / Local LLM)
AI_API_KEY_VAR=your-actual-9router-api-key
AI_BASE_URL=http://127.0.0.1:20128/v1
AI_MODEL=cbai/glm-5.2

# Server Port (Opsional, Default 8090)
PORT=8090
```

Pemetaan kunci rahasia untuk Chainlink CRE dikelola secara deklaratif di `secrets.yaml`:
```yaml
secrets:
  AI_API_KEY: ${AI_API_KEY_VAR}
```

---

## 📖 Dokumentasi Lengkap Proyek
- 📋 [Product Requirements Document (PRD.md)](./PRD.md)
- 🤖 [Konteks Operasional AI Agent & Hermes (AGENTS.md)](./AGENTS.md)
- 🏗️ [Arsitektur & Diagram Alur Data (STRUCTURE.md)](./STRUCTURE.md)
- ⚡ [Detail Alur Kerja CRE Go (prediction-workflow/README.md)](./prediction-workflow/README.md)
