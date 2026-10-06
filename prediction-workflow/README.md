# Chainlink CRE 5-Minute Prediction Workflow (Go WASM)

Direktori ini berisi implementasi alur kerja (*workflow*) terdesentralisasi berbasis **Chainlink Runtime Environment (CRE)** yang dikompilasi ke target WebAssembly (`wasip1`).

---

## 📌 Gambaran Alur Kerja

Workflow ini berjalan secara otonom di atas jaringan Chainlink DON (*Decentralized Oracle Network*):
1. **Pemicu Terjadwal (Cron Capability)**: Dipicu setiap 5 menit menggunakan standar cron 6-field (`0 */5 * * * *`).
2. **Pengambilan Data Pasar (HTTP Capability)**: Meminta data pasar Binance (ticker 24 jam) menggunakan capability HTTP terdesentralisasi dengan konsensus agregasi BFT (*Consensus Identical Aggregation*).
3. **Analisis Prediksi AI (9router)**: Mengirimkan konteks teknikal pasar ke penyedia model AI lokal/kustom via `runtime.GetSecret("AI_API_KEY")`.
4. **Resiliensi Deterministik**: Jika endpoint AI sedang offline atau tidak merespons dalam batas waktu batas, workflow secara otomatis mengeksekusi analisis matematis deterministik agar konsensus node tetap tercapai.

---

## 📂 Berkas & Konfigurasi

- [`workflow.go`](./workflow.go): Logika inti alur kerja CRE dalam bahasa Go.
- [`workflow_test.go`](./workflow_test.go): Pengujian unit Go untuk workflow.
- [`workflow.yaml`](./workflow.yaml): Metadata deklarasi alur kerja CRE.
- [`config.staging.json`](./config.staging.json): Konfigurasi target staging/simulasi (URL, simbol, model AI).
- [`config.production.json`](./config.production.json): Konfigurasi target produksi.

---

## 🚀 Cara Menjalankan Simulasi Lokal

Jalankan perintah ini dari **root direktori proyek** (`E:\apep\WEB\cre-prediction-bot`):

```powershell
cre workflow simulate prediction-workflow --target staging-settings --non-interactive --trigger-index 0
```

### Parameter Konfigurasi Staging (`config.staging.json`)
```json
{
  "schedule": "0 */5 * * * *",
  "symbol": "BTCUSDT",
  "price_url": "https://api.binance.com/api/v3/ticker/24hr?symbol=BTCUSDT",
  "ai_base_url": "http://127.0.0.1:20128/v1",
  "ai_model": "cbai/glm-5.2",
  "default_position_usd": 100.0,
  "max_leverage": 5
}
```

---

## 🧪 Menjalankan Pengujian Unit
```powershell
go test ./...
```
