# Chainlink CRE 5-Minute Binance Prediction Bot (Go)

Workflow otomatis berbasis **Chainlink Runtime Environment (CRE)** yang ditulis dalam bahasa **Go** (WebAssembly guest `wasip1`). Bot ini melakukan analisis dan menghasilkan prediksi posisi trading (LONG / SHORT / NEUTRAL), target take profit, level stop loss, dan rekomendasi ukuran posisi setiap 5 menit secara terdesentralisasi, dengan integrasi ke penyedia AI lokal (**9router**) dan *fallback* teknikal deterministik.

---

## Fitur Utama
1. **Jadwal 5 Menit Terdesentralisasi**: Dikelola oleh capability `cron` standar 6-field CRE (`0 */5 * * * *`).
2. **Data Pasar Real-Time**: Mengambil data ticker 24 jam langsung dari API Binance melalui `creHttp.SendRequest` dengan *BFT consensus aggregation*.
3. **Kombinasi AI Provider (9router)**: Terintegrasi dengan endpoint OpenAI-compatible lokal 9router (`http://127.0.0.1:20128/v1/chat/completions`) menggunakan model seperti `cbai/glm-5.2`.
4. **Resilience & Fallback Mechanism**: Jika endpoint AI sedang offline atau mengalami kegagalan respons, workflow secara otomatis beralih ke kalkulasi teknikal momentum 24 jam agar simulasi dan kontrak tetap berjalan tanpa kendala.
5. **Keamanan Kunci Rahasia**: API Key dikelola melalui `secrets.yaml` bawaan Chainlink CRE yang dihubungkan ke environment variable lokal (`AI_API_KEY_VAR`).

---

## Struktur Berkas & Panduan
- 📋 [Product Requirements Document (PRD.md)](./PRD.md)
- 🤖 [Panduan AI Agent & Hermes (AGENTS.md)](./AGENTS.md)
- 🏗️ [Arsitektur & Struktur Direktori (STRUCTURE.md)](./STRUCTURE.md)

---

## Cara Menjalankan Simulasi

### 1. Prasyarat
- **CRE CLI v1.37.0+** terinstal dan telah login (`cre whoami`).
- **Go 1.25.3+** terinstal di sistem.

### 2. Konfigurasi Kunci API AI
Pastikan file `.env` di root proyek memiliki variabel:
```env
AI_API_KEY_VAR=your_9router_api_key_here
```

### 3. Eksekusi Simulasi
Jalankan perintah berikut dari root direktori proyek (`E:\apep\WEB\cre-prediction-bot`):

```powershell
cre workflow simulate prediction-workflow --target staging-settings --non-interactive --trigger-index 0
```

Hasil simulasi akan menampilkan log data pasar Binance dan output prediksi lengkap:
```json
{
  "Prediction": {
    "ConfidencePct": 75,
    "CurrentPrice": 85780,
    "Direction": "LONG",
    "High24h": 86698.99,
    "HorizonMinutes": 5,
    "Low24h": 84972.01,
    "PositionSizeUSD": 100,
    "PriceChange24hPct": 0.65,
    "Provider": "deterministic-technical-model",
    "Reasoning": "Mid-range consolidation (46.8% of 24h band); scalping minor upward drift",
    "RecommendedLeverage": 5,
    "RiskRewardRatio": 1.43,
    "StopLossPrice": 85179.54,
    "Symbol": "BTCUSDT",
    "TargetPrice": 86637.8,
    "Timestamp": "2026-10-06T16:06:05Z"
  },
  "Status": "SUCCESS"
}
```

---

## Unit Testing
Jalankan pengujian unit Go dengan:
```powershell
go test ./...
```
