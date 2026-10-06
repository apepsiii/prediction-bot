# Product Requirements Document (PRD): Chainlink CRE 5-Minute Binance Prediction Bot

## 1. Executive Summary
Proyek ini adalah implementasi alur kerja (*workflow*) terdesentralisasi berbasis **Chainlink Runtime Environment (CRE)** menggunakan bahasa **Go (WASM)**. Sistem ini secara otomatis memicu evaluasi pasar kripto setiap 5 menit (menggunakan CRON trigger CRE), mengambil data *ticker* terkini dari bursa (Binance API), dan mengeksekusi sinyal prediksi trading (Arah Posisi, Target Take Profit, Batas Stop Loss, Rasio Risk/Reward, dan Ukuran Posisi) yang dikombinasikan dengan penyedia model AI lokal/kustom (**9router** pada `http://127.0.0.1:20128/v1`).

---

## 2. Objective & Scope
- **Autonomous Execution**: Berjalan di atas jaringan terdesentralisasi Chainlink DON (*Decentralized Oracle Network*) atau simulasi WASM lokal tanpa ketergantungan server runtime Node.js/Python biasa.
- **5-Minute Cadence**: Dipicu secara terjadwal setiap 5 menit (`0 */5 * * * *` format 6-field CRE standard).
- **Dual AI & Technical Engine**: Mengutamakan inferensi model AI (seperti `cbai/glm-5.2` atau `deepseek-chat` melalui 9router) dengan kemampuan *fallback* otomatis ke model kalkulasi momentum matematis deterministik bila endpoint AI tidak merespons atau sedang offline.
- **Secure Secrets Management**: Kunci API AI disimpan aman di file `.env` dan dipetakan melalui standar `secrets.yaml` Chainlink CRE (`AI_API_KEY`), sehingga tidak pernah ter-hardcode di kode program.

---

## 3. User Stories & Persona
- **Quant / Algorithmic Trader**: Membutuhkan sinyal posisi (LONG/SHORT/NEUTRAL) beserta target harga dan level stop loss yang terukur untuk pasangan aset kripto utama (misal: BTCUSDT, ETHUSDT, SOLUSDT).
- **DeFi Protocol / Prediction Market Consumer**: Mengharapkan *verifiable output* yang dapat disahkan oleh consensus node Chainlink untuk penyelesaian kontrak derivatif atau pasar prediksi on-chain.

---

## 4. Technical Specifications & Architecture

### 4.1. Core Components
1. **CRE Scheduler (Cron Capability)**:
   - Cron Trigger 6-field: `0 */5 * * * *` (setiap 5 menit tepat).
2. **CRE HTTP Networking Capability**:
   - Outbound HTTP Request dengan *Consensus Identical Aggregation* untuk memastikan node-node dalam DON mencapai kesepakatan BFT terhadap data pasar.
3. **Market Data Feed**:
   - Endpoint: Binance Public REST API `https://api.binance.com/api/v3/ticker/24hr?symbol=<SYMBOL>`.
   - Data yang diambil: `lastPrice`, `priceChangePercent`, `highPrice`, `lowPrice`, `volume`.
4. **AI Inference Engine (9router Integration)**:
   - Endpoint: `http://127.0.0.1:20128/v1/chat/completions`.
   - Method: `POST` dengan format request standar OpenAI API.
   - Headers: `Authorization: Bearer <AI_API_KEY>`, `Content-Type: application/json`.
   - Model Default: `cbai/glm-5.2` (dapat disesuaikan via `config.staging.json`).
5. **Deterministic Fallback Engine**:
   - Jika koneksi ke 9router gagal / timeout / API key belum aktif, kalkulasi dilakukan berdasarkan rentang 24 jam (*range position*) dan momentum *price change*:
     - *Mean Reversion Bounce* di support band ($\le 20\%$).
     - *Resistance Rejection* di ceiling band ($\ge 80\%$).
     - *Momentum Trend Continuation* saat lonjakan volume terdeteksi.

### 4.2. Data Output Schema
Setiap eksekusi menghasilkan payload JSON terstruktur:
```json
{
  "prediction": {
    "symbol": "BTCUSDT",
    "current_price": 85780.00,
    "price_change_24h_pct": 0.65,
    "high_24h": 86698.99,
    "low_24h": 84972.01,
    "direction": "LONG",
    "target_price": 86637.80,
    "stop_loss_price": 85179.54,
    "risk_reward_ratio": 1.43,
    "confidence_pct": 75,
    "position_size_usd": 100.00,
    "recommended_leverage": 5,
    "horizon_minutes": 5,
    "provider": "9router-ai (cbai/glm-5.2)",
    "reasoning": "Mid-range consolidation; scalping minor upward drift",
    "timestamp": "2026-10-06T16:06:05Z"
  },
  "status": "SUCCESS"
}
```

---

## 5. Security & Risk Management
1. **Zero Credential Exposure**: Token dan kunci privat tidak boleh di-commit ke Git. `.env` diabaikan oleh `.gitignore`.
2. **Execution Safeguards**: Level Stop Loss wajib dihasilkan pada setiap sinyal untuk melindungi modal dari *drawdown* ekstrem.
3. **Decoupled Connectivity**: Jika model AI mengalami gangguan latensi, siklus eksekusi tetap selesai tepat waktu melalui *deterministic fallback*.
