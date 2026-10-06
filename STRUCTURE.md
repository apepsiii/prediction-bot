# Sistem & Arsitektur: Continuous 5-Minute Prediction Arena

## 1. Diagram Alur Siklus Rantai 5 Menit (Chained 5M Cycle)

```mermaid
sequenceDiagram
    autonumber
    actor User as User (Human)
    participant Web as Web Dashboard (Browser)
    participant Engine as Continuous Engine (Go)
    participant DB as SQLite WAL (data/prediction.db)
    participant Binance as Binance API (Spot & Klines)
    participant AI as 9router LLM (GLM-5.2)

    Note over Engine,DB: Inisialisasi Ronde 5-Menit
    Engine->>Binance: Ambil harga cutoff ronde 5m sebelumnya ($85,740.00)
    Engine->>DB: Simpan Ronde Aktif (Patokan = $85,740.00, Cutoff = T+300s)
    Engine->>Binance: Fetch Indikator (5m RSI, EMA9/21, 1m Micro-trend, 24h Ticker)
    Engine->>AI: Kirim Prompt Quant Multi-Timeframe (Presisi 99.5%)
    AI-->>Engine: Output JSON (Pilihan: DOWN, Conf: 95%, Reasoning)
    Engine->>DB: Perbarui AI Choice & Reasoning di Ronde Aktif

    Note over User,Web: User Bertanding Selama Rentang 5 Menit
    Web->>Engine: GET /api/rounds/active (Tampilkan countdown & pergerakan harga)
    User->>Web: Klik [TEBAK DOWN]
    Web->>Engine: POST /api/rounds/bet (Terkunci ke Ronde Aktif)
    Engine->>DB: Update user_choice = 'DOWN'

    Note over Engine,Binance: Detik Cutoff 5 Menit Tiba!
    Engine->>Binance: Fetch harga spot final pada waktu cutoff ($85,580.00)
    Engine->>Engine: Bandingkan $85,580.00 vs Patokan $85,740.00 -> Hasil: DOWN
    Engine->>DB: Update Ronde: status='CONSOLIDATED', final_price=$85,580.00, Pemenang: Keduanya Benar
    
    Note over Engine,DB: Sambung Otomatis ke Ronde Berikutnya!
    Engine->>DB: Buka Ronde Baru (Patokan Acuan = $85,580.00, Cutoff = T+300s)
    Engine->>AI: Trigger Analisis Multi-Timeframe Baru
```

## 2. Struktur Direktori Proyek

```
cre-prediction-bot/
├── .env                              # Variabel rahasia lokal (AI_API_KEY_VAR, AI_BASE_URL, AI_MODEL)
├── secrets.yaml                      # Pemetaan secrets Chainlink CRE
├── project.yaml                      # Konfigurasi project CRE
├── PRD.md                            # Product Requirements Document
├── AGENTS.md                         # Panduan agen AI masa depan
├── STRUCTURE.md                      # Diagram alur & arsitektur sistem
├── README.md                         # Petunjuk operasional & CLI
│
├── data/
│   └── prediction.db                 # Database SQLite aktif (WAL Mode)
│
├── prediction-workflow/              # Chainlink CRE Go Native Workflow
│   ├── config.production.json        # Config runtime CRE
│   ├── config.staging.json           # Config staging CRE
│   ├── workflow.go                   # Core workflow logic (Cron 5m + WASM HTTP)
│   ├── workflow_test.go              # Unit test alur kerja CRE
│   └── workflow.yaml                 # Metadata alur kerja CRE
│
└── web/                              # Fullstack Prediction Arena Engine
    ├── server.go                     # Continuous rolling engine, workers, REST APIs
    ├── db.go                         # SQLite schema, queries, leaderboard logic
    └── static/
        └── index.html                # UI Dashboard (Tailwind CSS, Chart.js, Lucide)
```
