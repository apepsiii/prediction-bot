# Product Requirements Document (PRD): Chainlink CRE 5-Minute Binance Prediction Bot

## 1. Ringkasan Eksekutif
Proyek ini adalah implementasi sistem prediksi trading kripto 5-menit beruntun (*continuous 5-minute rolling rounds*) berbasis **Chainlink Runtime Environment (CRE)** menggunakan bahasa **Go (WASM)** dan dashboard web interaktif berlatar belakang database **SQLite (WAL mode)**.

Sistem berjalan otomatis 24/7 dengan rentang waktu tepat 5 menit. Harga yang dijadikan patokan acuan (*benchmark*) untuk setiap 5 menit ke depan adalah **harga cutoff saat 5 menit sebelumnya**. AI secara berkelanjutan melakukan analisis kuantitatif gabungan antara **jangka pendek (1m - 5m micro-momentum)** dan **jangka panjang (15m - 1h - 24h macro-structure)** untuk menghasilkan prediksi arah (UP atau DOWN) dengan target presisi 99.5%.

---

## 2. Logika Inti & Aturan Waktu (WIB / UTC+7)

### 2.1. Siklus Rantai 5 Menit (Continuous Chaining)
- **Rentang Waktu**: Tepat 5 menit per ronde.
- **Patokan Harga Acuan (*Benchmark Price*)**:
  - Ronde $N$ (misal 23:30 - 23:35) menggunakan harga closing cutoff dari Ronde $N-1$ (misal 23:30 seharga $85,400.00).
  - Ronde $N+1$ (misal 23:35 - 23:40) menggunakan harga closing cutoff dari Ronde $N$ (misal 23:35 seharga $85,550.00).
  - Terus bersambung secara otomatis tanpa henti.
- **Penentuan Hasil (*Resolution*)**:
  - Tepat pada detik cutoff (misal 23:35:00 WIB), sistem mengambil harga spot riil Binance.
  - Jika $\text{Harga Akhir} > \text{Harga Patokan}$: Hasil = **UP (▲)**.
  - Jika $\text{Harga Akhir} < \text{Harga Patokan}$: Hasil = **DOWN (▼)**.
  - Jika $\text{Harga Akhir} = \text{Harga Patokan}$: Hasil = **DRAW**.
- **Konsolidasi Pemenang**:
  - Mengevaluasi tebakan Manusia vs prediksi AI.
  - Memperbarui Leaderboard dan Win-rate di SQLite secara persisten.
  - Langsung membuka ronde berikutnya menggunakan harga cutoff barusan sebagai patokan baru.

---

## 3. Analisis AI Kuantitatif (Presisi 99.5%)

Model AI (via endpoint 9router `http://127.0.0.1:20128/v1` dengan model `cbai/glm-5.2` atau deterministic quant engine) mengevaluasi:
1. **Indikator Jangka Pendek (1m - 5m Micro-Flow)**:
   - RSI (14-period) pada timeframe 5m.
   - Exponential Moving Average (EMA 9 vs EMA 21) crossover status.
   - Micro-momentum candle 1m (higher highs / lower lows).
2. **Indikator Jangka Panjang (15m - 1h - 24h Macro)**:
   - Tren persentase 24 jam dan rentang High/Low 24 jam.
   - Volume transaksi 24 jam.
   - Level support dinamis dan resistensi lokal.
3. **Output Terstruktur**:
   - Pilihan: `UP` atau `DOWN`.
   - Confidence: 85 - 99%.
   - Ringkasan Jangka Pendek & Jangka Panjang.
   - Detail Penalaran Kuantitatif.

---

## 4. Arsitektur Komponen

1. **Chainlink CRE Go Workflow (`prediction-workflow/`)**:
   - Pemicu: Cron Schedule 6-field `0 */5 * * * *`.
   - Request HTTP native via capability WASM `creHttp.SendRequest`.
   - Memvalidasi dan mensimulasikan alur kerja secara terdesentralisasi.
2. **Web Server & Continuous Engine (`web/server.go`)**:
   - Engine latar belakang memeriksa tick setiap 2 detik.
   - Otomatis melakukan konsolidasi pada saat `now >= cutoff_at`.
   - Otomatis membuat ronde baru secara berantai (*chained*).
3. **Database Layer (`web/db.go`)**:
   - Driver murni Go SQLite (`modernc.org/sqlite`) berkinerja tinggi tanpa dependensi CGO.
   - Mode WAL (*Write-Ahead Logging*) dengan pragma busy timeout untuk konkurensi optimal.
4. **Interactive Dashboard (`web/static/index.html`)**:
   - Live stream ticker Binance setiap 1 detik.
   - Visualisasi Sparkline chart dengan garis kuning putus-putus (*Batas Patokan Acuan 5m lalu*).
   - Tombol tebakan user interaktif: `[▲ TEBAK UP]` dan `[▼ TEBAK DOWN]`.
   - Countdown progress bar dinamis 300 detik.
   - Leaderboard & Riwayat lengkap dari SQLite.
