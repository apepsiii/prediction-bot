# Hermes & AI Agent Context - Chainlink CRE Go Prediction Bot

Dokumen ini adalah panduan operasional (*Context File*) untuk agen AI/Hermes di masa depan yang akan mengembangkan, menguji, atau mengelola bot ini.

---

## 1. Lingkungan & Toolchain
- **Platform**: Chainlink Runtime Environment (CRE) CLI v1.37.0+.
- **Bahasa**: Go 1.25.6 (dikompilasi ke target `wasip1` WebAssembly).
- **Direktori Proyek**: `E:\apep\WEB\cre-prediction-bot`.
- **Target Target Config**: `prediction-workflow/config.staging.json` dan `prediction-workflow/config.production.json`.
- **Secrets Mapping**: `secrets.yaml` memetakan ID secret `AI_API_KEY` ke environment variable `AI_API_KEY_VAR` yang didefinisikan dalam `.env`.

---

## 2. Perintah Operasional Penting

### Menjalankan Simulasi Lokal (Receiver-Free)
Jalankan selalu dari **root direktori proyek** (`E:\apep\WEB\cre-prediction-bot`):
```powershell
cre workflow simulate prediction-workflow --target staging-settings --non-interactive --trigger-index 0
```

### Menjalankan Unit Test Go
```powershell
go test ./...
```

### Verifikasi Sesi & Akun CRE
```powershell
cre whoami
```

### Menyesuaikan API Key 9router
Tambahkan atau ubah pada file `.env`:
```env
AI_API_KEY_VAR=your-actual-9router-key
```
Workflow akan otomatis membaca secret tersebut via `runtime.GetSecret(&cre.SecretRequest{Id: "AI_API_KEY"})`.

---

## 3. Aturan Arsitektur & Aturan Kode (Invariants)
1. **WASM Guest Compatibility**:
   - Kode Go dikompilasi untuk `wasip1`. Jangan gunakan pemanggilan *raw socket* `net/http` secara langsung karena DNS loopback akan ditolak di environment guest WASM.
   - Semua pemanggilan HTTP wajib menggunakan capability resmi:
     `github.com/smartcontractkit/cre-sdk-go/capabilities/networking/http`
     melalui `creHttp.SendRequest(...)` dengan `cre.ConsensusIdenticalAggregation` atau `cre.ConsensusMedianAggregation`.
2. **Determinisme & Waktu**:
   - Gunakan `runtime.Now()` untuk timestamp konsensus, bukan `time.Now()`.
   - Gunakan `runtime.Rand()` jika memerlukan keacakan.
3. **Format Cron 6-Field**:
   - Jadwal CRE wajib memiliki 6 field: `detik menit jam hari-bulan bulan hari-minggu`.
   - Contoh untuk 5 menit: `0 */5 * * * *`.
4. **Resiliensi AI**:
   - Fungsi `queryAIOrFallback` harus selalu memiliki *fallback* deterministik yang valid agar simulasi tidak pernah *panic* atau *hang* jika 9router lokal offline atau jika model merespons lambat.
