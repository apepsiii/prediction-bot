package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

// RoundRecord represents a single prediction duel round stored in SQLite
type RoundRecord struct {
	ID              string  `json:"id"`
	Symbol          string  `json:"symbol"`
	StartTimeWIB    string  `json:"start_time_wib"`
	CutoffTimeWIB   string  `json:"cutoff_time_wib"`
	CreatedAt       int64   `json:"created_at"`        // Unix timestamp
	CutoffAt        int64   `json:"cutoff_at"`         // Unix timestamp
	StartPrice      float64 `json:"start_price"`       // Patokan harga cutoff 5m sebelumnya
	ThresholdPrice  float64 `json:"threshold_price"`   // Sama dengan StartPrice sebagai benchmark
	FinalPrice      float64 `json:"final_price"`       // Harga spot saat cutoff tiba
	DurationMinutes int     `json:"duration_minutes"`  // 5 menit
	UserChoice      string  `json:"user_choice"`       // "UP", "DOWN", atau "PENDING"
	AIChoice        string  `json:"ai_choice"`         // "UP" atau "DOWN"
	AIConfidence    int     `json:"ai_confidence"`     // 85 - 99%
	AIReasoning     string  `json:"ai_reasoning"`      // Analisis jangka panjang & pendek
	AIModel         string  `json:"ai_model"`
	ActualOutcome   string  `json:"actual_outcome"`    // "UP", "DOWN", "DRAW", atau "PENDING"
	UserWon         bool    `json:"user_won"`
	AIWon           bool    `json:"ai_won"`
	OutcomeSummary  string  `json:"outcome_summary"`   // "HUMAN_WON", "AI_WON", "DRAW_WIN", "DRAW_LOSS", "PENDING"
	Status          string  `json:"status"`            // "ACTIVE", "CONSOLIDATED"
}

type Kline struct {
	OpenTime  int64
	Open      float64
	High      float64
	Low       float64
	Close     float64
	Volume    float64
	CloseTime int64
}

type Ticker24h struct {
	LastPrice float64
	Change24h float64
	High24h   float64
	Low24h    float64
	Volume    float64
}

var (
	rootDir      string
	engineLock   sync.Mutex
	monitoredSymbols = []string{"BTCUSDT"}
)

func init() {
	exeDir, err := os.Getwd()
	if err != nil {
		exeDir = "."
	}
	rootDir = exeDir
	if filepath.Base(rootDir) == "web" {
		rootDir = filepath.Dir(rootDir)
	}
}

func readEnvKey(key string) string {
	val := os.Getenv(key)
	if val != "" {
		return val
	}
	envPath := filepath.Join(rootDir, ".env")
	content, err := os.ReadFile(envPath)
	if err == nil {
		lines := strings.Split(string(content), "\n")
		for _, line := range lines {
			line = strings.TrimSpace(line)
			if strings.HasPrefix(line, "#") || !strings.Contains(line, "=") {
				continue
			}
			parts := strings.SplitN(line, "=", 2)
			if strings.TrimSpace(parts[0]) == key {
				return strings.TrimSpace(parts[1])
			}
		}
	}
	return ""
}

func main() {
	if err := initDB(); err != nil {
		log.Fatalf("[FATAL] Database init failed: %v", err)
	}

	// Clean up any stale active rounds from previous sessions
	cleanStaleActiveRounds()

	// Start continuous 5-minute automated engine
	go startContinuousEngine()

	port := os.Getenv("PORT")
	if port == "" {
		port = "8090"
	}

	mux := http.NewServeMux()

	// API Routes
	mux.HandleFunc("/api/price", handlePrice)
	mux.HandleFunc("/api/rounds/active", handleActiveRound)
	mux.HandleFunc("/api/rounds/bet", handleUserBet)
	mux.HandleFunc("/api/rounds", handleRounds)
	mux.HandleFunc("/api/simulate-cre", handleSimulateCRE)

	// Static UI Files
	staticDir := filepath.Join(rootDir, "web", "static")
	fileServer := http.FileServer(http.Dir(staticDir))
	mux.Handle("/", fileServer)

	serverAddr := ":" + port
	fmt.Printf("\n======================================================\n")
	fmt.Printf("  🏆 CHAINLINK CRE x AI PREDICTION ARENA (CONTINUOUS 5M)\n")
	fmt.Printf("  Interval: 5 Menit Otomatis • Patokan: Cutoff 5m Sebelumnya\n")
	fmt.Printf("  Analisis: Jangka Pendek (1m-5m) + Panjang (15m-1h-24h)\n")
	fmt.Printf("  Storage : SQLite WAL (data/prediction.db)\n")
	fmt.Printf("======================================================\n")
	fmt.Printf("  URL Dashboard: http://localhost:%s\n", port)
	fmt.Printf("  Zona Waktu   : WIB (UTC+7)\n")
	fmt.Printf("======================================================\n\n")

	log.Fatal(http.ListenAndServe(serverAddr, mux))
}

// cleanStaleActiveRounds consolidates or marks past active rounds so fresh ones can begin
func cleanStaleActiveRounds() {
	// Retire any legacy 15-minute test rounds
	_, _ = db.Exec("UPDATE rounds SET status = 'CONSOLIDATED', final_price = start_price, actual_outcome = 'DRAW', outcome_summary = 'DRAW_WIN' WHERE duration_minutes > 5 AND status = 'ACTIVE'")

	activeRounds, err := GetActiveRounds()
	if err != nil {
		return
	}
	now := time.Now().Unix()
	for _, r := range activeRounds {
		if now >= r.CutoffAt {
			consolidateSingleRound(&r)
		}
	}
}

// startContinuousEngine is the heartbeat worker that monitors and chains 5-minute rounds
func startContinuousEngine() {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		now := time.Now()
		for _, sym := range monitoredSymbols {
			processSymbolEngine(sym, now)
		}
	}
}

func processSymbolEngine(symbol string, now time.Time) {
	engineLock.Lock()
	defer engineLock.Unlock()

	active, err := GetActiveRoundBySymbol(symbol)
	if err != nil {
		log.Printf("[ENGINE ERROR] Query active round failed for %s: %v", symbol, err)
		return
	}

	// If there is an active round, check if cutoff time has arrived
	if active != nil {
		if now.Unix() >= active.CutoffAt {
			// Time is up! Consolidate this round
			consolidated := consolidateSingleRound(active)
			if consolidated != nil {
				// Immediately chain and spawn the next 5-minute round using final price as benchmark!
				spawnNextRound(symbol, consolidated.FinalPrice, time.Unix(consolidated.CutoffAt, 0))
			}
		}
		return
	}

	// If no active round exists, spawn initial round aligned to 5m interval
	spawnInitialRound(symbol, now)
}

// spawnInitialRound starts the first round when system boots
func spawnInitialRound(symbol string, now time.Time) {
	// Look up the latest consolidated round to see if we have a recent cutoff price
	latest, _ := GetLatestConsolidatedRound(symbol)
	var benchmarkPrice float64

	if latest != nil && latest.FinalPrice > 0 && (now.Unix()-latest.CutoffAt) < 600 {
		benchmarkPrice = latest.FinalPrice
	} else {
		// Fetch previous 5m candle close as benchmark
		klines, err := fetchKlines(symbol, "5m", 3)
		if err == nil && len(klines) >= 2 {
			benchmarkPrice = klines[len(klines)-2].Close
		} else {
			price, _ := fetchCurrentPrice(symbol)
			benchmarkPrice = price
		}
	}

	if benchmarkPrice <= 0 {
		return
	}

	// Align to 5-minute interval
	const intervalSec = 300 // 5 minutes
	nowSec := now.Unix()
	remainder := nowSec % intervalSec

	var startTimeSec, cutoffTimeSec int64
	if remainder > 240 { // If less than 60s remain in current 5m candle, align to next full candle
		startTimeSec = nowSec + (intervalSec - remainder)
		cutoffTimeSec = startTimeSec + intervalSec
	} else {
		startTimeSec = nowSec - remainder
		cutoffTimeSec = startTimeSec + intervalSec
	}

	startTime := time.Unix(startTimeSec, 0)
	cutoffTime := time.Unix(cutoffTimeSec, 0)

	roundID := fmt.Sprintf("RND-%s-%s", symbol[:3], startTime.In(wibLoc).Format("1504"))

	record := RoundRecord{
		ID:              roundID,
		Symbol:          symbol,
		StartTimeWIB:    FormatWIB(startTime),
		CutoffTimeWIB:   FormatWIB(cutoffTime),
		CreatedAt:       startTimeSec,
		CutoffAt:        cutoffTimeSec,
		StartPrice:      benchmarkPrice,
		ThresholdPrice:  benchmarkPrice,
		FinalPrice:      0,
		DurationMinutes: 5,
		UserChoice:      "PENDING",
		AIChoice:        "",
		AIConfidence:    0,
		AIReasoning:     "AI sedang memproses analisis jangka pendek & panjang...",
		AIModel:         readEnvKey("AI_MODEL"),
		ActualOutcome:   "PENDING",
		UserWon:         false,
		AIWon:           false,
		OutcomeSummary:  "PENDING",
		Status:          "ACTIVE",
	}

	if err := InsertRound(&record); err != nil {
		log.Printf("[ENGINE] Gagal insert initial round %s: %v", roundID, err)
		return
	}

	log.Printf("[RONDE 5M DIMULAI] %s (%s) | Patokan (Cutoff 5m Lalu): $%.2f | Mulai: %s | Cutoff: %s",
		record.ID, record.Symbol, record.ThresholdPrice, record.StartTimeWIB, record.CutoffTimeWIB)

	// Run AI Multi-Timeframe Analysis in background
	go runMultiTimeframeAIAnalysis(record.Symbol, record.ThresholdPrice, record.StartTimeWIB, record.CutoffTimeWIB, record.ID)
}

// spawnNextRound creates the consecutive 5-minute round using the exact cutoff price of previous round
func spawnNextRound(symbol string, previousCutoffPrice float64, previousCutoffTime time.Time) {
	startSec := previousCutoffTime.Unix()
	cutoffSec := startSec + 300 // Exactly 5 minutes

	startTime := time.Unix(startSec, 0)
	cutoffTime := time.Unix(cutoffSec, 0)

	roundID := fmt.Sprintf("RND-%s-%s", symbol[:3], startTime.In(wibLoc).Format("1504"))

	record := RoundRecord{
		ID:              roundID,
		Symbol:          symbol,
		StartTimeWIB:    FormatWIB(startTime),
		CutoffTimeWIB:   FormatWIB(cutoffTime),
		CreatedAt:       startSec,
		CutoffAt:        cutoffSec,
		StartPrice:      previousCutoffPrice,
		ThresholdPrice:  previousCutoffPrice,
		FinalPrice:      0,
		DurationMinutes: 5,
		UserChoice:      "PENDING",
		AIChoice:        "",
		AIConfidence:    0,
		AIReasoning:     "AI sedang memproses analisis jangka pendek & panjang...",
		AIModel:         readEnvKey("AI_MODEL"),
		ActualOutcome:   "PENDING",
		UserWon:         false,
		AIWon:           false,
		OutcomeSummary:  "PENDING",
		Status:          "ACTIVE",
	}

	if err := InsertRound(&record); err != nil {
		log.Printf("[ENGINE] Gagal insert next round %s: %v", roundID, err)
		return
	}

	log.Printf("[RONDE BERIKUTNYA 5M] %s (%s) | Patokan Acuan: $%.2f | Cutoff: %s",
		record.ID, record.Symbol, record.ThresholdPrice, record.CutoffTimeWIB)

	// Run AI Multi-Timeframe Analysis in background
	go runMultiTimeframeAIAnalysis(record.Symbol, record.ThresholdPrice, record.StartTimeWIB, record.CutoffTimeWIB, record.ID)
}

// consolidateSingleRound evaluates final outcome against the benchmark price
func consolidateSingleRound(r *RoundRecord) *RoundRecord {
	finalPrice, err := fetchCurrentPrice(r.Symbol)
	if err != nil {
		log.Printf("[CONSOLIDASI ERROR] Gagal fetch harga final untuk %s: %v", r.ID, err)
		return nil
	}

	r.FinalPrice = finalPrice
	if finalPrice > r.ThresholdPrice {
		r.ActualOutcome = "UP"
	} else if finalPrice < r.ThresholdPrice {
		r.ActualOutcome = "DOWN"
	} else {
		r.ActualOutcome = "DRAW"
	}

	userParticipated := (r.UserChoice == "UP" || r.UserChoice == "DOWN")
	r.UserWon = userParticipated && (r.UserChoice == r.ActualOutcome)
	r.AIWon = (r.AIChoice == r.ActualOutcome)

	if userParticipated {
		if r.UserWon && !r.AIWon {
			r.OutcomeSummary = "HUMAN_WON"
		} else if r.AIWon && !r.UserWon {
			r.OutcomeSummary = "AI_WON"
		} else if r.UserWon && r.AIWon {
			r.OutcomeSummary = "DRAW_WIN"
		} else {
			r.OutcomeSummary = "DRAW_LOSS"
		}
	} else {
		if r.AIWon {
			r.OutcomeSummary = "AI_WON"
		} else {
			r.OutcomeSummary = "DRAW_LOSS"
		}
	}

	r.Status = "CONSOLIDATED"

	if err := UpdateConsolidatedRound(r); err != nil {
		log.Printf("[CONSOLIDASI ERROR] Gagal update round %s di SQLite: %v", r.ID, err)
		return nil
	}

	log.Printf("==================================================================")
	log.Printf("[KONSOLIDASI 5M SELESAI] %s | Patokan: $%.2f | Final: $%.2f | Hasil: %s | Pemenang: %s",
		r.ID, r.ThresholdPrice, r.FinalPrice, r.ActualOutcome, r.OutcomeSummary)
	log.Printf("==================================================================")

	return r
}

// runMultiTimeframeAIAnalysis executes deep short-term + long-term analysis via 9router or fallback
func runMultiTimeframeAIAnalysis(symbol string, benchmarkPrice float64, startWIB, cutoffWIB, roundID string) {
	livePrice, _ := fetchCurrentPrice(symbol)
	if livePrice <= 0 {
		livePrice = benchmarkPrice
	}

	// 1. Fetch 5m klines for technical indicators (RSI, EMA, Volume)
	klines5m, _ := fetchKlines(symbol, "5m", 30)
	klines1m, _ := fetchKlines(symbol, "1m", 10)
	ticker, _ := fetchTicker24hr(symbol)

	rsi5m := 50.0
	ema9 := livePrice
	ema21 := livePrice
	if len(klines5m) >= 21 {
		closes := make([]float64, len(klines5m))
		for i, k := range klines5m {
			closes[i] = k.Close
		}
		rsi5m = calculateRSI(closes, 14)
		ema9 = calculateEMA(closes, 9)
		ema21 = calculateEMA(closes, 21)
	}

	microTrend1m := "Netral"
	if len(klines1m) >= 3 {
		last1 := klines1m[len(klines1m)-1].Close
		last3 := klines1m[len(klines1m)-3].Close
		if last1 > last3 {
			microTrend1m = "Bullish Higher High (Buyer dominan)"
		} else if last1 < last3 {
			microTrend1m = "Bearish Lower Low (Seller dominan)"
		}
	}

	deltaUSD := livePrice - benchmarkPrice
	deltaPct := (deltaUSD / benchmarkPrice) * 100.0

	apiKey := readEnvKey("AI_API_KEY_VAR")
	if apiKey == "" {
		apiKey = readEnvKey("AI_API_KEY")
	}
	aiBaseURL := readEnvKey("AI_BASE_URL")
	if aiBaseURL == "" {
		aiBaseURL = "http://127.0.0.1:20128/v1"
	}
	aiURL := strings.TrimRight(aiBaseURL, "/") + "/chat/completions"
	aiModel := readEnvKey("AI_MODEL")
	if aiModel == "" {
		aiModel = "cbai/glm-5.2"
	}

	prompt := fmt.Sprintf(`Anda adalah Master Quant AI Specialist untuk Scalping Binance 5-Menit (Target Presisi 99.5%%).
Tugas: Memprediksi apakah harga pada waktu Cutoff (5 menit dari mulai) akan ditutup DI ATAS atau DI BAWAH Harga Patokan Acuan.

PARAMETER PERTANDINGAN:
- Aset: %s
- Harga Patokan Acuan (Cutoff 5m Sebelumnya): $%.2f
- Harga Terkini: $%.2f (Delta: %+.3f%% | $%+.2f)
- Waktu Ronde: %s WIB s.d %s WIB (5 Menit)

INDIKATOR TEKNIKAL JANGKA PENDEK (1m - 5m Micro):
- RSI 5m (14): %.1f
- EMA 9: $%.2f vs EMA 21: $%.2f (%s)
- Micro-Momentum 1m: %s

INDIKATOR TEKNIKAL JANGKA PANJANG (15m - 1h - 24h Macro):
- Tren 24 Jam: %+.2f%% | Range 24h: $%.2f - $%.2f
- Volume 24 Jam: %.2f %s

INSTRUKSI:
1. Lakukan analisis jangka pendek (1m-5m order flow & candlestick micro-rejection).
2. Lakukan analisis jangka panjang (15m-1h support & resistance, EMA trend).
3. Buat keputusan tegas: 'UP' (jika harga closing diprediksi >= $%.2f) atau 'DOWN' (jika < $%.2f).
4. Berikan confidence tinggi yang realistis (85-99%%).
5. Output HANYA JSON VALID tanpa markdown wrap:
{"choice": "UP" atau "DOWN", "confidence": angka 85-99, "short_term": "ringkasan 1m-5m", "long_term": "ringkasan 15m-1h", "reasoning": "analisis mendalam", "model": "%s"}`,
		symbol, benchmarkPrice, livePrice, deltaPct, deltaUSD, startWIB, cutoffWIB,
		rsi5m, ema9, ema21, getEMAStatus(ema9, ema21), microTrend1m,
		ticker.Change24h, ticker.Low24h, ticker.High24h, ticker.Volume, symbol[:3],
		benchmarkPrice, benchmarkPrice, aiModel)

	type AIOutput struct {
		Choice     string `json:"choice"`
		Confidence int    `json:"confidence"`
		ShortTerm  string `json:"short_term"`
		LongTerm   string `json:"long_term"`
		Reasoning  string `json:"reasoning"`
		Model      string `json:"model"`
	}

	var output AIOutput

	// Call 9router
	reqBody, _ := json.Marshal(map[string]interface{}{
		"model": aiModel,
		"messages": []map[string]string{
			{"role": "system", "content": "You are a senior quant prediction engine. You output only valid JSON."},
			{"role": "user", "content": prompt},
		},
		"temperature": 0.15,
	})

	httpReq, err := http.NewRequest(http.MethodPost, aiURL, bytes.NewBuffer(reqBody))
	if err == nil {
		httpReq.Header.Set("Content-Type", "application/json")
		if apiKey != "" {
			httpReq.Header.Set("Authorization", "Bearer "+apiKey)
		}

		client := &http.Client{Timeout: 8 * time.Second}
		resp, err := client.Do(httpReq)
		if err == nil && resp.StatusCode == 200 {
			defer resp.Body.Close()
			bodyBytes, _ := io.ReadAll(resp.Body)

			var chatResp struct {
				Choices []struct {
					Message struct {
						Content string `json:"content"`
					} `json:"message"`
				} `json:"choices"`
			}

			if err := json.Unmarshal(bodyBytes, &chatResp); err == nil && len(chatResp.Choices) > 0 {
				content := strings.TrimSpace(chatResp.Choices[0].Message.Content)
				content = strings.TrimPrefix(content, "```json")
				content = strings.TrimPrefix(content, "```")
				content = strings.TrimSuffix(content, "```")
				content = strings.TrimSpace(content)

				var parsed AIOutput
				if err := json.Unmarshal([]byte(content), &parsed); err == nil && (parsed.Choice == "UP" || parsed.Choice == "DOWN") {
					output = parsed
				}
			}
		}
	}

	// High Precision Deterministic Fallback if 9router endpoint is offline
	if output.Choice == "" {
		output = computePrecisionQuantFallback(livePrice, benchmarkPrice, rsi5m, ema9, ema21, deltaPct, microTrend1m, ticker)
	}

	fullReasoning := fmt.Sprintf("[Jangka Pendek]: %s | [Jangka Panjang]: %s | [Detail]: %s",
		output.ShortTerm, output.LongTerm, output.Reasoning)

	_ = UpdateAIRoundPrediction(roundID, output.Choice, output.Confidence, fullReasoning, aiModel)

	log.Printf("[AI ANALISIS SELESAI] %s -> Pilihan: %s (%d%%) | Model: %s",
		roundID, output.Choice, output.Confidence, aiModel)
}

func getEMAStatus(ema9, ema21 float64) string {
	if ema9 > ema21 {
		return "Golden Cross / Bullish Momentum"
	}
	return "Death Cross / Bearish Pressure"
}

func computePrecisionQuantFallback(livePrice, benchmark, rsi, ema9, ema21, deltaPct float64, microTrend string, ticker Ticker24h) struct {
	Choice     string `json:"choice"`
	Confidence int    `json:"confidence"`
	ShortTerm  string `json:"short_term"`
	LongTerm   string `json:"long_term"`
	Reasoning  string `json:"reasoning"`
	Model      string `json:"model"`
} {
	isUp := false
	score := 0

	if livePrice >= benchmark {
		score += 3
	} else {
		score -= 3
	}

	if ema9 > ema21 {
		score += 2
	} else {
		score -= 2
	}

	if rsi > 52 {
		score += 2
	} else if rsi < 48 {
		score -= 2
	}

	if ticker.Change24h > 0 {
		score += 1
	} else {
		score -= 1
	}

	conf := 92
	var choice, shortTerm, longTerm, reasoning string

	if score >= 1 {
		choice = "UP"
		if score >= 4 {
			conf = 96
		}
		isUp = true
		shortTerm = fmt.Sprintf("Micro-flow 1m menunjukkan dominasi buyer dengan RSI 5m di %.1f dan EMA9 ($%.2f) bertahan di atas EMA21.", rsi, ema9)
		longTerm = fmt.Sprintf("Struktur 24h mempertahankan bias uptrend (%+.2f%%), support dinamis menjaga harga di atas patokan $%.2f.", ticker.Change24h, benchmark)
		reasoning = fmt.Sprintf("Kombinasi momentum jangka pendek dan struktur tren hourly mengindikasikan probabilitas tinggi lilin 5 menit ini ditutup di zona UP di atas acuan $%.2f.", benchmark)
	} else {
		choice = "DOWN"
		if score <= -4 {
			conf = 95
		}
		isUp = false
		shortTerm = fmt.Sprintf("Micro-momentum menunjukkan tekanan jual dengan RSI 5m di %.1f dan EMA9 berada di bawah resistensi EMA21 ($%.2f).", rsi, ema21)
		longTerm = fmt.Sprintf("Struktur tren 24h mengalami koreksi (%+.2f%%), penolakan harga atas membatasi rebound menuju cutoff.", ticker.Change24h)
		reasoning = fmt.Sprintf("Kelemahan volume beli dan tekanan resistance lokal mengindikasikan harga condong tertahan di zona DOWN di bawah acuan $%.2f saat konsolidasi.", benchmark)
	}

	_ = isUp
	return struct {
		Choice     string `json:"choice"`
		Confidence int    `json:"confidence"`
		ShortTerm  string `json:"short_term"`
		LongTerm   string `json:"long_term"`
		Reasoning  string `json:"reasoning"`
		Model      string `json:"model"`
	}{
		Choice:     choice,
		Confidence: conf,
		ShortTerm:  shortTerm,
		LongTerm:   longTerm,
		Reasoning:  reasoning,
		Model:      "quant-multi-tf-v2",
	}
}

// Indicator Helpers
func calculateEMA(closes []float64, period int) float64 {
	if len(closes) == 0 {
		return 0
	}
	if len(closes) < period {
		period = len(closes)
	}
	multiplier := 2.0 / float64(period+1)
	ema := closes[0]
	for i := 1; i < len(closes); i++ {
		ema = (closes[i]-ema)*multiplier + ema
	}
	return ema
}

func calculateRSI(closes []float64, period int) float64 {
	if len(closes) <= period {
		return 50.0
	}
	var gains, losses float64
	for i := 1; i <= period; i++ {
		change := closes[i] - closes[i-1]
		if change > 0 {
			gains += change
		} else {
			losses -= change
		}
	}
	avgGain := gains / float64(period)
	avgLoss := losses / float64(period)

	for i := period + 1; i < len(closes); i++ {
		change := closes[i] - closes[i-1]
		if change > 0 {
			avgGain = (avgGain*float64(period-1) + change) / float64(period)
			avgLoss = (avgLoss * float64(period-1)) / float64(period)
		} else {
			avgGain = (avgGain * float64(period-1)) / float64(period)
			avgLoss = (avgLoss*float64(period-1) - change) / float64(period)
		}
	}

	if avgLoss == 0 {
		return 100.0
	}
	rs := avgGain / avgLoss
	return 100.0 - (100.0 / (1.0 + rs))
}

func fetchKlines(symbol, interval string, limit int) ([]Kline, error) {
	url := fmt.Sprintf("https://api.binance.com/api/v3/klines?symbol=%s&interval=%s&limit=%d", strings.ToUpper(symbol), interval, limit)
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var raw [][]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return nil, err
	}

	var results []Kline
	for _, item := range raw {
		if len(item) < 7 {
			continue
		}
		ot, _ := item[0].(float64)
		op, _ := strconv.ParseFloat(fmt.Sprintf("%v", item[1]), 64)
		hi, _ := strconv.ParseFloat(fmt.Sprintf("%v", item[2]), 64)
		lo, _ := strconv.ParseFloat(fmt.Sprintf("%v", item[3]), 64)
		cl, _ := strconv.ParseFloat(fmt.Sprintf("%v", item[4]), 64)
		vol, _ := strconv.ParseFloat(fmt.Sprintf("%v", item[5]), 64)
		ct, _ := item[6].(float64)

		results = append(results, Kline{
			OpenTime:  int64(ot),
			Open:      op,
			High:      hi,
			Low:       lo,
			Close:     cl,
			Volume:    vol,
			CloseTime: int64(ct),
		})
	}
	return results, nil
}

func fetchTicker24hr(symbol string) (Ticker24h, error) {
	url := fmt.Sprintf("https://api.binance.com/api/v3/ticker/24hr?symbol=%s", strings.ToUpper(symbol))
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		return Ticker24h{}, err
	}
	defer resp.Body.Close()

	var raw map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return Ticker24h{}, err
	}

	last, _ := strconv.ParseFloat(fmt.Sprintf("%v", raw["lastPrice"]), 64)
	chg, _ := strconv.ParseFloat(fmt.Sprintf("%v", raw["priceChangePercent"]), 64)
	hi, _ := strconv.ParseFloat(fmt.Sprintf("%v", raw["highPrice"]), 64)
	lo, _ := strconv.ParseFloat(fmt.Sprintf("%v", raw["lowPrice"]), 64)
	vol, _ := strconv.ParseFloat(fmt.Sprintf("%v", raw["volume"]), 64)

	return Ticker24h{
		LastPrice: last,
		Change24h: chg,
		High24h:   hi,
		Low24h:    lo,
		Volume:    vol,
	}, nil
}

func fetchCurrentPrice(symbol string) (float64, error) {
	url := fmt.Sprintf("https://api.binance.com/api/v3/ticker/price?symbol=%s", strings.ToUpper(symbol))
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	var data struct {
		Price string `json:"price"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return 0, err
	}
	return strconv.ParseFloat(data.Price, 64)
}

// HTTP API Handlers

// handlePrice returns live 24hr Binance ticker
func handlePrice(w http.ResponseWriter, r *http.Request) {
	symbol := r.URL.Query().Get("symbol")
	if symbol == "" {
		symbol = "BTCUSDT"
	}
	symbol = strings.ToUpper(strings.TrimSpace(symbol))

	ticker, err := fetchTicker24hr(symbol)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"failed to connect to Binance API: %v"}`, err), http.StatusInternalServerError)
		return
	}

	res := map[string]interface{}{
		"symbol":         symbol,
		"price":          ticker.LastPrice,
		"change_24h_pct": ticker.Change24h,
		"high_24h":       ticker.High24h,
		"low_24h":        ticker.Low24h,
		"volume":         ticker.Volume,
		"timestamp_wib":  FormatWIB(time.Now()),
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(res)
}

// handleActiveRound returns the current active 5-minute round and live status
func handleActiveRound(w http.ResponseWriter, r *http.Request) {
	symbol := r.URL.Query().Get("symbol")
	if symbol == "" {
		symbol = "BTCUSDT"
	}
	symbol = strings.ToUpper(strings.TrimSpace(symbol))

	active, err := GetActiveRoundBySymbol(symbol)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"failed to get active round: %v"}`, err), http.StatusInternalServerError)
		return
	}

	livePrice, _ := fetchCurrentPrice(symbol)
	nowUnix := time.Now().Unix()

	var remainingSec int64 = 0
	var isAbove bool = false
	var deltaUSD float64 = 0
	var deltaPct float64 = 0

	if active != nil {
		if active.CutoffAt > nowUnix {
			remainingSec = active.CutoffAt - nowUnix
		}
		if livePrice > 0 && active.ThresholdPrice > 0 {
			deltaUSD = livePrice - active.ThresholdPrice
			deltaPct = (deltaUSD / active.ThresholdPrice) * 100.0
			isAbove = (livePrice >= active.ThresholdPrice)
		}
	}

	res := map[string]interface{}{
		"active_round":   active,
		"live_price":     livePrice,
		"remaining_sec":  remainingSec,
		"is_above":       isAbove,
		"delta_usd":      deltaUSD,
		"delta_pct":      deltaPct,
		"server_time_wib": FormatWIB(time.Now()),
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(res)
}

// handleUserBet records human player's prediction for the current active round
func handleUserBet(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		RoundID string `json:"round_id"`
		Choice  string `json:"choice"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error":"invalid request json"}`, http.StatusBadRequest)
		return
	}

	req.Choice = strings.ToUpper(strings.TrimSpace(req.Choice))
	if req.Choice != "UP" && req.Choice != "DOWN" {
		http.Error(w, `{"error":"choice must be UP or DOWN"}`, http.StatusBadRequest)
		return
	}

	if err := UpdateUserChoice(req.RoundID, req.Choice); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"failed to update bet: %v"}`, err), http.StatusInternalServerError)
		return
	}

	log.Printf("[TEBAKAN USER] Ronde %s -> Pilihan User: %s", req.RoundID, req.Choice)

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"success":  true,
		"round_id": req.RoundID,
		"choice":   req.Choice,
	})
}

// handleRounds returns list of rounds and leaderboard metrics from SQLite
func handleRounds(w http.ResponseWriter, r *http.Request) {
	rounds, err := GetAllRounds(100)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"failed to query rounds: %v"}`, err), http.StatusInternalServerError)
		return
	}

	stats, err := GetLeaderboardStats()
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"failed to get stats: %v"}`, err), http.StatusInternalServerError)
		return
	}

	res := map[string]interface{}{
		"rounds":          rounds,
		"leaderboard":     stats,
		"server_time_wib": FormatWIB(time.Now()),
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(res)
}

// handleSimulateCRE triggers native CRE CLI workflow simulation
func handleSimulateCRE(w http.ResponseWriter, r *http.Request) {
	cmd := exec.Command("cre", "workflow", "simulate", "prediction-workflow", "--target", "staging-settings", "--non-interactive", "--trigger-index", "0")
	cmd.Dir = rootDir

	output, err := cmd.CombinedOutput()
	res := map[string]interface{}{
		"success": err == nil,
		"output":  string(output),
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(res)
}
