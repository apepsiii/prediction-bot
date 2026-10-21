package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
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

	// 3-Stage Multi-Observation Pipeline & Final Verdict
	AIStage1Choice  string  `json:"ai_stage1_choice"`  // Detik 0
	AIStage1Conf    int     `json:"ai_stage1_conf"`
	AIStage1Reason  string  `json:"ai_stage1_reason"`
	AIStage2Choice  string  `json:"ai_stage2_choice"`  // Detik 30
	AIStage2Conf    int     `json:"ai_stage2_conf"`
	AIStage2Reason  string  `json:"ai_stage2_reason"`
	AIStage3Choice  string  `json:"ai_stage3_choice"`  // Detik 60
	AIStage3Conf    int     `json:"ai_stage3_conf"`
	AIStage3Reason  string  `json:"ai_stage3_reason"`
	WhaleAlert      string  `json:"whale_alert"`       // Peringatan pergerakan paus jika terdeteksi

	// Mid-Round Informational Radar (Menit 2, 3, dan 4)
	AIM2Info string `json:"ai_m2_info"` // Radar Menit 2 (T=120s)
	AIM3Info string `json:"ai_m3_info"` // Radar Menit 3 (T=180s)
	AIM4Info string `json:"ai_m4_info"` // Radar Menit 4 (T=240s)
}

type Kline struct {
	OpenTime       int64
	Open           float64
	High           float64
	Low            float64
	Close          float64
	Volume         float64
	CloseTime      int64
	TakerBuyVolume float64
}

type OrderBookDepth struct {
	BidVolume float64 `json:"bid_volume"`
	AskVolume float64 `json:"ask_volume"`
	Ratio     float64 `json:"ratio"`
	Status    string  `json:"status"`
}

type BinaryQuantScore struct {
	Score          int     `json:"score"`
	Choice         string  `json:"choice"`
	Confidence     int     `json:"confidence"`
	OrderBookRatio float64 `json:"order_book_ratio"`
	TakerRatio1m   float64 `json:"taker_ratio_1m"`
	TakerRatio5m   float64 `json:"taker_ratio_5m"`
	StochK         float64 `json:"stoch_k"`
	StochD         float64 `json:"stoch_d"`
	RSI5m          float64 `json:"rsi_5m"`
	ShortTerm      string  `json:"short_term"`
	LongTerm       string  `json:"long_term"`
	Reasoning      string  `json:"reasoning"`
}

type Ticker24h struct {
	LastPrice float64
	Change24h float64
	High24h   float64
	Low24h    float64
	Volume    float64
}

var (
	rootDir             string
	engineLock          sync.Mutex
	monitoredSymbols    = []string{"BTCUSDT"}
	activeWorkflowsLock sync.Mutex
	activeWorkflows     = make(map[string]bool)
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
		} else {
			// Ensure AI workflow is running for this active round if not completed yet
			activeWorkflowsLock.Lock()
			running := activeWorkflows[active.ID]
			activeWorkflowsLock.Unlock()
			if !running && active.AIM4Info == "" {
				go runThreeStageAIWorkflow(active.Symbol, active.ThresholdPrice, active.StartTimeWIB, active.CutoffTimeWIB, active.ID, active.CreatedAt)
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
		AIReasoning:     "AI sedang memproses observasi 3-tahap (Detik 0, 30, 60)...",
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

	// Run AI 3-Stage Workflow in background
	go runThreeStageAIWorkflow(record.Symbol, record.ThresholdPrice, record.StartTimeWIB, record.CutoffTimeWIB, record.ID, record.CreatedAt)
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
		AIReasoning:     "AI sedang memproses observasi 3-tahap (Detik 0, 30, 60)...",
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

	// Run AI 3-Stage Workflow in background
	go runThreeStageAIWorkflow(record.Symbol, record.ThresholdPrice, record.StartTimeWIB, record.CutoffTimeWIB, record.ID, record.CreatedAt)
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

type StageEvaluation struct {
	Choice     string  `json:"choice"`
	Confidence int     `json:"confidence"`
	ShortTerm  string  `json:"short_term"`
	LongTerm   string  `json:"long_term"`
	Reasoning  string  `json:"reasoning"`
	DepthRatio float64 `json:"depth_ratio"`
	TakerRatio float64 `json:"taker_ratio"`
	LivePrice  float64 `json:"live_price"`
	StochK     float64 `json:"stoch_k"`
	MicroTrend string  `json:"micro_trend"`
	WhaleAlert string  `json:"whale_alert"`
	Model      string  `json:"model"`
}

// evaluateMarketStage conducts an observation snapshot for a specific stage (Stage 1, 2, or 3)
func evaluateMarketStage(symbol string, benchmarkPrice float64, stageNum int, prevDepthRatio float64) StageEvaluation {
	livePrice, _ := fetchCurrentPrice(symbol)
	if livePrice <= 0 {
		livePrice = benchmarkPrice
	}

	klines5m, _ := fetchKlines(symbol, "5m", 35)
	klines1m, _ := fetchKlines(symbol, "1m", 15)
	ticker, _ := fetchTicker24hr(symbol)
	depth, _ := fetchOrderBookDepth(symbol)

	rsi5m := 50.0
	ema9 := livePrice
	ema21 := livePrice
	stochK := 50.0
	stochD := 50.0

	if len(klines5m) >= 21 {
		closes := make([]float64, len(klines5m))
		for i, k := range klines5m {
			closes[i] = k.Close
		}
		rsi5m = calculateRSI(closes, 14)
		ema9 = calculateEMA(closes, 9)
		ema21 = calculateEMA(closes, 21)
		stochK, stochD = calculateStochRSI(closes, 14, 14, 3, 3)
	}

	takerRatio5m := 0.50
	if len(klines5m) > 0 && klines5m[len(klines5m)-1].Volume > 0 {
		takerRatio5m = klines5m[len(klines5m)-1].TakerBuyVolume / klines5m[len(klines5m)-1].Volume
	}

	takerRatio1m := 0.50
	if len(klines1m) > 0 && klines1m[len(klines1m)-1].Volume > 0 {
		takerRatio1m = klines1m[len(klines1m)-1].TakerBuyVolume / klines1m[len(klines1m)-1].Volume
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

	// Whale Detection Logic
	whaleAlert := ""
	if stageNum >= 2 {
		if prevDepthRatio > 0 && depth.Ratio >= 2.0 && (depth.Ratio/prevDepthRatio >= 2.0 || depth.Ratio >= 10.0) {
			whaleAlert = fmt.Sprintf("WHALE_BUY: Tembok Beli Raksasa Masuk (%.2fx)!", depth.Ratio)
		} else if prevDepthRatio > 0 && depth.Ratio <= 0.5 && (prevDepthRatio/depth.Ratio >= 2.0 || depth.Ratio <= 0.1) {
			whaleAlert = fmt.Sprintf("WHALE_SELL: Dinding Jual Menghantam Pasar (%.2fx)!", depth.Ratio)
		} else if depth.Ratio >= 15.0 {
			whaleAlert = fmt.Sprintf("WHALE_BUY: Dinding Beli Dominan Ekstrem (%.2fx)!", depth.Ratio)
		} else if depth.Ratio <= 0.15 {
			whaleAlert = fmt.Sprintf("WHALE_SELL: Dinding Jual Dominan Ekstrem (%.2fx)!", depth.Ratio)
		}
	}

	quantScore := calculateBinaryQuantScore(livePrice, benchmarkPrice, rsi5m, ema9, ema21, stochK, stochD, depth, takerRatio1m, takerRatio5m, microTrend1m, ticker)

	// Incorporate whale alert into quant score
	if strings.Contains(whaleAlert, "WHALE_BUY") {
		quantScore.Score += 4
		if quantScore.Score > 0 {
			quantScore.Choice = "UP"
			quantScore.Confidence = int(math.Min(98, float64(quantScore.Confidence+4)))
		}
	} else if strings.Contains(whaleAlert, "WHALE_SELL") {
		quantScore.Score -= 4
		if quantScore.Score < 0 {
			quantScore.Choice = "DOWN"
			quantScore.Confidence = int(math.Min(98, float64(quantScore.Confidence+4)))
		}
	}

	stageTitle := "TAHAP 1: Observasi Awal Detik 0 (Garis Start)"
	if stageNum == 2 {
		stageTitle = "TAHAP 2: Re-evaluasi Aliran Detik 30 (+30 Detik)"
	} else if stageNum == 3 {
		stageTitle = "TAHAP 3: Validasi Candle 1-Menit (+60 Detik)"
	}

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

	deltaUSD := livePrice - benchmarkPrice
	deltaPct := (deltaUSD / benchmarkPrice) * 100.0

	prompt := fmt.Sprintf(`Anda adalah Master Quant Engine Prediksi Penutupan Biner 5-Menit (%s).
TUJUAN: Tentukan prediksi arah penutupan cutoff 5m (UP jika closing spot > $%.2f, DOWN jika < $%.2f).

KONDISI PASAR:
- Aset: %s | Patokan: $%.2f | Spot Terkini: $%.2f (Delta: %+.2f USD | %+.3f%%)
- Order Book Ratio (Top 20): %.2fx (%s) | Rasio Sebelumnya: %.2fx
- Taker Buy Ratio 1m: %.1f%% | Taker 5m: %.1f%%
- Stoch RSI 5m: %%K=%.1f, %%D=%.1f | RSI: %.1f | EMA9 vs EMA21: %s
- Micro-Momentum 1m: %s | 24h Trend: %+.2f%%
- Deteksi Paus / Whale: %s
- Skor Matematis Algoritmik: %s (Skor: %+d, Keyakinan: %d%%)

Berikan keputusan tegas 'UP' atau 'DOWN' dengan confidence 85-99.
Output HANYA JSON VALID:
{"choice": "UP" atau "DOWN", "confidence": angka 85-99, "short_term": "analisis singkat order flow", "long_term": "analisis momentum", "reasoning": "kesimpulan tahap ini", "model": "%s"}`,
		stageTitle, benchmarkPrice, benchmarkPrice,
		symbol, benchmarkPrice, livePrice, deltaUSD, deltaPct,
		depth.Ratio, depth.Status, prevDepthRatio,
		takerRatio1m*100, takerRatio5m*100,
		stochK, stochD, rsi5m, getEMAStatus(ema9, ema21),
		microTrend1m, ticker.Change24h,
		whaleAlert,
		quantScore.Choice, quantScore.Score, quantScore.Confidence,
		aiModel)

	type AIOutput struct {
		Choice     string `json:"choice"`
		Confidence int    `json:"confidence"`
		ShortTerm  string `json:"short_term"`
		LongTerm   string `json:"long_term"`
		Reasoning  string `json:"reasoning"`
		Model      string `json:"model"`
	}

	var output AIOutput

	reqBody, _ := json.Marshal(map[string]interface{}{
		"model": aiModel,
		"messages": []map[string]string{
			{"role": "system", "content": "You are a senior binary settlement quant engine. You output only valid JSON."},
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
		client := &http.Client{Timeout: 4 * time.Second}
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
					if output.Model == "" {
						output.Model = aiModel
					}
				}
			}
		}
	}

	if output.Choice == "" {
		output = AIOutput{
			Choice:     quantScore.Choice,
			Confidence: quantScore.Confidence,
			ShortTerm:  quantScore.ShortTerm,
			LongTerm:   quantScore.LongTerm,
			Reasoning:  quantScore.Reasoning,
			Model:      "quant-settlement-v3",
		}
	}

	return StageEvaluation{
		Choice:     output.Choice,
		Confidence: output.Confidence,
		ShortTerm:  output.ShortTerm,
		LongTerm:   output.LongTerm,
		Reasoning:  output.Reasoning,
		DepthRatio: depth.Ratio,
		TakerRatio: takerRatio1m,
		LivePrice:  livePrice,
		StochK:     stochK,
		MicroTrend: microTrend1m,
		WhaleAlert: whaleAlert,
		Model:      output.Model,
	}
}

type MidRoundRadar struct {
	Minute       int     `json:"minute"`        // 2, 3, atau 4
	Status       string  `json:"status"`        // "ON_TRACK", "UNDER_PRESSURE", "TIGHT_BATTLE"
	Signal       string  `json:"signal"`        // "UP" atau "DOWN" (Sinyal Arah Objektif Terkini)
	SignalType   string  `json:"signal_type"`   // "CONFIRMATION" atau "REVERSAL_ALERT"
	Confidence   int     `json:"confidence"`    // 85 - 98%
	Spot         float64 `json:"spot"`
	DeltaUSD     float64 `json:"delta_usd"`
	DeltaPct     float64 `json:"delta_pct"`
	DepthRatio   float64 `json:"depth_ratio"`
	TakerPct     float64 `json:"taker_pct"`
	ActionAdvice string  `json:"action_advice"` // Saran aksi konkret untuk trader/user
	Message      string  `json:"message"`
}

func evaluateMidRoundRadar(symbol string, benchmarkPrice float64, minute int, lockedChoice string) (string, MidRoundRadar) {
	livePrice, _ := fetchCurrentPrice(symbol)
	if livePrice <= 0 {
		livePrice = benchmarkPrice
	}
	depth, _ := fetchOrderBookDepth(symbol)
	klines1m, _ := fetchKlines(symbol, "1m", 5)
	taker1m := 0.50
	if len(klines1m) > 0 && klines1m[len(klines1m)-1].Volume > 0 {
		taker1m = klines1m[len(klines1m)-1].TakerBuyVolume / klines1m[len(klines1m)-1].Volume
	}
	deltaUSD := livePrice - benchmarkPrice
	deltaPct := (deltaUSD / benchmarkPrice) * 100.0

	// 1. Kalkulasi Sinyal Arah Objektif Pasar (UP vs DOWN)
	signalScore := 0.0

	// Bobot Delta Harga vs Acuan (Faktor Primer Settlement)
	if deltaUSD >= 5.0 {
		signalScore += 3.5
	} else if deltaUSD >= 1.5 {
		signalScore += 2.0
	} else if deltaUSD > 0 {
		signalScore += 0.8
	} else if deltaUSD <= -5.0 {
		signalScore -= 3.5
	} else if deltaUSD <= -1.5 {
		signalScore -= 2.0
	} else if deltaUSD < 0 {
		signalScore -= 0.8
	}

	// Bobot Imbalance Orderbook Top 20
	if depth.Ratio >= 1.30 {
		signalScore += 1.5
	} else if depth.Ratio <= 0.70 {
		signalScore -= 1.5
	}

	// Bobot Taker Flow Pasar (Agresi Pembeli vs Penjual 1m)
	if taker1m >= 0.55 {
		signalScore += 1.5
	} else if taker1m <= 0.45 {
		signalScore -= 1.5
	}

	signal := "UP"
	if signalScore < 0 {
		signal = "DOWN"
	} else if signalScore == 0 {
		if livePrice >= benchmarkPrice {
			signal = "UP"
		} else {
			signal = "DOWN"
		}
	}

	// 2. Tingkat Keyakinan Sinyal (Confidence)
	confidence := 88
	if minute == 3 {
		confidence = 90
		if math.Abs(deltaUSD) >= 10.0 {
			confidence = 96
		} else if math.Abs(deltaUSD) >= 4.0 {
			confidence = 93
		}
	} else if minute == 4 {
		confidence = 94
		if math.Abs(deltaUSD) >= 10.0 {
			confidence = 98
		} else if math.Abs(deltaUSD) >= 4.0 {
			confidence = 96
		}
	} else {
		if math.Abs(deltaUSD) >= 10.0 {
			confidence = 92
		}
	}

	// 3. Tipe Sinyal (Konfirmasi atau Peringatan Balik Arah)
	signalType := "CONFIRMATION"
	if lockedChoice != "" && signal != lockedChoice {
		signalType = "REVERSAL_ALERT"
	}

	// Status Radar Telemetri
	status := "ON_TRACK"
	if math.Abs(deltaUSD) < 2.0 {
		status = "TIGHT_BATTLE"
	} else if lockedChoice == "UP" {
		if livePrice < benchmarkPrice {
			status = "UNDER_PRESSURE"
		}
	} else if lockedChoice == "DOWN" {
		if livePrice > benchmarkPrice {
			status = "UNDER_PRESSURE"
		}
	}

	var actionAdvice string
	var message string

	if signalType == "REVERSAL_ALERT" {
		actionAdvice = fmt.Sprintf("🚨 SINYAL BALIK ARAH: Pasar beralih ke %s (%+.2f USD). Disarankan SWITCH ke %s!", signal, deltaUSD, signal)
	} else {
		actionAdvice = fmt.Sprintf("✅ SINYAL %s KOKOH: Pertahankan pilihan %s menuju cutoff (%+.2f USD).", signal, signal, deltaUSD)
	}

	switch minute {
	case 2:
		if signalType == "REVERSAL_ALERT" {
			message = fmt.Sprintf("Menit 2 (Sisa 3m): Warning Sinyal %s! Spot %+.2f USD kontra %s. Taker flow %.1f%%.", signal, deltaUSD, lockedChoice, (1.0-taker1m)*100)
		} else {
			message = fmt.Sprintf("Menit 2 (Sisa 3m): Sinyal %s aman memimpin (%+.2f USD). Orderbook %.2fx mendukung.", signal, deltaUSD, depth.Ratio)
		}
	case 3:
		if signalType == "REVERSAL_ALERT" {
			message = fmt.Sprintf("Menit 3 (Sisa 2m): 🚨 SINYAL BALIK ARAH -> %s (%d%%)! Spot kontra %+.2f USD. Tekanan pasar membalik arah pemenang.", signal, confidence, deltaUSD)
		} else {
			message = fmt.Sprintf("Menit 3 (Sisa 2m): Sinyal %s Terkonfirmasi (%d%%)! Spot unggul %+.2f USD, modal agresif menjaga zona.", signal, confidence, deltaUSD)
		}
	case 4:
		if signalType == "REVERSAL_ALERT" {
			message = fmt.Sprintf("Menit 4 (Final 60s): 🚨 SINYAL FINAL SPRINT -> %s (%d%%)! Posisi kontra %+.2f USD. Menit krusial settlement!", signal, confidence, deltaUSD)
		} else {
			message = fmt.Sprintf("Menit 4 (Final 60s): Sinyal %s Sangat Kuat (%d%%)! Keunggulan %+.2f USD siap terkunci pada cutoff.", signal, confidence, deltaUSD)
		}
	}

	radar := MidRoundRadar{
		Minute:       minute,
		Status:       status,
		Signal:       signal,
		SignalType:   signalType,
		Confidence:   confidence,
		Spot:         livePrice,
		DeltaUSD:     math.Round(deltaUSD*100) / 100,
		DeltaPct:     math.Round(deltaPct*1000) / 1000,
		DepthRatio:   math.Round(depth.Ratio*100) / 100,
		TakerPct:     math.Round(taker1m*1000) / 10,
		ActionAdvice: actionAdvice,
		Message:      message,
	}

	b, _ := json.Marshal(radar)
	return string(b), radar
}

// runThreeStageAIWorkflow executes the 3 observation checkpoints (Detik 0, +30s, +60s) and produces the Final Verdict, then continues radar monitoring for Minutes 2, 3, 4
func runThreeStageAIWorkflow(symbol string, benchmarkPrice float64, startWIB, cutoffWIB, roundID string, createdAt int64) {
	activeWorkflowsLock.Lock()
	if activeWorkflows[roundID] {
		activeWorkflowsLock.Unlock()
		return
	}
	activeWorkflows[roundID] = true
	activeWorkflowsLock.Unlock()

	defer func() {
		activeWorkflowsLock.Lock()
		delete(activeWorkflows, roundID)
		activeWorkflowsLock.Unlock()
	}()

	var finalChoice string
	var finalConf int
	var finalReason string

	curActive, _ := GetActiveRoundBySymbol(symbol)
	if curActive != nil && curActive.ID == roundID && curActive.AIStage3Choice != "" && curActive.AIChoice != "" {
		// Stages 1-3 already completed in previous run, resume final verdict
		finalChoice = curActive.AIChoice
		finalConf = curActive.AIConfidence
		finalReason = curActive.AIReasoning
		log.Printf("[AI WORKFLOW RESUME] %s sudah memiliki kesimpulan final: %s (%d%%). Melanjutkan ke radar monitoring...", roundID, finalChoice, finalConf)
	} else {
		log.Printf("[AI 3-STAGE WORKFLOW] Memulai pipeline 3-tahap untuk ronde %s...", roundID)

		// ==================== TAHAP 1 (Detik 0) ====================
		stage1 := evaluateMarketStage(symbol, benchmarkPrice, 1, 1.0)
		reason1 := fmt.Sprintf("Detik 0: Orderbook %.2fx, Taker %.1f%%, StochK %.1f | %s",
			stage1.DepthRatio, stage1.TakerRatio*100, stage1.StochK, stage1.Reasoning)
		_ = UpdateAIStage1(roundID, stage1.Choice, stage1.Confidence, reason1)
		log.Printf("[AI TAHAP 1 / DETIK 0] %s -> %s (%d%%) : %s", roundID, stage1.Choice, stage1.Confidence, stage1.Reasoning)

		// ==================== TAHAP 2 (Detik 30) ====================
		sleepSec1 := (createdAt + 30) - time.Now().Unix()
		if sleepSec1 > 0 {
			time.Sleep(time.Duration(sleepSec1) * time.Second)
		}

		active, err := GetActiveRoundBySymbol(symbol)
		if err != nil || active == nil || active.ID != roundID {
			log.Printf("[AI WORKFLOW] Ronde %s sudah tidak aktif, batal lanjut tahap 2.", roundID)
			return
		}

		stage2 := evaluateMarketStage(symbol, benchmarkPrice, 2, stage1.DepthRatio)
		whaleAlert := stage2.WhaleAlert
		reason2 := fmt.Sprintf("Detik 30: Orderbook %.2fx, Taker %.1f%% | %s",
			stage2.DepthRatio, stage2.TakerRatio*100, stage2.Reasoning)
		_ = UpdateAIStage2(roundID, stage2.Choice, stage2.Confidence, reason2, whaleAlert)
		log.Printf("[AI TAHAP 2 / DETIK 30] %s -> %s (%d%%) | Whale: %s", roundID, stage2.Choice, stage2.Confidence, whaleAlert)

		// ==================== TAHAP 3 (Detik 60) ====================
		sleepSec2 := (createdAt + 60) - time.Now().Unix()
		if sleepSec2 > 0 {
			time.Sleep(time.Duration(sleepSec2) * time.Second)
		}

		active, err = GetActiveRoundBySymbol(symbol)
		if err != nil || active == nil || active.ID != roundID {
			log.Printf("[AI WORKFLOW] Ronde %s sudah tidak aktif, batal lanjut tahap 3.", roundID)
			return
		}

		stage3 := evaluateMarketStage(symbol, benchmarkPrice, 3, stage2.DepthRatio)
		if stage3.WhaleAlert != "" {
			whaleAlert = stage3.WhaleAlert
		}
		reason3 := fmt.Sprintf("Detik 60: Candle 1m Validasi, Orderbook %.2fx, Taker %.1f%% | %s",
			stage3.DepthRatio, stage3.TakerRatio*100, stage3.Reasoning)

		// ==================== SINTESIS KESIMPULAN AKHIR AI ====================
		if stage2.Choice == stage3.Choice && stage2.Choice != stage1.Choice {
			finalChoice = stage2.Choice
			finalConf = int(math.Max(float64(stage2.Confidence), float64(stage3.Confidence)))
			if finalConf < 92 {
				finalConf = 92
			}
			finalReason = fmt.Sprintf("⚡ [REVERSAL TERKONFIRMASI]: Observasi awal (T+0s: %s) dibatalkan karena dorongan volume masif & order book pada T+30s (%s) dan T+60s (%s). AI secara adaptif menetapkan pilihan final %s.",
				stage1.Choice, stage2.Choice, stage3.Choice, finalChoice)
		} else if stage1.Choice == stage2.Choice && stage2.Choice == stage3.Choice {
			finalChoice = stage1.Choice
			finalConf = int(math.Min(99, math.Max(float64(stage1.Confidence), math.Max(float64(stage2.Confidence), float64(stage3.Confidence)))+2))
			finalReason = fmt.Sprintf("✅ [KONFLUENSI 3 TAHAP KOMPAK]: T+0s (%s), T+30s (%s), dan T+60s (%s) kompak mengonfirmasi %s. Likuiditas order book dan taker buy flow konsisten menjaga harga menuju penutupan cutoff.",
				stage1.Choice, stage2.Choice, stage3.Choice, finalChoice)
		} else if stage1.Choice == stage3.Choice && stage2.Choice != stage1.Choice {
			finalChoice = stage1.Choice
			finalConf = stage3.Confidence
			finalReason = fmt.Sprintf("🛡️ [FAKEOUT TERLEWATI]: Fluktuasi sesaat di T+30s (%s) berhasil ditepis oleh validasi candle 1m di T+60s (%s). AI mempertahankan arah dominan %s.",
				stage2.Choice, stage3.Choice, finalChoice)
		} else {
			finalChoice = stage3.Choice
			finalConf = stage3.Confidence
			finalReason = fmt.Sprintf("🎯 [VALIDASI CANDLE 1M]: Berdasarkan dinamika 3 observasi (T0: %s, T30: %s, T60: %s), struktur candle 1m memvalidasi arah %s menuju cutoff.",
				stage1.Choice, stage2.Choice, stage3.Choice, finalChoice)
		}

		modelUsed := stage3.Model
		if modelUsed == "" {
			modelUsed = "cbai/glm-5.2"
		}

		_ = UpdateAIStage3AndVerdict(roundID, stage3.Choice, stage3.Confidence, reason3, finalChoice, finalConf, finalReason, modelUsed, whaleAlert)

		log.Printf("==================================================================")
		log.Printf("[AI KESIMPULAN AKHIR RESMI] %s -> FINAL VERDICT: %s (%d%%) | Model: %s",
			roundID, finalChoice, finalConf, modelUsed)
		log.Printf("Detail Sintesis: %s", finalReason)
		log.Printf("==================================================================")
	}

	// ==================== RADAR & SINYAL AKTIF MENIT KE-2, 3 & 4 ====================
	// Menit 2 (T = 120s / Sisa 3 Menit)
	sleepSecM2 := (createdAt + 120) - time.Now().Unix()
	if sleepSecM2 > 0 {
		time.Sleep(time.Duration(sleepSecM2) * time.Second)
	}
	checkActive, err := GetActiveRoundBySymbol(symbol)
	if err == nil && checkActive != nil && checkActive.ID == roundID && checkActive.AIM2Info == "" {
		m2JSON, radarM2 := evaluateMidRoundRadar(symbol, benchmarkPrice, 2, finalChoice)
		_ = UpdateAIMidRoundInfo(roundID, 2, m2JSON)
		log.Printf("[AI SINYAL MENIT 2] %s -> Sinyal: %s (%s, %d%%) | Spot: $%.2f | Delta: %+.2f USD",
			roundID, radarM2.Signal, radarM2.SignalType, radarM2.Confidence, radarM2.Spot, radarM2.DeltaUSD)
	}

	// Menit 3 (T = 180s / Sisa 2 Menit - Sinyal Prediksi Kritis untuk User)
	sleepSecM3 := (createdAt + 180) - time.Now().Unix()
	if sleepSecM3 > 0 {
		time.Sleep(time.Duration(sleepSecM3) * time.Second)
	}
	checkActive, err = GetActiveRoundBySymbol(symbol)
	if err == nil && checkActive != nil && checkActive.ID == roundID && checkActive.AIM3Info == "" {
		m3JSON, radarM3 := evaluateMidRoundRadar(symbol, benchmarkPrice, 3, finalChoice)
		_ = UpdateAIMidRoundInfo(roundID, 3, m3JSON)
		log.Printf("[AI SINYAL MENIT 3] %s -> Sinyal: %s (%s, %d%%) | Spot: $%.2f | Delta: %+.2f USD",
			roundID, radarM3.Signal, radarM3.SignalType, radarM3.Confidence, radarM3.Spot, radarM3.DeltaUSD)
	}

	// Menit 4 (T = 240s / Sisa 1 Menit - Final Sprint Signal Menuju Cutoff)
	sleepSecM4 := (createdAt + 240) - time.Now().Unix()
	if sleepSecM4 > 0 {
		time.Sleep(time.Duration(sleepSecM4) * time.Second)
	}
	checkActive, err = GetActiveRoundBySymbol(symbol)
	if err == nil && checkActive != nil && checkActive.ID == roundID && checkActive.AIM4Info == "" {
		m4JSON, radarM4 := evaluateMidRoundRadar(symbol, benchmarkPrice, 4, finalChoice)
		_ = UpdateAIMidRoundInfo(roundID, 4, m4JSON)
		log.Printf("[AI SINYAL MENIT 4] %s -> Sinyal Final: %s (%s, %d%%) | Spot: $%.2f | Delta: %+.2f USD",
			roundID, radarM4.Signal, radarM4.SignalType, radarM4.Confidence, radarM4.Spot, radarM4.DeltaUSD)
	}
}

func getEMAStatus(ema9, ema21 float64) string {
	if ema9 > ema21 {
		return "Golden Cross / Bullish Momentum"
	}
	return "Death Cross / Bearish Pressure"
}

// calculateBinaryQuantScore computes mathematical binary cutoff settlement probabilities
func calculateBinaryQuantScore(livePrice, benchmark, rsi5m, ema9, ema21, stochK, stochD float64, depth OrderBookDepth, taker1m, taker5m float64, microTrend string, ticker Ticker24h) BinaryQuantScore {
	score := 0

	// 1. Delta posisi awal vs benchmark (+2 / -2)
	if livePrice > benchmark {
		score += 2
	} else if livePrice < benchmark {
		score -= 2
	}

	// 2. Order Book Depth Imbalance (Likuiditas Limit Order)
	if depth.Ratio >= 1.35 {
		score += 3 // Bid wall sangat tebal (dorongan beli)
	} else if depth.Ratio <= 0.70 {
		score -= 3 // Ask wall sangat tebal (tahanan jual)
	} else if depth.Ratio >= 1.15 {
		score += 1
	} else if depth.Ratio <= 0.85 {
		score -= 1
	}

	// 3. Taker Buy Volume Aggression (Aliran Pesanan Pasar Nyata)
	if taker1m >= 0.55 {
		score += 3 // Buyer agresif melahap antrean
	} else if taker1m <= 0.45 {
		score -= 3 // Seller agresif menghantam bid
	}

	if taker5m >= 0.54 {
		score += 1
	} else if taker5m <= 0.46 {
		score -= 1
	}

	// 4. Stochastic RSI (14, 14, 3, 3) 5m Dynamics
	if stochK < 20 {
		score += 3 // Oversold ekstrem, peluang rebound kuat
	} else if stochK > 80 {
		score -= 3 // Overbought ekstrem, peluang koreksi kuat
	} else if stochK > stochD && stochK < 65 {
		score += 2 // Bullish crossover momentum
	} else if stochK < stochD && stochK > 35 {
		score -= 2 // Bearish crossover momentum
	}

	// 5. RSI 5m Standar Extreme Bounds
	if rsi5m < 32 {
		score += 2
	} else if rsi5m > 68 {
		score -= 2
	}

	// 6. Micro-Momentum 1m Structure
	if strings.Contains(microTrend, "Bullish") {
		score += 2
	} else if strings.Contains(microTrend, "Bearish") {
		score -= 2
	}

	// 7. EMA 9 vs EMA 21 (hanya jika StochK berada di zona tren 30-70)
	if stochK >= 30 && stochK <= 70 {
		if ema9 > ema21 {
			score += 2
		} else {
			score -= 2
		}
	}

	// 8. Macro 24h trend
	if ticker.Change24h > 0.5 {
		score += 1
	} else if ticker.Change24h < -0.5 {
		score -= 1
	}

	conf := 88
	var choice, shortTerm, longTerm, reasoning string

	if score >= 1 {
		choice = "UP"
		conf = 88 + int(math.Min(10, float64(score)*1.2))
		if conf > 98 {
			conf = 98
		}
		shortTerm = fmt.Sprintf("Order book ratio (%.2fx) dan Taker Buy Flow (%.1f%%) menunjukkan akumulasi buyer dengan Stoch RSI (%%K=%.1f).", depth.Ratio, taker1m*100, stochK)
		longTerm = fmt.Sprintf("Struktur tren 24h (%+.2f%%) dan batas likuiditas bawah menjaga harga penutupan di atas patokan $%.2f.", ticker.Change24h, benchmark)
		reasoning = fmt.Sprintf("Agresivitas aliran beli pasar (Taker 1m: %.1f%%) didukung oleh dinding bid depth (%.2fx) memperbesar probabilitas harga spot pada detik ke-300 ditutup di zona UP di atas acuan $%.2f.", taker1m*100, depth.Ratio, benchmark)
	} else {
		choice = "DOWN"
		absScore := int(math.Abs(float64(score)))
		conf = 88 + int(math.Min(10, float64(absScore)*1.2))
		if conf > 98 {
			conf = 98
		}
		shortTerm = fmt.Sprintf("Tekanan sell order book (ratio: %.2fx) dan Taker Sell Flow dominan (%.1f%%) dengan Stoch RSI (%%K=%.1f).", depth.Ratio, (1.0-taker1m)*100, stochK)
		longTerm = fmt.Sprintf("Resistensi lokal dan tren 24h (%+.2f%%) membatasi potensi kenaikan menuju detik cutoff.", ticker.Change24h)
		reasoning = fmt.Sprintf("Ketidakseimbangan likuiditas jual (Ask dominan) dan defisit volume beli mengindikasikan harga condong tertahan di zona DOWN di bawah acuan $%.2f saat konsolidasi 5m.", benchmark)
	}

	return BinaryQuantScore{
		Score:          score,
		Choice:         choice,
		Confidence:     conf,
		OrderBookRatio: depth.Ratio,
		TakerRatio1m:   taker1m,
		TakerRatio5m:   taker5m,
		StochK:         stochK,
		StochD:         stochD,
		RSI5m:          rsi5m,
		ShortTerm:      shortTerm,
		LongTerm:       longTerm,
		Reasoning:      reasoning,
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

		tbv := 0.0
		if len(item) > 9 {
			tbv, _ = strconv.ParseFloat(fmt.Sprintf("%v", item[9]), 64)
		}

		results = append(results, Kline{
			OpenTime:       int64(ot),
			Open:           op,
			High:           hi,
			Low:            lo,
			Close:          cl,
			Volume:         vol,
			CloseTime:      int64(ct),
			TakerBuyVolume: tbv,
		})
	}
	return results, nil
}

// fetchOrderBookDepth analyzes top 20 orderbook depth for liquidity imbalance
func fetchOrderBookDepth(symbol string) (OrderBookDepth, error) {
	url := fmt.Sprintf("https://api.binance.com/api/v3/depth?symbol=%s&limit=20", strings.ToUpper(symbol))
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		return OrderBookDepth{Ratio: 1.0, Status: "Netral"}, err
	}
	defer resp.Body.Close()

	var data struct {
		Bids [][]string `json:"bids"`
		Asks [][]string `json:"asks"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return OrderBookDepth{Ratio: 1.0, Status: "Netral"}, err
	}

	var bidVol, askVol float64
	for _, b := range data.Bids {
		if len(b) >= 2 {
			qty, _ := strconv.ParseFloat(b[1], 64)
			bidVol += qty
		}
	}
	for _, a := range data.Asks {
		if len(a) >= 2 {
			qty, _ := strconv.ParseFloat(a[1], 64)
			askVol += qty
		}
	}

	ratio := 1.0
	if askVol > 0 {
		ratio = bidVol / askVol
	}

	status := "Seimbang (Rasio Netral)"
	if ratio >= 1.30 {
		status = "Bid Dominan (Dinding Beli Tebal / Dorongan UP)"
	} else if ratio <= 0.70 {
		status = "Ask Dominan (Dinding Jual Tebal / Tekanan Tahan DOWN)"
	}

	return OrderBookDepth{
		BidVolume: bidVol,
		AskVolume: askVol,
		Ratio:     ratio,
		Status:    status,
	}, nil
}

// calculateStochRSI computes Stochastic RSI %K and %D (14, 14, 3, 3)
func calculateStochRSI(closes []float64, rsiPeriod, stochPeriod, kSmooth, dSmooth int) (float64, float64) {
	minRequired := rsiPeriod + stochPeriod + kSmooth
	if len(closes) < minRequired {
		return 50.0, 50.0
	}

	// 1. Calculate RSI series
	rsiSeries := make([]float64, 0, len(closes)-rsiPeriod+1)
	for i := rsiPeriod; i <= len(closes); i++ {
		rsiVal := calculateRSI(closes[:i], rsiPeriod)
		rsiSeries = append(rsiSeries, rsiVal)
	}

	if len(rsiSeries) < stochPeriod {
		return 50.0, 50.0
	}

	// 2. Compute Raw Stoch RSI values
	rawStoch := make([]float64, 0, len(rsiSeries)-stochPeriod+1)
	for i := stochPeriod - 1; i < len(rsiSeries); i++ {
		window := rsiSeries[i-stochPeriod+1 : i+1]
		minRSI := window[0]
		maxRSI := window[0]
		for _, v := range window {
			if v < minRSI {
				minRSI = v
			}
			if v > maxRSI {
				maxRSI = v
			}
		}
		if maxRSI-minRSI == 0 {
			rawStoch = append(rawStoch, 50.0)
		} else {
			stochVal := ((window[len(window)-1] - minRSI) / (maxRSI - minRSI)) * 100.0
			rawStoch = append(rawStoch, stochVal)
		}
	}

	// 3. Smooth to get %K
	kSeries := make([]float64, 0)
	for i := kSmooth - 1; i < len(rawStoch); i++ {
		sum := 0.0
		for j := i - kSmooth + 1; j <= i; j++ {
			sum += rawStoch[j]
		}
		kSeries = append(kSeries, sum/float64(kSmooth))
	}

	if len(kSeries) == 0 {
		return 50.0, 50.0
	}

	kVal := kSeries[len(kSeries)-1]

	// 4. Smooth %K to get %D
	dVal := kVal
	if len(kSeries) >= dSmooth {
		sumD := 0.0
		for i := len(kSeries) - dSmooth; i < len(kSeries); i++ {
			sumD += kSeries[i]
		}
		dVal = sumD / float64(dSmooth)
	}

	return kVal, dVal
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

	depth, _ := fetchOrderBookDepth(symbol)
	klines1m, _ := fetchKlines(symbol, "1m", 2)
	taker1m := 0.50
	if len(klines1m) > 0 && klines1m[len(klines1m)-1].Volume > 0 {
		taker1m = klines1m[len(klines1m)-1].TakerBuyVolume / klines1m[len(klines1m)-1].Volume
	}

	res := map[string]interface{}{
		"active_round":      active,
		"live_price":        livePrice,
		"remaining_sec":     remainingSec,
		"is_above":          isAbove,
		"delta_usd":         deltaUSD,
		"delta_pct":         deltaPct,
		"order_book_ratio":  math.Round(depth.Ratio*100) / 100,
		"order_book_status": depth.Status,
		"taker_buy_pct_1m":  math.Round(taker1m*1000) / 10,
		"server_time_wib":   FormatWIB(time.Now()),
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
	creBin := "cre"
	if _, err := exec.LookPath(creBin); err != nil {
		userProfile := os.Getenv("USERPROFILE")
		candidate := filepath.Join(userProfile, "AppData", "Local", "Programs", "cre", "cre.exe")
		if _, errStat := os.Stat(candidate); errStat == nil {
			creBin = candidate
		}
	}

	cmd := exec.Command(creBin, "workflow", "simulate", "prediction-workflow", "--target", "staging-settings", "--non-interactive", "--trigger-index", "0")
	cmd.Dir = rootDir

	output, err := cmd.CombinedOutput()
	res := map[string]interface{}{
		"success": err == nil,
		"output":  string(output),
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(res)
}
