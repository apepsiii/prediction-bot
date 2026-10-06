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

// Global state and storage
type RoundRecord struct {
	ID              string  `json:"id"`
	Timestamp       string  `json:"timestamp"`
	Symbol          string  `json:"symbol"`
	StartPrice      float64 `json:"start_price"`
	ThresholdPrice  float64 `json:"threshold_price"`
	FinalPrice      float64 `json:"final_price"`
	DurationSec     int     `json:"duration_sec"`
	UserChoice      string  `json:"user_choice"`      // "UP" or "DOWN"
	AIChoice        string  `json:"ai_choice"`        // "UP" or "DOWN"
	AIConfidence    int     `json:"ai_confidence"`    // percentage
	AIReasoning     string  `json:"ai_reasoning"`     // explanation
	ActualOutcome   string  `json:"actual_outcome"`   // "UP" or "DOWN"
	UserWon         bool    `json:"user_won"`
	AIWon           bool    `json:"ai_won"`
	OutcomeSummary  string  `json:"outcome_summary"`  // "HUMAN_WON", "AI_WON", "DRAW_WIN", "DRAW_LOSS"
}

type Scoreboard struct {
	TotalRounds int `json:"total_rounds"`
	UserWins    int `json:"user_wins"`
	AIWins      int `json:"ai_wins"`
	Draws       int `json:"draws"`
}

type AppState struct {
	sync.RWMutex
	Rounds     []RoundRecord `json:"rounds"`
	Scoreboard Scoreboard    `json:"scoreboard"`
}

var state AppState
var dataFilePath string
var rootDir string

func init() {
	exeDir, err := os.Getwd()
	if err != nil {
		exeDir = "."
	}
	rootDir = exeDir
	// If running inside web/, root is parent
	if filepath.Base(rootDir) == "web" {
		rootDir = filepath.Dir(rootDir)
	}

	dataDir := filepath.Join(rootDir, "data")
	_ = os.MkdirAll(dataDir, 0755)
	dataFilePath = filepath.Join(dataDir, "rounds.json")

	loadState()
}

func loadState() {
	state.Lock()
	defer state.Unlock()

	data, err := os.ReadFile(dataFilePath)
	if err == nil {
		_ = json.Unmarshal(data, &state)
	}
	calculateScoreboard()
}

func saveState() {
	state.Lock()
	defer state.Unlock()
	calculateScoreboard()

	data, err := json.MarshalIndent(state, "", "  ")
	if err == nil {
		_ = os.WriteFile(dataFilePath, data, 0644)
	}
}

func calculateScoreboard() {
	sb := Scoreboard{TotalRounds: len(state.Rounds)}
	for _, r := range state.Rounds {
		if r.UserWon && !r.AIWon {
			sb.UserWins++
		} else if r.AIWon && !r.UserWon {
			sb.AIWins++
		} else {
			sb.Draws++
		}
	}
	state.Scoreboard = sb
}

// readEnvKey reads key from root .env file
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
	port := os.Getenv("PORT")
	if port == "" {
		port = "8090"
	}

	mux := http.NewServeMux()

	// API Routes
	mux.HandleFunc("/api/price", handlePrice)
	mux.HandleFunc("/api/ai-predict", handleAIPredict)
	mux.HandleFunc("/api/rounds", handleRounds)
	mux.HandleFunc("/api/simulate-cre", handleSimulateCRE)

	// Static UI Files
	staticDir := filepath.Join(rootDir, "web", "static")
	fileServer := http.FileServer(http.Dir(staticDir))
	mux.Handle("/", fileServer)

	serverAddr := ":" + port
	fmt.Printf("\n======================================================\n")
	fmt.Printf("  🏆 CHAINLINK CRE x AI PREDICTION ARENA\n")
	fmt.Printf("  Human vs AI Quant Duel (Binance 5m Engine)\n")
	fmt.Printf("======================================================\n")
	fmt.Printf("  URL Dashboard: http://localhost:%s\n", port)
	fmt.Printf("  Root Project : %s\n", rootDir)
	fmt.Printf("======================================================\n\n")

	log.Fatal(http.ListenAndServe(serverAddr, mux))
}

// handlePrice fetches real-time 24hr ticker from Binance
func handlePrice(w http.ResponseWriter, r *http.Request) {
	symbol := r.URL.Query().Get("symbol")
	if symbol == "" {
		symbol = "BTCUSDT"
	}
	symbol = strings.ToUpper(strings.TrimSpace(symbol))

	url := fmt.Sprintf("https://api.binance.com/api/v3/ticker/24hr?symbol=%s", symbol)
	resp, err := http.Get(url)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"failed to connect to Binance API: %v"}`, err), http.StatusInternalServerError)
		return
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		http.Error(w, `{"error":"failed to read Binance response"}`, http.StatusInternalServerError)
		return
	}

	var raw map[string]interface{}
	if err := json.Unmarshal(body, &raw); err != nil {
		http.Error(w, `{"error":"invalid json from Binance"}`, http.StatusInternalServerError)
		return
	}

	lastPrice, _ := strconv.ParseFloat(fmt.Sprintf("%v", raw["lastPrice"]), 64)
	change24h, _ := strconv.ParseFloat(fmt.Sprintf("%v", raw["priceChangePercent"]), 64)
	highPrice, _ := strconv.ParseFloat(fmt.Sprintf("%v", raw["highPrice"]), 64)
	lowPrice, _ := strconv.ParseFloat(fmt.Sprintf("%v", raw["lowPrice"]), 64)
	volume, _ := strconv.ParseFloat(fmt.Sprintf("%v", raw["volume"]), 64)

	res := map[string]interface{}{
		"symbol":         symbol,
		"price":          lastPrice,
		"change_24h_pct": change24h,
		"high_24h":       highPrice,
		"low_24h":        lowPrice,
		"volume":         volume,
		"timestamp":      time.Now().UTC().Format(time.RFC3339),
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(res)
}

// handleAIPredict asks 9router AI (or fallback) whether price will be UP or DOWN relative to threshold
func handleAIPredict(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Symbol          string  `json:"symbol"`
		CurrentPrice    float64 `json:"current_price"`
		ThresholdPrice  float64 `json:"threshold_price"`
		DurationSec     int     `json:"duration_sec"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error":"invalid request json"}`, http.StatusBadRequest)
		return
	}

	if req.CurrentPrice <= 0 || req.ThresholdPrice <= 0 {
		http.Error(w, `{"error":"current_price and threshold_price must be > 0"}`, http.StatusBadRequest)
		return
	}

	apiKey := readEnvKey("AI_API_KEY_VAR")
	aiURL := "http://127.0.0.1:20128/v1/chat/completions"
	aiModel := "cbai/glm-5.2"

	// Difference between current and threshold
	diffPct := ((req.ThresholdPrice - req.CurrentPrice) / req.CurrentPrice) * 100.0

	prompt := fmt.Sprintf(`Anda adalah AI Quant Trader untuk Binance 5-Minute Prediction Market.
Kondisi Pasar Saat Ini:
- Aset: %s
- Harga Sekarang: $%.2f
- Batas Target (Threshold): $%.2f (Selisih: %+.2f%%)
- Durasi Ronde: %d detik

Pertanyaan: Apakah menurut Anda harga aset akan berakhir di ATAS batas target (UP) atau di BAWAH batas target (DOWN) saat waktu habis?

Jawab HANYA dalam format JSON berikut tanpa markdown:
{"choice": "UP" atau "DOWN", "confidence": angka integer 55-95, "reasoning": "alasan singkat analisis teknikal & momentum"}`,
		req.Symbol, req.CurrentPrice, req.ThresholdPrice, diffPct, req.DurationSec)

	type AIOutput struct {
		Choice     string `json:"choice"`
		Confidence int    `json:"confidence"`
		Reasoning  string `json:"reasoning"`
		Provider   string `json:"provider"`
	}

	var output AIOutput

	// Attempt calling 9router
	reqBody, _ := json.Marshal(map[string]interface{}{
		"model": aiModel,
		"messages": []map[string]string{
			{"role": "system", "content": "You are a professional quant prediction engine. Output strictly valid JSON."},
			{"role": "user", "content": prompt},
		},
		"temperature": 0.2,
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
					parsed.Provider = fmt.Sprintf("9router (%s)", aiModel)
					output = parsed
				}
			}
		}
	}

	// Deterministic Fallback if AI is offline or failed
	if output.Choice == "" {
		output = computeTechnicalChoice(req.CurrentPrice, req.ThresholdPrice, diffPct)
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(output)
}

func computeTechnicalChoice(currentPrice, threshold, diffPct float64) struct {
	Choice     string `json:"choice"`
	Confidence int    `json:"confidence"`
	Reasoning  string `json:"reasoning"`
	Provider   string `json:"provider"`
} {
	// If threshold is below current price, market has momentum to stay above or drift
	if currentPrice >= threshold {
		conf := 72
		if currentPrice > threshold*1.002 {
			conf = 84
		}
		return struct {
			Choice     string `json:"choice"`
			Confidence int    `json:"confidence"`
			Reasoning  string `json:"reasoning"`
			Provider   string `json:"provider"`
		}{
			Choice:     "UP",
			Confidence: conf,
			Reasoning:  fmt.Sprintf("Harga saat ini ($%.2f) telah berada di atas batas acuan ($%.2f); momentum mengindikasikan bertahan di zona UP.", currentPrice, threshold),
			Provider:   "Deterministic Momentum Engine (Fallback)",
		}
	} else {
		conf := 70
		if currentPrice < threshold*0.998 {
			conf = 82
		}
		return struct {
			Choice     string `json:"choice"`
			Confidence int    `json:"confidence"`
			Reasoning  string `json:"reasoning"`
			Provider   string `json:"provider"`
		}{
			Choice:     "DOWN",
			Confidence: conf,
			Reasoning:  fmt.Sprintf("Harga saat ini ($%.2f) berada di bawah batas acuan ($%.2f); resistensi jangka pendek menahan pergerakan tetap di zona DOWN.", currentPrice, threshold),
			Provider:   "Deterministic Momentum Engine (Fallback)",
		}
	}
}

// handleRounds handles fetching and recording resolved duel rounds
func handleRounds(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		state.RLock()
		defer state.RUnlock()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(state)
		return
	}

	if r.Method == http.MethodPost {
		var round RoundRecord
		if err := json.NewDecoder(r.Body).Decode(&round); err != nil {
			http.Error(w, `{"error":"invalid round data"}`, http.StatusBadRequest)
			return
		}

		if round.ID == "" {
			round.ID = fmt.Sprintf("RND-%d", time.Now().UnixNano()%1000000)
		}
		round.Timestamp = time.Now().Format("15:04:05")

		// Determine outcome
		if round.FinalPrice >= round.ThresholdPrice {
			round.ActualOutcome = "UP"
		} else {
			round.ActualOutcome = "DOWN"
		}

		round.UserWon = (round.UserChoice == round.ActualOutcome)
		round.AIWon = (round.AIChoice == round.ActualOutcome)

		if round.UserWon && !round.AIWon {
			round.OutcomeSummary = "HUMAN_WON"
		} else if round.AIWon && !round.UserWon {
			round.OutcomeSummary = "AI_WON"
		} else if round.UserWon && round.AIWon {
			round.OutcomeSummary = "DRAW_WIN"
		} else {
			round.OutcomeSummary = "DRAW_LOSS"
		}

		state.Lock()
		state.Rounds = append([]RoundRecord{round}, state.Rounds...) // prepend newest
		if len(state.Rounds) > 100 {
			state.Rounds = state.Rounds[:100]
		}
		state.Unlock()

		saveState()

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(round)
		return
	}

	http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
}

// handleSimulateCRE triggers native CRE CLI workflow simulation and returns the result
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
