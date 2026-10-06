package main

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"strconv"
	"strings"

	creHttp "github.com/smartcontractkit/cre-sdk-go/capabilities/networking/http"
	"github.com/smartcontractkit/cre-sdk-go/capabilities/scheduler/cron"
	"github.com/smartcontractkit/cre-sdk-go/cre"
)

// Config defines the workflow parameters loaded from target JSON config.
type Config struct {
	Schedule           string  `json:"schedule"`
	Symbol             string  `json:"symbol"`
	PriceURL           string  `json:"price_url"`
	AIBaseURL          string  `json:"ai_base_url"`
	AIModel            string  `json:"ai_model"`
	DefaultPositionUSD float64 `json:"default_position_usd"`
	MaxLeverage        int     `json:"max_leverage"`
}

// BinanceTicker represents the 24hr ticker response from Binance API.
type BinanceTicker struct {
	Symbol             string `json:"symbol"`
	LastPrice          string `json:"lastPrice"`
	PriceChangePercent string `json:"priceChangePercent"`
	HighPrice          string `json:"highPrice"`
	LowPrice           string `json:"lowPrice"`
	Volume             string `json:"volume"`
}

// AIPredictionResponse represents structured prediction output from AI or fallback.
type AIPredictionResponse struct {
	Direction       string  `json:"direction"`
	TargetPrice     float64 `json:"target_price"`
	StopLoss        float64 `json:"stop_loss"`
	Confidence      int     `json:"confidence"`
	PositionSizeUSD float64 `json:"position_size_usd"`
	Reasoning       string  `json:"reasoning"`
}

// PredictionResult represents the finalized prediction payload emitted by the workflow.
type PredictionResult struct {
	Symbol             string  `json:"symbol"`
	CurrentPrice       float64 `json:"current_price"`
	PriceChange24hPct  float64 `json:"price_change_24h_pct"`
	High24h            float64 `json:"high_24h"`
	Low24h             float64 `json:"low_24h"`
	Direction          string  `json:"direction"`
	TargetPrice        float64 `json:"target_price"`
	StopLossPrice      float64 `json:"stop_loss_price"`
	RiskRewardRatio    float64 `json:"risk_reward_ratio"`
	ConfidencePct      int     `json:"confidence_pct"`
	PositionSizeUSD    float64 `json:"position_size_usd"`
	RecommendedLeverage int     `json:"recommended_leverage"`
	HorizonMinutes     int     `json:"horizon_minutes"`
	Provider           string  `json:"provider"`
	Reasoning          string  `json:"reasoning"`
	Timestamp          string  `json:"timestamp"`
}

// ExecutionResult wraps the workflow output for CRE runner.
type ExecutionResult struct {
	Prediction *PredictionResult `json:"prediction"`
	Status     string            `json:"status"`
}

// InitWorkflow registers the cron trigger and handler.
func InitWorkflow(config *Config, logger *slog.Logger, secretsProvider cre.SecretsProvider) (cre.Workflow[*Config], error) {
	schedule := "0 */5 * * * *"
	if config.Schedule != "" {
		schedule = config.Schedule
	}
	cronTrigger := cron.Trigger(&cron.Config{Schedule: schedule})

	return cre.Workflow[*Config]{
		cre.Handler(cronTrigger, onCronTrigger),
	}, nil
}

// onCronTrigger executes the 5-minute prediction cycle.
func onCronTrigger(config *Config, runtime cre.Runtime, trigger *cron.Payload) (*ExecutionResult, error) {
	logger := runtime.Logger()
	now := runtime.Now()
	logger.Info("Starting 5-minute prediction cycle", "time", now.Format("2006-01-02 15:04:05 MST"), "symbol", config.Symbol)

	// 1. Fetch live market ticker data from Binance API via CRE HTTP capability
	httpClient := &creHttp.Client{}
	tickerJSON, err := creHttp.SendRequest(config, runtime, httpClient, func(cfg *Config, log *slog.Logger, requester *creHttp.SendRequester) (string, error) {
		priceURL := cfg.PriceURL
		if priceURL == "" {
			priceURL = fmt.Sprintf("https://api.binance.com/api/v3/ticker/24hr?symbol=%s", cfg.Symbol)
		}
		req := &creHttp.Request{
			Url:    priceURL,
			Method: "GET",
		}
		resp, err := requester.SendRequest(req).Await()
		if err != nil {
			return "", err
		}
		return string(resp.Body), nil
	}, cre.ConsensusIdenticalAggregation[string]()).Await()

	if err != nil {
		logger.Error("Failed to fetch market data", "err", err)
		return nil, fmt.Errorf("failed to fetch market data: %w", err)
	}

	var ticker BinanceTicker
	if err := json.Unmarshal([]byte(tickerJSON), &ticker); err != nil {
		logger.Error("Failed to parse ticker JSON", "err", err)
		return nil, fmt.Errorf("failed to parse ticker JSON: %w", err)
	}

	currentPrice, _ := strconv.ParseFloat(ticker.LastPrice, 64)
	change24h, _ := strconv.ParseFloat(ticker.PriceChangePercent, 64)
	highPrice, _ := strconv.ParseFloat(ticker.HighPrice, 64)
	lowPrice, _ := strconv.ParseFloat(ticker.LowPrice, 64)

	logger.Info("Market data acquired",
		"symbol", ticker.Symbol,
		"price", currentPrice,
		"24h_change_pct", change24h,
		"high", highPrice,
		"low", lowPrice,
	)

	// 2. Retrieve AI API Key from CRE Secrets
	var apiKey string
	secretPromise := runtime.GetSecret(&cre.SecretRequest{Id: "AI_API_KEY"})
	if secretPromise != nil {
		secret, secErr := secretPromise.Await()
		if secErr == nil && secret != nil {
			apiKey = secret.Value
		}
	}

	// 3. Attempt AI analysis via 9router
	prediction, provider := queryAIOrFallback(config, runtime, httpClient, currentPrice, change24h, highPrice, lowPrice, apiKey, logger)

	// 4. Calculate Risk/Reward ratio
	var rrRatio float64
	if prediction.Direction == "LONG" && prediction.TargetPrice > currentPrice && currentPrice > prediction.StopLoss {
		potentialGain := prediction.TargetPrice - currentPrice
		potentialLoss := currentPrice - prediction.StopLoss
		if potentialLoss > 0 {
			rrRatio = math.Round((potentialGain/potentialLoss)*100) / 100
		}
	} else if prediction.Direction == "SHORT" && currentPrice > prediction.TargetPrice && prediction.StopLoss > currentPrice {
		potentialGain := currentPrice - prediction.TargetPrice
		potentialLoss := prediction.StopLoss - currentPrice
		if potentialLoss > 0 {
			rrRatio = math.Round((potentialGain/potentialLoss)*100) / 100
		}
	} else {
		rrRatio = 1.5
	}

	leverage := config.MaxLeverage
	if leverage <= 0 {
		leverage = 3
	}

	finalResult := &PredictionResult{
		Symbol:              config.Symbol,
		CurrentPrice:        currentPrice,
		PriceChange24hPct:   change24h,
		High24h:             highPrice,
		Low24h:              lowPrice,
		Direction:           prediction.Direction,
		TargetPrice:         prediction.TargetPrice,
		StopLossPrice:       prediction.StopLoss,
		RiskRewardRatio:     rrRatio,
		ConfidencePct:       prediction.Confidence,
		PositionSizeUSD:     prediction.PositionSizeUSD,
		RecommendedLeverage: leverage,
		HorizonMinutes:      5,
		Provider:            provider,
		Reasoning:           prediction.Reasoning,
		Timestamp:           now.UTC().Format("2006-01-02T15:04:05Z"),
	}

	logger.Info("Prediction generated successfully",
		"symbol", finalResult.Symbol,
		"direction", finalResult.Direction,
		"target", finalResult.TargetPrice,
		"stop_loss", finalResult.StopLossPrice,
		"confidence", finalResult.ConfidencePct,
		"provider", finalResult.Provider,
	)

	return &ExecutionResult{
		Prediction: finalResult,
		Status:     "SUCCESS",
	}, nil
}

// queryAIOrFallback queries 9router endpoint via CRE HTTP or computes deterministic fallback.
func queryAIOrFallback(
	config *Config,
	runtime cre.Runtime,
	httpClient *creHttp.Client,
	price, change24h, high, low float64,
	apiKey string,
	logger *slog.Logger,
) (*AIPredictionResponse, string) {
	if config.AIBaseURL != "" {
		prompt := fmt.Sprintf(`Predict the next 5-minute price action for %s.
Current Price: %.2f USD
24h Price Change: %.2f%%
24h High: %.2f USD
24h Low: %.2f USD

Provide your analysis strictly in this JSON format without markdown code blocks:
{"direction": "LONG"|"SHORT"|"NEUTRAL", "target_price": number, "stop_loss": number, "confidence": number between 50-95, "position_size_usd": number, "reasoning": "brief explanation"}`,
			config.Symbol, price, change24h, high, low)

		reqPayload := map[string]interface{}{
			"model": config.AIModel,
			"messages": []map[string]string{
				{"role": "system", "content": "You are an autonomous quant trading prediction agent. You output only valid JSON."},
				{"role": "user", "content": prompt},
			},
			"temperature": 0.2,
		}

		payloadBytes, err := json.Marshal(reqPayload)
		if err == nil {
			aiEndpoint := strings.TrimRight(config.AIBaseURL, "/") + "/chat/completions"
			aiRespJSON, aiErr := creHttp.SendRequest(config, runtime, httpClient, func(cfg *Config, log *slog.Logger, requester *creHttp.SendRequester) (string, error) {
				headers := map[string]string{
					"Content-Type": "application/json",
				}
				if apiKey != "" {
					headers["Authorization"] = "Bearer " + apiKey
				}
				req := &creHttp.Request{
					Url:     aiEndpoint,
					Method:  "POST",
					Headers: headers,
					Body:    payloadBytes,
				}
				resp, err := requester.SendRequest(req).Await()
				if err != nil {
					return "", err
				}
				return string(resp.Body), nil
			}, cre.ConsensusIdenticalAggregation[string]()).Await()

			if aiErr == nil && aiRespJSON != "" {
				// Parse standard OpenAI chat completion response
				var chatResp struct {
					Choices []struct {
						Message struct {
							Content string `json:"content"`
						} `json:"message"`
					} `json:"choices"`
				}

				if err := json.Unmarshal([]byte(aiRespJSON), &chatResp); err == nil && len(chatResp.Choices) > 0 {
					content := strings.TrimSpace(chatResp.Choices[0].Message.Content)
					if strings.HasPrefix(content, "```json") {
						content = strings.TrimPrefix(content, "```json")
						content = strings.TrimSuffix(content, "```")
					} else if strings.HasPrefix(content, "```") {
						content = strings.TrimPrefix(content, "```")
						content = strings.TrimSuffix(content, "```")
					}
					content = strings.TrimSpace(content)

					var aiPrediction AIPredictionResponse
					if err := json.Unmarshal([]byte(content), &aiPrediction); err == nil && aiPrediction.Direction != "" {
						if aiPrediction.PositionSizeUSD <= 0 {
							aiPrediction.PositionSizeUSD = config.DefaultPositionUSD
						}
						return &aiPrediction, fmt.Sprintf("9router-ai (%s)", config.AIModel)
					}
				}
			} else {
				logger.Warn("AI provider request failed or not accessible, using deterministic technical model", "err", aiErr)
			}
		}
	}

	// Deterministic Technical Momentum Model (Fallback)
	return computeTechnicalPrediction(config, price, change24h, high, low), "deterministic-technical-model"
}

// computeTechnicalPrediction provides a reliable deterministic prediction based on 24h momentum and range.
func computeTechnicalPrediction(config *Config, price, change24h, high, low float64) *AIPredictionResponse {
	posSize := config.DefaultPositionUSD
	if posSize <= 0 {
		posSize = 100.0
	}

	rangeWidth := high - low
	var rangePosition float64
	if rangeWidth > 0 {
		rangePosition = (price - low) / rangeWidth
	} else {
		rangePosition = 0.5
	}

	// Mean reversion and momentum calculation
	var direction string
	var targetPrice, stopLoss float64
	var confidence int
	var reasoning string

	if change24h > 1.5 && rangePosition < 0.85 {
		// Strong upward momentum not yet at ceiling
		direction = "LONG"
		targetPrice = math.Round(price*1.018*100) / 100
		stopLoss = math.Round(price*0.991*100) / 100
		confidence = 78
		reasoning = fmt.Sprintf("Bullish momentum (+%.2f%% 24h) with room to range high; target +1.8%% take profit, -0.9%% stop loss", change24h)
	} else if change24h < -1.5 && rangePosition > 0.15 {
		// Downward trend continuing
		direction = "SHORT"
		targetPrice = math.Round(price*0.982*100) / 100
		stopLoss = math.Round(price*1.009*100) / 100
		confidence = 74
		reasoning = fmt.Sprintf("Bearish momentum (%.2f%% 24h); targeting short continuation at -1.8%% with tight stop", change24h)
	} else if rangePosition <= 0.20 {
		// Oversold at bottom of range -> Bounce LONG
		direction = "LONG"
		targetPrice = math.Round(price*1.015*100) / 100
		stopLoss = math.Round(price*0.992*100) / 100
		confidence = 81
		reasoning = fmt.Sprintf("Mean reversion bounce from 24h support floor (range position %.1f%%)", rangePosition*100)
	} else if rangePosition >= 0.80 {
		// Overbought near top of range -> Pullback SHORT
		direction = "SHORT"
		targetPrice = math.Round(price*0.985*100) / 100
		stopLoss = math.Round(price*1.008*100) / 100
		confidence = 79
		reasoning = fmt.Sprintf("Mean reversion rejection from 24h resistance ceiling (range position %.1f%%)", rangePosition*100)
	} else {
		// Consolidation
		direction = "LONG"
		targetPrice = math.Round(price*1.010*100) / 100
		stopLoss = math.Round(price*0.993*100) / 100
		confidence = 65
		reasoning = fmt.Sprintf("Mid-range consolidation (%.1f%% of 24h band); scalping minor upward drift", rangePosition*100)
	}

	return &AIPredictionResponse{
		Direction:       direction,
		TargetPrice:     targetPrice,
		StopLoss:        stopLoss,
		Confidence:      confidence,
		PositionSizeUSD: posSize,
		Reasoning:       reasoning,
	}
}
