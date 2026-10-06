package main

import (
	"database/sql"
	"fmt"
	"log"
	"math"
	"os"
	"path/filepath"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

var (
	db     *sql.DB
	dbLock sync.RWMutex
	wibLoc *time.Location
)

func initWIB() {
	// Fixed UTC+7 location for WIB
	wibLoc = time.FixedZone("WIB", 7*3600)
}

func initDB() error {
	initWIB()

	dataDir := filepath.Join(rootDir, "data")
	_ = os.MkdirAll(dataDir, 0755)
	dbPath := filepath.Join(dataDir, "prediction.db")

	var err error
	db, err = sql.Open("sqlite", dbPath+"?_pragma=busy_timeout(10000)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)")
	if err != nil {
		return fmt.Errorf("failed to open sqlite database: %w", err)
	}

	createTableSQL := `
	CREATE TABLE IF NOT EXISTS rounds (
		id TEXT PRIMARY KEY,
		symbol TEXT NOT NULL,
		start_time_wib TEXT NOT NULL,
		cutoff_time_wib TEXT NOT NULL,
		created_at INTEGER NOT NULL,
		cutoff_at INTEGER NOT NULL,
		start_price REAL NOT NULL,
		threshold_price REAL NOT NULL,
		final_price REAL DEFAULT 0,
		duration_minutes INTEGER DEFAULT 15,
		user_choice TEXT NOT NULL,
		ai_choice TEXT NOT NULL,
		ai_confidence INTEGER DEFAULT 75,
		ai_reasoning TEXT,
		ai_model TEXT,
		actual_outcome TEXT DEFAULT 'PENDING',
		user_won INTEGER DEFAULT 0,
		ai_won INTEGER DEFAULT 0,
		outcome_summary TEXT DEFAULT 'PENDING',
		status TEXT DEFAULT 'ACTIVE'
	);
	CREATE INDEX IF NOT EXISTS idx_rounds_status ON rounds(status);
	CREATE INDEX IF NOT EXISTS idx_rounds_cutoff ON rounds(cutoff_at);
	`

	_, err = db.Exec(createTableSQL)
	if err != nil {
		return fmt.Errorf("failed to initialize rounds table: %w", err)
	}

	log.Printf("[DATABASE] SQLite connected: %s (WAL Mode)", dbPath)
	return nil
}

// FormatWIB formats a Unix timestamp into readable WIB string
func FormatWIB(t time.Time) string {
	if wibLoc == nil {
		initWIB()
	}
	return t.In(wibLoc).Format("15:04:05 WIB")
}

func FormatWIBFull(t time.Time) string {
	if wibLoc == nil {
		initWIB()
	}
	return t.In(wibLoc).Format("2006-01-02 15:04:05 WIB")
}

// InsertRound inserts a new active round
func InsertRound(r *RoundRecord) error {
	dbLock.Lock()
	defer dbLock.Unlock()

	query := `
	INSERT INTO rounds (
		id, symbol, start_time_wib, cutoff_time_wib, created_at, cutoff_at,
		start_price, threshold_price, final_price, duration_minutes,
		user_choice, ai_choice, ai_confidence, ai_reasoning, ai_model,
		actual_outcome, user_won, ai_won, outcome_summary, status
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`

	userWonInt := 0
	if r.UserWon {
		userWonInt = 1
	}
	aiWonInt := 0
	if r.AIWon {
		aiWonInt = 1
	}

	_, err := db.Exec(query,
		r.ID, r.Symbol, r.StartTimeWIB, r.CutoffTimeWIB, r.CreatedAt, r.CutoffAt,
		r.StartPrice, r.ThresholdPrice, r.FinalPrice, r.DurationMinutes,
		r.UserChoice, r.AIChoice, r.AIConfidence, r.AIReasoning, r.AIModel,
		r.ActualOutcome, userWonInt, aiWonInt, r.OutcomeSummary, r.Status,
	)
	return err
}

// UpdateConsolidatedRound records final price and resolves the round
func UpdateConsolidatedRound(r *RoundRecord) error {
	dbLock.Lock()
	defer dbLock.Unlock()

	query := `
	UPDATE rounds SET
		final_price = ?,
		actual_outcome = ?,
		user_won = ?,
		ai_won = ?,
		outcome_summary = ?,
		status = 'CONSOLIDATED'
	WHERE id = ?
	`

	userWonInt := 0
	if r.UserWon {
		userWonInt = 1
	}
	aiWonInt := 0
	if r.AIWon {
		aiWonInt = 1
	}

	_, err := db.Exec(query,
		r.FinalPrice, r.ActualOutcome, userWonInt, aiWonInt, r.OutcomeSummary, r.ID,
	)
	return err
}

// GetActiveRounds retrieves all rounds that are pending consolidation
func GetActiveRounds() ([]RoundRecord, error) {
	dbLock.RLock()
	defer dbLock.RUnlock()

	query := `SELECT * FROM rounds WHERE status = 'ACTIVE' ORDER BY cutoff_at ASC`
	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return scanRounds(rows)
}

// GetAllRounds retrieves rounds with pagination/limit
func GetAllRounds(limit int) ([]RoundRecord, error) {
	dbLock.RLock()
	defer dbLock.RUnlock()

	query := `SELECT * FROM rounds ORDER BY created_at DESC LIMIT ?`
	rows, err := db.Query(query, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return scanRounds(rows)
}

// UpdateUserChoice updates the human player's choice for an active round
func UpdateUserChoice(roundID string, userChoice string) error {
	dbLock.Lock()
	defer dbLock.Unlock()

	query := `UPDATE rounds SET user_choice = ? WHERE id = ? AND status = 'ACTIVE'`
	_, err := db.Exec(query, userChoice, roundID)
	return err
}

// UpdateAIRoundPrediction updates AI's forecast on an active round
func UpdateAIRoundPrediction(roundID string, aiChoice string, aiConf int, aiReasoning string, aiModel string) error {
	dbLock.Lock()
	defer dbLock.Unlock()

	query := `UPDATE rounds SET ai_choice = ?, ai_confidence = ?, ai_reasoning = ?, ai_model = ? WHERE id = ? AND status = 'ACTIVE'`
	_, err := db.Exec(query, aiChoice, aiConf, aiReasoning, aiModel, roundID)
	return err
}

// GetActiveRoundBySymbol retrieves the currently active round for a symbol
func GetActiveRoundBySymbol(symbol string) (*RoundRecord, error) {
	dbLock.RLock()
	defer dbLock.RUnlock()

	query := `SELECT * FROM rounds WHERE symbol = ? AND status = 'ACTIVE' ORDER BY cutoff_at DESC LIMIT 1`
	rows, err := db.Query(query, symbol)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	rounds, err := scanRounds(rows)
	if err != nil || len(rounds) == 0 {
		return nil, err
	}
	return &rounds[0], nil
}

// GetLatestConsolidatedRound returns the most recently completed round for a symbol
func GetLatestConsolidatedRound(symbol string) (*RoundRecord, error) {
	dbLock.RLock()
	defer dbLock.RUnlock()

	query := `SELECT * FROM rounds WHERE symbol = ? AND status = 'CONSOLIDATED' ORDER BY cutoff_at DESC LIMIT 1`
	rows, err := db.Query(query, symbol)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	rounds, err := scanRounds(rows)
	if err != nil || len(rounds) == 0 {
		return nil, err
	}
	return &rounds[0], nil
}

// GetLeaderboardStats computes overall win/loss/draw counts and winrates
func GetLeaderboardStats() (map[string]interface{}, error) {
	dbLock.RLock()
	defer dbLock.RUnlock()

	query := `
	SELECT 
		COUNT(*) as total_rounds,
		SUM(CASE WHEN status = 'CONSOLIDATED' THEN 1 ELSE 0 END) as consolidated_rounds,
		SUM(CASE WHEN status = 'CONSOLIDATED' AND user_choice IN ('UP', 'DOWN') AND user_won = 1 AND ai_won = 0 THEN 1 ELSE 0 END) as human_solo_wins,
		SUM(CASE WHEN status = 'CONSOLIDATED' AND ai_won = 1 AND (user_won = 0 OR user_choice NOT IN ('UP', 'DOWN')) THEN 1 ELSE 0 END) as ai_solo_wins,
		SUM(CASE WHEN status = 'CONSOLIDATED' AND user_choice IN ('UP', 'DOWN') AND user_won = 1 AND ai_won = 1 THEN 1 ELSE 0 END) as draw_wins,
		SUM(CASE WHEN status = 'CONSOLIDATED' AND user_choice IN ('UP', 'DOWN') AND user_won = 0 AND ai_won = 0 THEN 1 ELSE 0 END) as draw_losses,
		SUM(CASE WHEN status = 'CONSOLIDATED' AND user_choice IN ('UP', 'DOWN') THEN 1 ELSE 0 END) as human_played_rounds
	FROM rounds
	`

	var total, consolidated, humanSoloWins, aiSoloWins, drawWins, drawLosses, humanPlayed sql.NullInt64
	err := db.QueryRow(query).Scan(&total, &consolidated, &humanSoloWins, &aiSoloWins, &drawWins, &drawLosses, &humanPlayed)
	if err != nil {
		return nil, err
	}

	cCount := int(consolidated.Int64)
	hSolo := int(humanSoloWins.Int64)
	aSolo := int(aiSoloWins.Int64)
	dWins := int(drawWins.Int64)
	dLosses := int(drawLosses.Int64)
	hPlayed := int(humanPlayed.Int64)
	totalDraws := dWins + dLosses

	humanTotalWins := hSolo + dWins
	aiTotalWins := aSolo + dWins

	var humanWR, aiWR float64
	if hPlayed > 0 {
		humanWR = math.Round((float64(humanTotalWins)/float64(hPlayed))*1000) / 10
	}
	if cCount > 0 {
		aiWR = math.Round((float64(aiTotalWins)/float64(cCount))*1000) / 10
	}

	return map[string]interface{}{
		"total_rounds":        int(total.Int64),
		"consolidated_rounds": cCount,
		"active_rounds":       int(total.Int64) - cCount,
		"human_solo_wins":     hSolo,
		"ai_solo_wins":        aSolo,
		"draw_wins":           dWins,
		"draw_losses":         dLosses,
		"total_draws":         totalDraws,
		"human_total_wins":    humanTotalWins,
		"ai_total_wins":       aiTotalWins,
		"human_played":        hPlayed,
		"human_winrate_pct":   humanWR,
		"ai_winrate_pct":      aiWR,
	}, nil
}

func scanRounds(rows *sql.Rows) ([]RoundRecord, error) {
	var results []RoundRecord
	for rows.Next() {
		var r RoundRecord
		var uWon, aWon int
		err := rows.Scan(
			&r.ID, &r.Symbol, &r.StartTimeWIB, &r.CutoffTimeWIB, &r.CreatedAt, &r.CutoffAt,
			&r.StartPrice, &r.ThresholdPrice, &r.FinalPrice, &r.DurationMinutes,
			&r.UserChoice, &r.AIChoice, &r.AIConfidence, &r.AIReasoning, &r.AIModel,
			&r.ActualOutcome, &uWon, &aWon, &r.OutcomeSummary, &r.Status,
		)
		if err != nil {
			return nil, err
		}
		r.UserWon = (uWon == 1)
		r.AIWon = (aWon == 1)
		results = append(results, r)
	}
	return results, nil
}
