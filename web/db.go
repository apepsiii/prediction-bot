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
		duration_minutes INTEGER DEFAULT 5,
		user_choice TEXT NOT NULL,
		ai_choice TEXT NOT NULL,
		ai_confidence INTEGER DEFAULT 75,
		ai_reasoning TEXT,
		ai_model TEXT,
		actual_outcome TEXT DEFAULT 'PENDING',
		user_won INTEGER DEFAULT 0,
		ai_won INTEGER DEFAULT 0,
		outcome_summary TEXT DEFAULT 'PENDING',
		status TEXT DEFAULT 'ACTIVE',
		ai_stage1_choice TEXT DEFAULT '',
		ai_stage1_conf INTEGER DEFAULT 0,
		ai_stage1_reason TEXT DEFAULT '',
		ai_stage2_choice TEXT DEFAULT '',
		ai_stage2_conf INTEGER DEFAULT 0,
		ai_stage2_reason TEXT DEFAULT '',
		ai_stage3_choice TEXT DEFAULT '',
		ai_stage3_conf INTEGER DEFAULT 0,
		ai_stage3_reason TEXT DEFAULT '',
		whale_alert TEXT DEFAULT '',
		ai_m2_info TEXT DEFAULT '',
		ai_m3_info TEXT DEFAULT '',
		ai_m4_info TEXT DEFAULT ''
	);
	CREATE INDEX IF NOT EXISTS idx_rounds_status ON rounds(status);
	CREATE INDEX IF NOT EXISTS idx_rounds_cutoff ON rounds(cutoff_at);
	`

	_, err = db.Exec(createTableSQL)
	if err != nil {
		return fmt.Errorf("failed to initialize rounds table: %w", err)
	}

	// Idempotent column migrations for 3-stage predictions and mid-round radar
	migrationCols := []string{
		"ALTER TABLE rounds ADD COLUMN ai_stage1_choice TEXT DEFAULT ''",
		"ALTER TABLE rounds ADD COLUMN ai_stage1_conf INTEGER DEFAULT 0",
		"ALTER TABLE rounds ADD COLUMN ai_stage1_reason TEXT DEFAULT ''",
		"ALTER TABLE rounds ADD COLUMN ai_stage2_choice TEXT DEFAULT ''",
		"ALTER TABLE rounds ADD COLUMN ai_stage2_conf INTEGER DEFAULT 0",
		"ALTER TABLE rounds ADD COLUMN ai_stage2_reason TEXT DEFAULT ''",
		"ALTER TABLE rounds ADD COLUMN ai_stage3_choice TEXT DEFAULT ''",
		"ALTER TABLE rounds ADD COLUMN ai_stage3_conf INTEGER DEFAULT 0",
		"ALTER TABLE rounds ADD COLUMN ai_stage3_reason TEXT DEFAULT ''",
		"ALTER TABLE rounds ADD COLUMN whale_alert TEXT DEFAULT ''",
		"ALTER TABLE rounds ADD COLUMN ai_m2_info TEXT DEFAULT ''",
		"ALTER TABLE rounds ADD COLUMN ai_m3_info TEXT DEFAULT ''",
		"ALTER TABLE rounds ADD COLUMN ai_m4_info TEXT DEFAULT ''",
	}
	for _, sqlCol := range migrationCols {
		_, _ = db.Exec(sqlCol)
	}

	log.Printf("[DATABASE] SQLite connected: %s (WAL Mode)", dbPath)
	return nil
}

const roundSelectCols = `
	id, symbol, start_time_wib, cutoff_time_wib, created_at, cutoff_at,
	start_price, threshold_price, final_price, duration_minutes,
	user_choice, ai_choice, ai_confidence, ai_reasoning, ai_model,
	actual_outcome, user_won, ai_won, outcome_summary, status,
	COALESCE(ai_stage1_choice, '') as ai_stage1_choice,
	COALESCE(ai_stage1_conf, 0) as ai_stage1_conf,
	COALESCE(ai_stage1_reason, '') as ai_stage1_reason,
	COALESCE(ai_stage2_choice, '') as ai_stage2_choice,
	COALESCE(ai_stage2_conf, 0) as ai_stage2_conf,
	COALESCE(ai_stage2_reason, '') as ai_stage2_reason,
	COALESCE(ai_stage3_choice, '') as ai_stage3_choice,
	COALESCE(ai_stage3_conf, 0) as ai_stage3_conf,
	COALESCE(ai_stage3_reason, '') as ai_stage3_reason,
	COALESCE(whale_alert, '') as whale_alert,
	COALESCE(ai_m2_info, '') as ai_m2_info,
	COALESCE(ai_m3_info, '') as ai_m3_info,
	COALESCE(ai_m4_info, '') as ai_m4_info
`

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
		actual_outcome, user_won, ai_won, outcome_summary, status,
		ai_stage1_choice, ai_stage1_conf, ai_stage1_reason,
		ai_stage2_choice, ai_stage2_conf, ai_stage2_reason,
		ai_stage3_choice, ai_stage3_conf, ai_stage3_reason,
		whale_alert,
		ai_m2_info, ai_m3_info, ai_m4_info
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
		r.AIStage1Choice, r.AIStage1Conf, r.AIStage1Reason,
		r.AIStage2Choice, r.AIStage2Conf, r.AIStage2Reason,
		r.AIStage3Choice, r.AIStage3Conf, r.AIStage3Reason,
		r.WhaleAlert,
		r.AIM2Info, r.AIM3Info, r.AIM4Info,
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

	query := `SELECT ` + roundSelectCols + ` FROM rounds WHERE status = 'ACTIVE' ORDER BY cutoff_at ASC`
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

	query := `SELECT ` + roundSelectCols + ` FROM rounds ORDER BY created_at DESC LIMIT ?`
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

// UpdateAIStage1 updates Stage 1 (Initial / Detik 0) forecast
func UpdateAIStage1(roundID string, choice string, conf int, reason string) error {
	dbLock.Lock()
	defer dbLock.Unlock()

	query := `UPDATE rounds SET ai_stage1_choice = ?, ai_stage1_conf = ?, ai_stage1_reason = ?, ai_choice = ?, ai_confidence = ?, ai_reasoning = ? WHERE id = ? AND status = 'ACTIVE'`
	_, err := db.Exec(query, choice, conf, reason, choice, conf, reason, roundID)
	return err
}

// UpdateAIStage2 updates Stage 2 (Flow Confirmation / Detik 30) forecast
func UpdateAIStage2(roundID string, choice string, conf int, reason string, whaleAlert string) error {
	dbLock.Lock()
	defer dbLock.Unlock()

	query := `UPDATE rounds SET ai_stage2_choice = ?, ai_stage2_conf = ?, ai_stage2_reason = ?, whale_alert = ? WHERE id = ? AND status = 'ACTIVE'`
	_, err := db.Exec(query, choice, conf, reason, whaleAlert, roundID)
	return err
}

// UpdateAIStage3AndVerdict updates Stage 3 (Detik 60) and locks the official AI Final Verdict
func UpdateAIStage3AndVerdict(roundID string, stage3Choice string, stage3Conf int, stage3Reason string, finalChoice string, finalConf int, finalReason string, model string, whaleAlert string) error {
	dbLock.Lock()
	defer dbLock.Unlock()

	query := `UPDATE rounds SET ai_stage3_choice = ?, ai_stage3_conf = ?, ai_stage3_reason = ?, ai_choice = ?, ai_confidence = ?, ai_reasoning = ?, ai_model = ?, whale_alert = ? WHERE id = ? AND status = 'ACTIVE'`
	_, err := db.Exec(query, stage3Choice, stage3Conf, stage3Reason, finalChoice, finalConf, finalReason, model, whaleAlert, roundID)
	return err
}

// UpdateAIMidRoundInfo stores informational radar telemetry analysis for Minute 2, 3, or 4
func UpdateAIMidRoundInfo(roundID string, minute int, info string) error {
	dbLock.Lock()
	defer dbLock.Unlock()

	var col string
	switch minute {
	case 2:
		col = "ai_m2_info"
	case 3:
		col = "ai_m3_info"
	case 4:
		col = "ai_m4_info"
	default:
		return fmt.Errorf("invalid minute for radar info: %d", minute)
	}

	query := fmt.Sprintf("UPDATE rounds SET %s = ? WHERE id = ? AND status = 'ACTIVE'", col)
	_, err := db.Exec(query, info, roundID)
	return err
}

// UpdateAIAdaptiveChoice adapts official AI bet and reasoning if a strong reversal signal is confirmed in Minute 3 or 4
func UpdateAIAdaptiveChoice(roundID string, newChoice string, newConf int, reasonAppend string) error {
	dbLock.Lock()
	defer dbLock.Unlock()

	query := `UPDATE rounds SET ai_choice = ?, ai_confidence = ?, ai_reasoning = ai_reasoning || ' ' || ? WHERE id = ? AND status = 'ACTIVE'`
	_, err := db.Exec(query, newChoice, newConf, reasonAppend, roundID)
	return err
}

// GetActiveRoundBySymbol retrieves the currently active round for a symbol
func GetActiveRoundBySymbol(symbol string) (*RoundRecord, error) {
	dbLock.RLock()
	defer dbLock.RUnlock()

	query := `SELECT ` + roundSelectCols + ` FROM rounds WHERE symbol = ? AND status = 'ACTIVE' ORDER BY cutoff_at DESC LIMIT 1`
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

	query := `SELECT ` + roundSelectCols + ` FROM rounds WHERE symbol = ? AND status = 'CONSOLIDATED' ORDER BY cutoff_at DESC LIMIT 1`
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
			&r.AIStage1Choice, &r.AIStage1Conf, &r.AIStage1Reason,
			&r.AIStage2Choice, &r.AIStage2Conf, &r.AIStage2Reason,
			&r.AIStage3Choice, &r.AIStage3Conf, &r.AIStage3Reason,
			&r.WhaleAlert,
			&r.AIM2Info, &r.AIM3Info, &r.AIM4Info,
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
