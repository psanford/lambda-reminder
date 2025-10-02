package scheduler

import (
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/psanford/lambda-reminder/config"
	"github.com/psanford/lambda-reminder/state"
)

func TestValidateRule(t *testing.T) {
	lgr := slog.New(slog.NewTextHandler(os.Stderr, nil))
	s := New(lgr)

	tests := []struct {
		name    string
		rule    config.Rule
		wantErr bool
	}{
		{
			name: "valid daily cron",
			rule: config.Rule{
				Name: "daily",
				Cron: "0 9 * * *",
			},
			wantErr: false,
		},
		{
			name: "valid weekday cron",
			rule: config.Rule{
				Name: "weekdays",
				Cron: "0 9 * * 1-5",
			},
			wantErr: false,
		},
		{
			name: "valid last day of month cron",
			rule: config.Rule{
				Name: "monthly_last_day",
				Cron: "0 9 L * *",
			},
			wantErr: false,
		},
		{
			name: "invalid cron expression",
			rule: config.Rule{
				Name: "invalid",
				Cron: "invalid cron",
			},
			wantErr: true,
		},
		{
			name: "invalid cron with wrong fields",
			rule: config.Rule{
				Name: "wrong_fields",
				Cron: "0 9 * *",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := s.ValidateRule(&tt.rule)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateRule() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestGetNextRunTime(t *testing.T) {
	lgr := slog.New(slog.NewTextHandler(os.Stderr, nil))
	s := New(lgr)

	// Test time: 2024-01-15 08:00:00 (Monday)
	testTime := time.Date(2024, 1, 15, 8, 0, 0, 0, time.UTC)

	tests := []struct {
		name     string
		cronExpr string
		fromTime time.Time
		wantErr  bool
		checkFn  func(time.Time) bool
	}{
		{
			name:     "daily at 9am",
			cronExpr: "0 9 * * *",
			fromTime: testTime,
			wantErr:  false,
			checkFn: func(next time.Time) bool {
				// Should be today at 9am (1 hour later)
				expected := time.Date(2024, 1, 15, 9, 0, 0, 0, time.UTC)
				return next.Equal(expected)
			},
		},
		{
			name:     "daily at 7am (past time today)",
			cronExpr: "0 7 * * *",
			fromTime: testTime,
			wantErr:  false,
			checkFn: func(next time.Time) bool {
				// Should be tomorrow at 7am
				expected := time.Date(2024, 1, 16, 7, 0, 0, 0, time.UTC)
				return next.Equal(expected)
			},
		},
		{
			name:     "weekdays only",
			cronExpr: "0 9 * * 1-5",
			fromTime: testTime,
			wantErr:  false,
			checkFn: func(next time.Time) bool {
				// Should be today at 9am (Monday)
				expected := time.Date(2024, 1, 15, 9, 0, 0, 0, time.UTC)
				return next.Equal(expected)
			},
		},
		{
			name:     "last day of month",
			cronExpr: "0 9 L * *",
			fromTime: testTime,
			wantErr:  false,
			checkFn: func(next time.Time) bool {
				// Should be January 31st at 9am (last day of January 2024)
				expected := time.Date(2024, 1, 31, 9, 0, 0, 0, time.UTC)
				return next.Equal(expected)
			},
		},
		{
			name:     "last day of February (leap year)",
			cronExpr: "0 9 L * *",
			fromTime: time.Date(2024, 2, 15, 8, 0, 0, 0, time.UTC), // Feb 15, 2024 (leap year)
			wantErr:  false,
			checkFn: func(next time.Time) bool {
				// Should be February 29th at 9am (2024 is a leap year)
				expected := time.Date(2024, 2, 29, 9, 0, 0, 0, time.UTC)
				return next.Equal(expected)
			},
		},
		{
			name:     "last day of February (non-leap year)",
			cronExpr: "0 9 L * *",
			fromTime: time.Date(2023, 2, 15, 8, 0, 0, 0, time.UTC), // Feb 15, 2023 (non-leap year)
			wantErr:  false,
			checkFn: func(next time.Time) bool {
				// Should be February 28th at 9am (2023 is not a leap year)
				expected := time.Date(2023, 2, 28, 9, 0, 0, 0, time.UTC)
				return next.Equal(expected)
			},
		},
		{
			name:     "last day of month when already on last day (before time)",
			cronExpr: "0 9 L * *",
			fromTime: time.Date(2024, 1, 31, 8, 0, 0, 0, time.UTC), // Jan 31 at 8am
			wantErr:  false,
			checkFn: func(next time.Time) bool {
				// Should be today at 9am since we're before the scheduled time
				expected := time.Date(2024, 1, 31, 9, 0, 0, 0, time.UTC)
				return next.Equal(expected)
			},
		},
		{
			name:     "last day of month when already on last day (after time)",
			cronExpr: "0 9 L * *",
			fromTime: time.Date(2024, 1, 31, 10, 0, 0, 0, time.UTC), // Jan 31 at 10am (past 9am)
			wantErr:  false,
			checkFn: func(next time.Time) bool {
				// Should be next month's last day at 9am (Feb 29, 2024 is leap year)
				expected := time.Date(2024, 2, 29, 9, 0, 0, 0, time.UTC)
				return next.Equal(expected)
			},
		},
		{
			name:     "invalid cron",
			cronExpr: "invalid",
			fromTime: testTime,
			wantErr:  true,
			checkFn:  nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			nextTime, err := s.GetNextRunTime(tt.cronExpr, tt.fromTime)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetNextRunTime() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr && tt.checkFn != nil {
				if !tt.checkFn(nextTime) {
					t.Errorf("GetNextRunTime() = %v, failed check function", nextTime)
				}
			}
		})
	}
}

func TestIsDue(t *testing.T) {
	lgr := slog.New(slog.NewTextHandler(os.Stderr, nil))
	s := New(lgr)

	now := time.Date(2024, 1, 15, 9, 0, 0, 0, time.UTC)
	pastTime := time.Date(2024, 1, 15, 8, 0, 0, 0, time.UTC)
	futureTime := time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC)

	tests := []struct {
		name     string
		cronExpr string
		lastRun  time.Time
		nextRun  time.Time
		now      time.Time
		want     bool
	}{
		{
			name:     "no next run time (zero time)",
			cronExpr: "0 9 * * *",
			lastRun:  time.Time{},
			nextRun:  time.Time{},
			now:      now,
			want:     true,
		},
		{
			name:     "next run time in past",
			cronExpr: "0 9 * * *",
			lastRun:  pastTime,
			nextRun:  pastTime,
			now:      now,
			want:     true,
		},
		{
			name:     "next run time is now",
			cronExpr: "0 9 * * *",
			lastRun:  pastTime,
			nextRun:  now,
			now:      now,
			want:     true,
		},
		{
			name:     "next run time in future",
			cronExpr: "0 9 * * *",
			lastRun:  pastTime,
			nextRun:  futureTime,
			now:      now,
			want:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := s.IsDue(tt.cronExpr, tt.lastRun, tt.nextRun, tt.now)
			if got != tt.want {
				t.Errorf("IsDue() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetDueRules(t *testing.T) {
	lgr := slog.New(slog.NewTextHandler(os.Stderr, nil))
	s := New(lgr)

	now := time.Date(2024, 1, 15, 9, 0, 0, 0, time.UTC)
	pastTime := time.Date(2024, 1, 15, 8, 0, 0, 0, time.UTC)
	futureTime := time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC)

	conf := &config.Config{
		Rules: []config.Rule{
			{
				Name: "due_rule",
				Cron: "0 9 * * *",
			},
			{
				Name: "not_due_rule",
				Cron: "0 10 * * *",
			},
			{
				Name: "new_rule",
				Cron: "0 11 * * *",
			},
			{
				Name: "changed_cron_rule",
				Cron: "0 12 * * *", // Changed from 0 9 * * *
			},
		},
	}

	st := &state.State{
		Rules: map[string]state.RuleState{
			"due_rule": {
				Name:        "due_rule",
				CronExpr:    "0 9 * * *",
				LastRunTime: pastTime,
				NextRunTime: now, // Due now
			},
			"not_due_rule": {
				Name:        "not_due_rule",
				CronExpr:    "0 10 * * *",
				LastRunTime: pastTime,
				NextRunTime: futureTime, // Not due yet
			},
			"changed_cron_rule": {
				Name:        "changed_cron_rule",
				CronExpr:    "0 9 * * *", // Old cron, will be updated
				LastRunTime: pastTime,
				NextRunTime: now,
			},
		},
	}

	dueRules, err := s.GetDueRules(conf, st, now)
	if err != nil {
		t.Fatalf("GetDueRules() error = %v", err)
	}

	// Should have 1 due rule: only due_rule (new_rule gets initialized but not marked as due)
	if len(dueRules) != 1 {
		t.Errorf("GetDueRules() returned %d rules, want 1", len(dueRules))
	}

	// Check that we got the right rule
	if len(dueRules) > 0 && dueRules[0].Name != "due_rule" {
		t.Errorf("Expected due_rule to be the only due rule, got %s", dueRules[0].Name)
	}

	// Verify that new_rule state was created but rule is not due
	newRuleState, exists := st.Rules["new_rule"]
	if !exists {
		t.Error("Expected new_rule state to be created")
	} else {
		// new_rule should have a next run time calculated but not be due yet
		if newRuleState.NextRunTime.IsZero() {
			t.Error("Expected new_rule to have next run time calculated")
		}
		if !newRuleState.LastRunTime.IsZero() {
			t.Error("Expected new_rule to have zero last run time")
		}
	}

	// Check that changed_cron_rule state was updated
	updatedState := st.Rules["changed_cron_rule"]
	if updatedState.CronExpr != "0 12 * * *" {
		t.Errorf("Expected changed_cron_rule cron to be updated to '0 12 * * *', got '%s'", updatedState.CronExpr)
	}
}

func TestUpdateRuleState(t *testing.T) {
	lgr := slog.New(slog.NewTextHandler(os.Stderr, nil))
	s := New(lgr)

	st := &state.State{
		Rules: make(map[string]state.RuleState),
	}

	runTime := time.Date(2024, 1, 15, 9, 0, 0, 0, time.UTC)
	cronExpr := "0 9 * * *"

	err := s.UpdateRuleState(st, "test_rule", cronExpr, runTime)
	if err != nil {
		t.Fatalf("UpdateRuleState() error = %v", err)
	}

	// Check that state was updated
	ruleState, exists := st.Rules["test_rule"]
	if !exists {
		t.Fatal("Rule state was not created")
	}

	if ruleState.Name != "test_rule" {
		t.Errorf("Expected name 'test_rule', got '%s'", ruleState.Name)
	}

	if ruleState.CronExpr != cronExpr {
		t.Errorf("Expected cron '%s', got '%s'", cronExpr, ruleState.CronExpr)
	}

	if !ruleState.LastRunTime.Equal(runTime) {
		t.Errorf("Expected last run time %v, got %v", runTime, ruleState.LastRunTime)
	}

	if ruleState.NextRunTime.IsZero() {
		t.Error("Next run time should not be zero")
	}

	// Next run should be tomorrow at 9am
	expectedNext := time.Date(2024, 1, 16, 9, 0, 0, 0, time.UTC)
	if !ruleState.NextRunTime.Equal(expectedNext) {
		t.Errorf("Expected next run time %v, got %v", expectedNext, ruleState.NextRunTime)
	}
}

func TestCronExpressionChange(t *testing.T) {
	lgr := slog.New(slog.NewTextHandler(os.Stderr, nil))
	s := New(lgr)

	now := time.Date(2024, 1, 15, 9, 0, 0, 0, time.UTC)
	pastTime := time.Date(2024, 1, 15, 8, 0, 0, 0, time.UTC)

	// Configuration with updated cron
	conf := &config.Config{
		Rules: []config.Rule{
			{
				Name: "test_rule",
				Cron: "0 17 * * *", // Changed to 5pm
			},
		},
	}

	// State with old cron
	st := &state.State{
		Rules: map[string]state.RuleState{
			"test_rule": {
				Name:        "test_rule",
				CronExpr:    "0 9 * * *", // Old cron was 9am
				LastRunTime: pastTime,
				NextRunTime: now,
			},
		},
	}

	// Get due rules - this should detect the cron change
	dueRules, err := s.GetDueRules(conf, st, now)
	if err != nil {
		t.Fatalf("GetDueRules() error = %v", err)
	}

	// Should not be due since cron changed
	if len(dueRules) != 0 {
		t.Errorf("Expected no due rules after cron change, got %d", len(dueRules))
	}

	// Check that state was updated with new cron
	updatedState := st.Rules["test_rule"]
	if updatedState.CronExpr != "0 17 * * *" {
		t.Errorf("Expected cron to be updated to '0 17 * * *', got '%s'", updatedState.CronExpr)
	}

	// Last run time should be preserved
	if !updatedState.LastRunTime.Equal(pastTime) {
		t.Errorf("Expected last run time to be preserved as %v, got %v", pastTime, updatedState.LastRunTime)
	}

	// Next run time should be recalculated for 5pm today
	expectedNext := time.Date(2024, 1, 15, 17, 0, 0, 0, time.UTC)
	if !updatedState.NextRunTime.Equal(expectedNext) {
		t.Errorf("Expected next run time %v, got %v", expectedNext, updatedState.NextRunTime)
	}
}

func TestNewRuleInitialization(t *testing.T) {
	lgr := slog.New(slog.NewTextHandler(os.Stderr, nil))
	s := New(lgr)

	// Current time: 2024-01-15 08:00:00 (Monday)
	now := time.Date(2024, 1, 15, 8, 0, 0, 0, time.UTC)

	// Configuration with a new rule that should run at 9am daily
	conf := &config.Config{
		Rules: []config.Rule{
			{
				Name: "new_daily_rule",
				Cron: "0 9 * * *", // 9am daily
			},
			{
				Name: "new_future_rule",
				Cron: "0 17 * * *", // 5pm daily
			},
		},
	}

	// Empty state - no existing rules
	st := &state.State{
		Rules: make(map[string]state.RuleState),
	}

	// Get due rules - should not mark new rules as due, but should initialize their state
	dueRules, err := s.GetDueRules(conf, st, now)
	if err != nil {
		t.Fatalf("GetDueRules() error = %v", err)
	}

	// Should have no due rules since new rules get their next run time calculated
	if len(dueRules) != 0 {
		t.Errorf("Expected no due rules for new rules, got %d", len(dueRules))
		for _, rule := range dueRules {
			t.Errorf("Unexpected due rule: %s", rule.Name)
		}
	}

	// Check that state was created for both new rules
	if len(st.Rules) != 2 {
		t.Fatalf("Expected 2 rules in state, got %d", len(st.Rules))
	}

	// Check first rule state (should run at 9am today since it's currently 8am)
	dailyState, exists := st.Rules["new_daily_rule"]
	if !exists {
		t.Fatal("new_daily_rule state was not created")
	}

	expectedDailyNext := time.Date(2024, 1, 15, 9, 0, 0, 0, time.UTC) // Today at 9am
	if !dailyState.NextRunTime.Equal(expectedDailyNext) {
		t.Errorf("Expected new_daily_rule next run time %v, got %v", expectedDailyNext, dailyState.NextRunTime)
	}

	if dailyState.CronExpr != "0 9 * * *" {
		t.Errorf("Expected new_daily_rule cron '0 9 * * *', got '%s'", dailyState.CronExpr)
	}

	if !dailyState.LastRunTime.IsZero() {
		t.Errorf("Expected new_daily_rule last run time to be zero, got %v", dailyState.LastRunTime)
	}

	// Check second rule state (should run at 5pm today)
	futureState, exists := st.Rules["new_future_rule"]
	if !exists {
		t.Fatal("new_future_rule state was not created")
	}

	expectedFutureNext := time.Date(2024, 1, 15, 17, 0, 0, 0, time.UTC) // Today at 5pm
	if !futureState.NextRunTime.Equal(expectedFutureNext) {
		t.Errorf("Expected new_future_rule next run time %v, got %v", expectedFutureNext, futureState.NextRunTime)
	}

	if futureState.CronExpr != "0 17 * * *" {
		t.Errorf("Expected new_future_rule cron '0 17 * * *', got '%s'", futureState.CronExpr)
	}

	if !futureState.LastRunTime.IsZero() {
		t.Errorf("Expected new_future_rule last run time to be zero, got %v", futureState.LastRunTime)
	}
}

func TestNewRuleBecomesDue(t *testing.T) {
	lgr := slog.New(slog.NewTextHandler(os.Stderr, nil))
	s := New(lgr)

	// Set up times for the test
	setupTime := time.Date(2024, 1, 15, 8, 0, 0, 0, time.UTC) // 8am - before scheduled time
	checkTime := time.Date(2024, 1, 15, 9, 0, 0, 0, time.UTC) // 9am - at scheduled time

	conf := &config.Config{
		Rules: []config.Rule{
			{
				Name: "test_rule",
				Cron: "0 9 * * *", // 9am daily
			},
		},
	}

	st := &state.State{
		Rules: make(map[string]state.RuleState),
	}

	// First call: Initialize the rule state (should not be due)
	dueRules, err := s.GetDueRules(conf, st, setupTime)
	if err != nil {
		t.Fatalf("GetDueRules() error = %v", err)
	}

	if len(dueRules) != 0 {
		t.Errorf("Expected no due rules during initialization, got %d", len(dueRules))
	}

	// Verify state was created
	if len(st.Rules) != 1 {
		t.Fatalf("Expected 1 rule in state after initialization, got %d", len(st.Rules))
	}

	// Second call: Check at the scheduled time (should now be due)
	dueRules, err = s.GetDueRules(conf, st, checkTime)
	if err != nil {
		t.Fatalf("GetDueRules() error = %v", err)
	}

	if len(dueRules) != 1 {
		t.Errorf("Expected 1 due rule at scheduled time, got %d", len(dueRules))
	}

	if len(dueRules) > 0 && dueRules[0].Name != "test_rule" {
		t.Errorf("Expected test_rule to be due, got %s", dueRules[0].Name)
	}
}
