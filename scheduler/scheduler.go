package scheduler

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/adhocore/gronx"
	"github.com/psanford/lambda-reminder/config"
	"github.com/psanford/lambda-reminder/state"
)

type Scheduler struct {
	cron *gronx.Gronx
	lgr  *slog.Logger
}

func New(lgr *slog.Logger) *Scheduler {
	return &Scheduler{
		cron: gronx.New(),
		lgr:  lgr,
	}
}

func (s *Scheduler) ValidateRule(rule *config.Rule) error {
	if !s.cron.IsValid(rule.Cron) {
		return fmt.Errorf("invalid cron expression: %s", rule.Cron)
	}
	return nil
}

func (s *Scheduler) GetNextRunTime(cronExpr string, fromTime time.Time) (time.Time, error) {
	if !s.cron.IsValid(cronExpr) {
		return time.Time{}, fmt.Errorf("invalid cron expression: %s", cronExpr)
	}

	nextTime, err := gronx.NextTickAfter(cronExpr, fromTime, false)
	if err != nil {
		return time.Time{}, fmt.Errorf("calculate next run time: %w", err)
	}

	if nextTime.IsZero() {
		return time.Time{}, fmt.Errorf("no next run time found for cron: %s", cronExpr)
	}

	return nextTime, nil
}

func (s *Scheduler) IsDue(cronExpr string, lastRun, nextRun time.Time, now time.Time) bool {
	if nextRun.IsZero() {
		return true
	}

	return now.After(nextRun) || now.Equal(nextRun)
}

func (s *Scheduler) GetDueRules(conf *config.Config, st *state.State, now time.Time) ([]config.Rule, error) {
	var dueRules []config.Rule

	for _, rule := range conf.Rules {
		ruleState, exists := st.Rules[rule.Name]

		// Check if rule has no state - create initial state
		if !exists {
			s.lgr.Info("rule has no state, calculating initial next run time", "rule", rule.Name)

			nextRun, err := s.GetNextRunTime(rule.Cron, now)
			if err != nil {
				s.lgr.Error("failed to calculate initial next run time for new rule",
					"rule", rule.Name, "cron", rule.Cron, "err", err)
				continue
			}

			// Create initial state for new rule
			st.Rules[rule.Name] = state.RuleState{
				Name:        rule.Name,
				CronExpr:    rule.Cron,
				LastRunTime: time.Time{}, // Never run before
				NextRunTime: nextRun,
			}

			s.lgr.Info("created initial state for new rule",
				"rule", rule.Name, "next_run", nextRun)
			continue
		}

		if ruleState.CronExpr != rule.Cron {
			s.lgr.Info("cron expression changed, recalculating next run time",
				"rule", rule.Name,
				"old_cron", ruleState.CronExpr,
				"new_cron", rule.Cron)

			// Recalculate next run time based on new cron expression
			nextRun, err := s.GetNextRunTime(rule.Cron, now)
			if err != nil {
				s.lgr.Error("failed to calculate next run time for updated cron",
					"rule", rule.Name, "cron", rule.Cron, "err", err)
				continue
			}

			// Update state with new cron and next run time
			st.Rules[rule.Name] = state.RuleState{
				Name:        rule.Name,
				CronExpr:    rule.Cron,
				LastRunTime: ruleState.LastRunTime, // Keep existing last run time
				NextRunTime: nextRun,
			}

			s.lgr.Info("updated rule state for cron change",
				"rule", rule.Name, "next_run", nextRun)
			continue
		}

		if s.IsDue(rule.Cron, ruleState.LastRunTime, ruleState.NextRunTime, now) {
			s.lgr.Info("rule is due", "rule", rule.Name, "next_run", ruleState.NextRunTime, "now", now)
			dueRules = append(dueRules, rule)
		} else {
			s.lgr.Debug("rule not due", "rule", rule.Name, "next_run", ruleState.NextRunTime, "now", now)
		}
	}

	return dueRules, nil
}

func (s *Scheduler) UpdateRuleState(st *state.State, ruleName, cronExpr string, runTime time.Time) error {
	nextRun, err := s.GetNextRunTime(cronExpr, runTime)
	if err != nil {
		return fmt.Errorf("calculate next run time for rule %s: %w", ruleName, err)
	}

	st.Rules[ruleName] = state.RuleState{
		Name:        ruleName,
		CronExpr:    cronExpr,
		LastRunTime: runTime,
		NextRunTime: nextRun,
	}

	s.lgr.Info("updated rule state", "rule", ruleName, "cron", cronExpr, "last_run", runTime, "next_run", nextRun)
	return nil
}
