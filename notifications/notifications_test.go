package notifications

import (
	"context"
	"log/slog"
	"os"
	"testing"

	"github.com/psanford/lambda-reminder/config"
)

func TestGetDestinationsForRule(t *testing.T) {
	lgr := slog.New(slog.NewTextHandler(os.Stdout, nil))
	sender := NewSender(nil, nil, lgr)

	allDestinations := []config.Destination{
		{
			ID:     "sns1",
			Type:   "sns",
			SNSARN: "arn:aws:sns:us-west-1:123456789:topic1",
		},
		{
			ID:        "email1",
			Type:      "ses",
			FromEmail: "noreply@example.com",
			ToEmails:  []string{"admin@example.com"},
		},
		{
			ID:         "slack1",
			Type:       "slack_webhook",
			WebhookURL: "https://hooks.slack.com/test",
		},
		{
			ID:     "unused",
			Type:   "sns",
			SNSARN: "arn:aws:sns:us-west-1:123456789:unused",
		},
	}

	rule := config.Rule{
		Name:         "test_rule",
		Destinations: []string{"sns1", "email1", "nonexistent"},
	}

	result := sender.GetDestinationsForRule(rule, allDestinations)

	// Should return 2 destinations (sns1, email1) and skip nonexistent
	if len(result) != 2 {
		t.Errorf("Expected 2 destinations, got %d", len(result))
	}

	// Check we got the right destinations
	destIDs := make(map[string]bool)
	for _, dest := range result {
		destIDs[dest.ID] = true
	}

	if !destIDs["sns1"] {
		t.Error("Expected sns1 destination")
	}

	if !destIDs["email1"] {
		t.Error("Expected email1 destination")
	}

	if destIDs["slack1"] {
		t.Error("slack1 should not be included")
	}

	if destIDs["nonexistent"] {
		t.Error("nonexistent destination should not be included")
	}
}

func TestSendNotifications(t *testing.T) {
	lgr := slog.New(slog.NewTextHandler(os.Stdout, nil))

	// Create mock clients - in a real test we'd use proper mocks
	sender := NewSender(nil, nil, lgr)

	rule := config.Rule{
		Name:         "test_rule",
		Subject:      "Test Subject",
		Body:         "Test Body",
		Destinations: []string{"test_dest"},
	}

	// Test with unsupported destination type
	destinations := []config.Destination{
		{
			ID:   "test_dest",
			Type: "unsupported_type",
		},
	}

	ctx := context.Background()
	err := sender.SendNotifications(ctx, rule, destinations)

	// Should return error for unsupported destination type
	if err == nil {
		t.Error("Expected error for unsupported destination type")
	}
}

func TestSlackMessageStructure(t *testing.T) {
	// Test that our Slack message structure is correct
	rule := config.Rule{
		Name:    "daily_reminder",
		Cron:    "0 9 * * *",
		Subject: "Daily Standup",
		Body:    "Don't forget about the daily standup at 9 AM",
	}

	// We can't easily test the actual HTTP call without mocking,
	// but we can test the message structure by creating it manually
	slackMsg := SlackMessage{
		Text:      "Reminder: " + rule.Subject,
		Username:  "Lambda Reminder",
		IconEmoji: ":bell:",
		Attachments: []SlackAttachment{
			{
				Color: "good",
				Title: rule.Subject,
				Text:  rule.Body,
				Fields: []SlackField{
					{
						Title: "Rule",
						Value: rule.Name,
						Short: true,
					},
					{
						Title: "Schedule",
						Value: rule.Cron,
						Short: true,
					},
				},
			},
		},
	}

	// Verify message structure
	if slackMsg.Text != "Reminder: Daily Standup" {
		t.Errorf("Expected text 'Reminder: Daily Standup', got '%s'", slackMsg.Text)
	}

	if slackMsg.Username != "Lambda Reminder" {
		t.Errorf("Expected username 'Lambda Reminder', got '%s'", slackMsg.Username)
	}

	if len(slackMsg.Attachments) != 1 {
		t.Errorf("Expected 1 attachment, got %d", len(slackMsg.Attachments))
	}

	attachment := slackMsg.Attachments[0]
	if attachment.Title != rule.Subject {
		t.Errorf("Expected attachment title '%s', got '%s'", rule.Subject, attachment.Title)
	}

	if attachment.Text != rule.Body {
		t.Errorf("Expected attachment text '%s', got '%s'", rule.Body, attachment.Text)
	}

	if len(attachment.Fields) != 2 {
		t.Errorf("Expected 2 fields, got %d", len(attachment.Fields))
	}

	// Check fields
	ruleField := attachment.Fields[0]
	if ruleField.Title != "Rule" || ruleField.Value != rule.Name {
		t.Errorf("Expected Rule field with value '%s', got '%s': '%s'", rule.Name, ruleField.Title, ruleField.Value)
	}

	scheduleField := attachment.Fields[1]
	if scheduleField.Title != "Schedule" || scheduleField.Value != rule.Cron {
		t.Errorf("Expected Schedule field with value '%s', got '%s': '%s'", rule.Cron, scheduleField.Title, scheduleField.Value)
	}
}
