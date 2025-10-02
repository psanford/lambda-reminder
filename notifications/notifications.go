package notifications

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/aws/aws-sdk-go-v2/service/sesv2"
	"github.com/aws/aws-sdk-go-v2/service/sesv2/types"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/psanford/lambda-reminder/config"
)

type NotificationSender struct {
	snsClient *sns.Client
	sesClient *sesv2.Client
	lgr       *slog.Logger
}

func NewSender(snsClient *sns.Client, sesClient *sesv2.Client, lgr *slog.Logger) *NotificationSender {
	return &NotificationSender{
		snsClient: snsClient,
		sesClient: sesClient,
		lgr:       lgr,
	}
}

func (n *NotificationSender) SendNotifications(ctx context.Context, rule config.Rule, destinations []config.Destination) error {
	var errors []error

	for _, dest := range destinations {
		n.lgr.Info("sending notification", "rule", rule.Name, "destination", dest.ID, "type", dest.Type)

		var err error
		switch dest.Type {
		case "sns":
			err = n.sendSNS(ctx, rule, dest)
		case "ses":
			err = n.sendSES(ctx, rule, dest)
		case "slack_webhook":
			err = n.sendSlackWebhook(ctx, rule, dest)
		case "log":
			n.lgr.Info("log notification event", "subject", rule.Subject, "body", rule.Body)
		default:
			err = fmt.Errorf("unsupported destination type: %s", dest.Type)
		}

		if err != nil {
			n.lgr.Error("failed to send notification",
				"rule", rule.Name,
				"destination", dest.ID,
				"type", dest.Type,
				"err", err)
			errors = append(errors, fmt.Errorf("destination %s: %w", dest.ID, err))
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("failed to send %d/%d notifications: %v", len(errors), len(destinations), errors)
	}

	return nil
}

func (n *NotificationSender) sendSNS(ctx context.Context, rule config.Rule, dest config.Destination) error {
	message := fmt.Sprintf("Reminder: %s\n\n%s", rule.Subject, rule.Body)

	_, err := n.snsClient.Publish(ctx, &sns.PublishInput{
		TopicArn: &dest.SNSARN,
		Message:  &message,
		Subject:  &rule.Subject,
	})
	if err != nil {
		return fmt.Errorf("publish to SNS: %w", err)
	}

	return nil
}

func (n *NotificationSender) sendSES(ctx context.Context, rule config.Rule, dest config.Destination) error {
	emailBody := fmt.Sprintf(`
<html>
<head><title>%s</title></head>
<body>
<h2>%s</h2>
<p>%s</p>
</body>
</html>`, rule.Subject, rule.Subject, rule.Body)

	_, err := n.sesClient.SendEmail(ctx, &sesv2.SendEmailInput{
		FromEmailAddress: &dest.FromEmail,
		Destination: &types.Destination{
			ToAddresses: dest.ToEmails,
		},
		Content: &types.EmailContent{
			Simple: &types.Message{
				Subject: &types.Content{
					Data: &rule.Subject,
				},
				Body: &types.Body{
					Html: &types.Content{
						Data: &emailBody,
					},
					Text: &types.Content{
						Data: &rule.Body,
					},
				},
			},
		},
	})
	if err != nil {
		return fmt.Errorf("send email via SES: %w", err)
	}

	return nil
}

type SlackMessage struct {
	Text        string            `json:"text"`
	Username    string            `json:"username,omitempty"`
	IconEmoji   string            `json:"icon_emoji,omitempty"`
	Attachments []SlackAttachment `json:"attachments,omitempty"`
}

type SlackAttachment struct {
	Color  string       `json:"color,omitempty"`
	Title  string       `json:"title,omitempty"`
	Text   string       `json:"text,omitempty"`
	Fields []SlackField `json:"fields,omitempty"`
}

type SlackField struct {
	Title string `json:"title"`
	Value string `json:"value"`
	Short bool   `json:"short"`
}

func (n *NotificationSender) sendSlackWebhook(ctx context.Context, rule config.Rule, dest config.Destination) error {
	slackMsg := SlackMessage{
		Text:      fmt.Sprintf("Reminder: %s", rule.Subject),
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

	msgBytes, err := json.Marshal(slackMsg)
	if err != nil {
		return fmt.Errorf("marshal slack message: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", dest.WebhookURL, bytes.NewReader(msgBytes))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("send webhook request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("webhook returned status %d", resp.StatusCode)
	}

	return nil
}

func (n *NotificationSender) GetDestinationsForRule(rule config.Rule, allDestinations []config.Destination) []config.Destination {
	var ruleDestinations []config.Destination

	destMap := make(map[string]config.Destination)
	for _, dest := range allDestinations {
		destMap[dest.ID] = dest
	}

	for _, destID := range rule.Destinations {
		if dest, exists := destMap[destID]; exists {
			ruleDestinations = append(ruleDestinations, dest)
		} else {
			n.lgr.Warn("destination not found for rule", "rule", rule.Name, "destination_id", destID)
		}
	}

	return ruleDestinations
}
