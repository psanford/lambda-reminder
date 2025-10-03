package config

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type Config struct {
	Rules        []Rule        `toml:"rule"`
	Destinations []Destination `toml:"destination"`
	Timezone     string        `toml:"timezone"`
}

type Rule struct {
	Name         string   `toml:"name"`
	Cron         string   `toml:"cron"`
	Destinations []string `toml:"destinations"`
	Subject      string   `toml:"subject"`
	Body         string   `toml:"body"`
}

type Destination struct {
	ID string `toml:"id"`
	// Type is a string of "sns" "slack_webhook" "ses"
	Type string `toml:"type"`

	// SNSARN is for type "sns"
	SNSARN string `toml:"sns_arn"`

	// WebhookURL is for type "slack_webhook"
	WebhookURL string `toml:"webhook_url"`

	// ToEmails is for type "ses"
	ToEmails []string `toml:"to_emails"`
	// FromEmail is for type "ses"
	FromEmail string `toml:"from_email"`
}

func LoadConfig(ctx context.Context, s3Client *s3.Client, lgr *slog.Logger, configPath string) (*Config, error) {
	var conf Config
	if configPath != "" {

		f, err := os.Open(configPath)
		if err != nil {
			return nil, fmt.Errorf("open config file err %w", err)
		}
		defer f.Close()
		_, err = toml.NewDecoder(f).Decode(&conf)
		if err != nil {
			return nil, fmt.Errorf("decode config: %w", err)
		}
	} else {
		bucketName := os.Getenv("S3_CONFIG_BUCKET")
		if bucketName == "" {
			return nil, fmt.Errorf("S3_CONFIG_BUCKET environment variable not set")
		}

		confPath := os.Getenv("S3_CONFIG_PATH")
		if confPath == "" {
			return nil, fmt.Errorf("S3_CONFIG_PATH environment variable not set")
		}

		confResp, err := s3Client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: &bucketName,
			Key:    &confPath,
		})
		if err != nil {
			return nil, fmt.Errorf("get config from s3: %w", err)
		}
		defer confResp.Body.Close()

		_, err = toml.NewDecoder(confResp.Body).Decode(&conf)
		if err != nil {
			return nil, fmt.Errorf("decode config: %w", err)
		}
	}

	err := validateConfig(&conf)
	if err != nil {
		return nil, fmt.Errorf("validate config: %w", err)
	}

	return &conf, nil
}

func validateConfig(conf *Config) error {
	if len(conf.Rules) == 0 {
		return fmt.Errorf("at least one rule must be defined")
	}

	if len(conf.Destinations) == 0 {
		return fmt.Errorf("at least one destination must be defined")
	}

	destMap := make(map[string]bool)
	for _, dest := range conf.Destinations {
		if dest.ID == "" {
			return fmt.Errorf("destination id cannot be empty")
		}
		if destMap[dest.ID] {
			return fmt.Errorf("duplicate destination id: %s", dest.ID)
		}
		destMap[dest.ID] = true

		err := validateDestination(&dest)
		if err != nil {
			return fmt.Errorf("destination %s: %w", dest.ID, err)
		}
	}

	for _, rule := range conf.Rules {
		err := validateRule(&rule, destMap)
		if err != nil {
			return fmt.Errorf("rule %s: %w", rule.Name, err)
		}
	}

	if conf.Timezone != "" {
		_, err := time.LoadLocation(conf.Timezone)
		if err != nil {
			return fmt.Errorf("invalid timezone: %w", err)
		}
	}

	return nil
}

func validateDestination(dest *Destination) error {
	switch dest.Type {
	case "sns":
		if dest.SNSARN == "" {
			return fmt.Errorf("sns_arn is required for sns destination")
		}
	case "slack_webhook":
		if dest.WebhookURL == "" {
			return fmt.Errorf("webhook_url is required for slack_webhook destination")
		}
	case "ses":
		if dest.FromEmail == "" {
			return fmt.Errorf("from_email is required for ses destination")
		}
		if len(dest.ToEmails) == 0 {
			return fmt.Errorf("to_emails is required for ses destination")
		}
	case "log":
		return nil
	default:
		return fmt.Errorf("unsupported destination type: %s", dest.Type)
	}
	return nil
}

func validateRule(rule *Rule, destMap map[string]bool) error {
	if rule.Name == "" {
		return fmt.Errorf("rule name cannot be empty")
	}
	if rule.Cron == "" {
		return fmt.Errorf("cron expression cannot be empty")
	}
	if len(rule.Destinations) == 0 {
		return fmt.Errorf("at least one destination must be specified")
	}
	if rule.Subject == "" {
		return fmt.Errorf("subject cannot be empty")
	}
	if rule.Body == "" {
		return fmt.Errorf("body cannot be empty")
	}

	for _, destID := range rule.Destinations {
		if !destMap[destID] {
			return fmt.Errorf("destination %s not found", destID)
		}
	}

	return nil
}
