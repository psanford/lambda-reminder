package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sesv2"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	appConfig "github.com/psanford/lambda-reminder/config"
	"github.com/psanford/lambda-reminder/scheduler"
	"github.com/psanford/lambda-reminder/state"
)

type handler struct {
	s3Client  *s3.Client
	snsClient *sns.Client
	sesClient *sesv2.Client
	lgr       *slog.Logger
}

func main() {
	lgr := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	slog.SetDefault(lgr)

	lgr.Info("starting lambda function", "function", "reminder")

	ctx := context.Background()
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		panic(fmt.Sprintf("load aws config: %s", err))
	}

	h := &handler{
		s3Client:  s3.NewFromConfig(cfg),
		snsClient: sns.NewFromConfig(cfg),
		sesClient: sesv2.NewFromConfig(cfg),
		lgr:       lgr,
	}

	lambda.Start(h.Handler)
}

func (h *handler) Handler(ctx context.Context, evt events.CloudWatchEvent) error {
	h.lgr.Info("processing scheduled event", "source", evt.Source)

	conf, err := appConfig.LoadConfig(ctx, h.s3Client, h.lgr)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	h.lgr.Info("config loaded", "rules", len(conf.Rules), "destinations", len(conf.Destinations))

	st, err := state.LoadState(ctx, h.s3Client, h.lgr)
	if err != nil {
		return fmt.Errorf("load state: %w", err)
	}

	sched := scheduler.New(h.lgr)
	now := time.Now()

	dueRules, err := sched.GetDueRules(conf, st, now)
	if err != nil {
		return fmt.Errorf("get due rules: %w", err)
	}

	h.lgr.Info("due rules found", "count", len(dueRules))

	for _, rule := range dueRules {
		h.lgr.Info("processing rule", "rule", rule.Name, "cron", rule.Cron)

		// TODO: Send notifications to destinations

		err = sched.UpdateRuleState(st, rule.Name, rule.Cron, now)
		if err != nil {
			h.lgr.Error("update rule state error", "rule", rule.Name, "err", err)
			continue
		}
	}

	err = state.SaveState(ctx, h.s3Client, st, h.lgr)
	if err != nil {
		return fmt.Errorf("save state: %w", err)
	}

	h.lgr.Info("processing complete", "processed_rules", len(dueRules))
	return nil
}
