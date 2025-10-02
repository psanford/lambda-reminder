package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sesv2"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/psanford/lambda-reminder/config"
	"github.com/psanford/lambda-reminder/notifications"
	"github.com/psanford/lambda-reminder/scheduler"
	"github.com/psanford/lambda-reminder/state"
)

var mode = flag.String("mode", "lambda", "Run mode (lambda|local)")
var configPath = flag.String("config", "", "Local config path, blank means load from s3")
var statePath = flag.String("state_path", "", "Local state path, blank means load from s3")

func main() {
	flag.Parse()

	lgr := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	slog.SetDefault(lgr)

	lgr.Info("starting")

	ctx := context.Background()
	cfg, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		panic(fmt.Sprintf("load aws config: %s", err))
	}

	h := &handler{
		s3Client:  s3.NewFromConfig(cfg),
		snsClient: sns.NewFromConfig(cfg),
		sesClient: sesv2.NewFromConfig(cfg),
		lgr:       lgr,
	}

	if *mode == "local" {
		h.localRunLoop(ctx)
	} else {
		lambda.Start(h.Handler)
	}
}

type handler struct {
	s3Client  *s3.Client
	snsClient *sns.Client
	sesClient *sesv2.Client
	lgr       *slog.Logger
}

func (h *handler) Handler(ctx context.Context, evt events.CloudWatchEvent) error {
	h.lgr.Info("processing scheduled event")

	conf, err := config.LoadConfig(ctx, h.s3Client, h.lgr, *configPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	h.lgr.Info("config loaded", "rules", len(conf.Rules), "destinations", len(conf.Destinations))

	st, err := state.LoadState(ctx, h.s3Client, h.lgr, *statePath)
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

	notificationSender := notifications.NewSender(h.snsClient, h.sesClient, h.lgr)

	var errs []error

	for _, rule := range dueRules {
		h.lgr.Info("processing rule", "rule", rule.Name, "cron", rule.Cron)

		// Get destinations for this rule
		ruleDestinations := notificationSender.GetDestinationsForRule(rule, conf.Destinations)

		// Send notifications
		err = notificationSender.SendNotifications(ctx, rule, ruleDestinations)
		if err != nil {
			h.lgr.Error("send notifications error", "rule", rule.Name, "err", err)
			errs = append(errs, err)
			continue
		}

		err = sched.UpdateRuleState(st, rule.Name, rule.Cron, now)
		if err != nil {
			h.lgr.Error("update rule state error", "rule", rule.Name, "err", err)
			errs = append(errs, err)
			continue
		}
	}

	err = state.SaveState(ctx, h.s3Client, st, h.lgr, *statePath)
	if err != nil {
		return fmt.Errorf("save state: %w", err)
	}

	h.lgr.Info("processing complete", "processed_rules", len(dueRules))

	if len(errs) > 0 {
		h.lgr.Info("processing errors", "errs", errs)
		return fmt.Errorf("processing errors %+v", errs)

	}

	return nil
}

func (h *handler) localRunLoop(ctx context.Context) {
	t := time.NewTicker(1 * time.Minute)

	for {
		err := h.Handler(ctx, events.CloudWatchEvent{})
		if err != nil {
			h.lgr.Error("handler error", "err", err)
			os.Exit(1)
		}

		<-t.C
	}
}
