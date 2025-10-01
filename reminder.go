package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sesv2"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	appConfig "github.com/psanford/lambda-reminder/config"
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

	// TODO: Load state
	// TODO: Process rules
	// TODO: Update state

	h.lgr.Info("processing complete")
	return nil
}
