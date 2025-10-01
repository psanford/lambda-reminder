package state

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

type RuleState struct {
	Name        string    `json:"name"`
	CronExpr    string    `json:"cron_expr"`
	LastRunTime time.Time `json:"last_run_time"`
	NextRunTime time.Time `json:"next_run_time"`
}

type State struct {
	Rules map[string]RuleState `json:"rules"`
}

func getStateLocation() (bucket, key string, err error) {
	bucket = os.Getenv("S3_STATE_BUCKET")
	if bucket == "" {
		return "", "", fmt.Errorf("S3_STATE_BUCKET environment variable not set")
	}

	stateDir := os.Getenv("S3_STATE_DIR")
	key = "rules_state.json"
	if stateDir != "" {
		key = fmt.Sprintf("%s/rules_state.json", stateDir)
	}

	return bucket, key, nil
}

func LoadState(ctx context.Context, s3Client *s3.Client, lgr *slog.Logger) (*State, error) {
	bucket, key, err := getStateLocation()
	if err != nil {
		return nil, err
	}

	lgr.Info("loading state", "bucket", bucket, "key", key)

	result, err := s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &bucket,
		Key:    &key,
	})
	if err != nil {
		var apiErr smithy.APIError
		if ok := errors.As(err, &apiErr); ok && apiErr.ErrorCode() == "NoSuchKey" {
			lgr.Info("state file does not exist, starting with empty state")
			return &State{Rules: make(map[string]RuleState)}, nil
		}
		return nil, fmt.Errorf("get state from s3: %w", err)
	}
	defer result.Body.Close()

	var state State
	err = json.NewDecoder(result.Body).Decode(&state)
	if err != nil {
		return nil, fmt.Errorf("decode state: %w", err)
	}

	if state.Rules == nil {
		state.Rules = make(map[string]RuleState)
	}

	lgr.Info("state loaded", "rule_count", len(state.Rules))
	return &state, nil
}

func SaveState(ctx context.Context, s3Client *s3.Client, state *State, lgr *slog.Logger) error {
	bucket, key, err := getStateLocation()
	if err != nil {
		return err
	}

	lgr.Info("saving state", "bucket", bucket, "key", key, "rule_count", len(state.Rules))

	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal state: %w", err)
	}

	_, err = s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: &bucket,
		Key:    &key,
		Body:   bytes.NewReader(data),
	})
	if err != nil {
		return fmt.Errorf("put state to s3: %w", err)
	}

	lgr.Info("state saved successfully")
	return nil
}
