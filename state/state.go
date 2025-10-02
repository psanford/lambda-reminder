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

func LoadState(ctx context.Context, s3Client *s3.Client, lgr *slog.Logger, localStatePath string) (*State, error) {
	var state State

	if localStatePath != "" {
		f, err := os.Open(localStatePath)

		if errors.Is(err, os.ErrNotExist) {
			lgr.Info("state file does not exist, starting with empty state")
			return &State{Rules: make(map[string]RuleState)}, nil
		} else if err != nil {
			return nil, fmt.Errorf("load local state file err %w", err)
		}

		defer f.Close()

		err = json.NewDecoder(f).Decode(&state)
		if err != nil {
			return nil, fmt.Errorf("decode state: %w", err)
		}
	} else {
		bucket, key, err := getStateLocation()
		if err != nil {
			return nil, err
		}

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

		err = json.NewDecoder(result.Body).Decode(&state)
		if err != nil {
			return nil, fmt.Errorf("decode state: %w", err)
		}
	}

	if state.Rules == nil {
		state.Rules = make(map[string]RuleState)
	}

	return &state, nil
}

func SaveState(ctx context.Context, s3Client *s3.Client, state *State, lgr *slog.Logger, localStatePath string) error {
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal state: %w", err)
	}

	if localStatePath != "" {
		err := os.WriteFile(localStatePath, data, 0600)
		if err != nil {
			return fmt.Errorf("create local state file: %w", err)
		}
	} else {

		bucket, key, err := getStateLocation()
		if err != nil {
			return err
		}

		_, err = s3Client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: &bucket,
			Key:    &key,
			Body:   bytes.NewReader(data),
		})
		if err != nil {
			return fmt.Errorf("put state to s3: %w", err)
		}
	}

	return nil
}
