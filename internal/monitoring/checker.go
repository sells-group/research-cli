package monitoring

import (
	"context"
	"time"

	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/config"
)

// Checker runs periodic alert checks in the background.
type Checker struct {
	collector *Collector
	alerter   *Alerter
	cfg       config.MonitoringConfig
}

// NewChecker creates a background alert checker.
func NewChecker(collector *Collector, alerter *Alerter, cfg config.MonitoringConfig) *Checker {
	return &Checker{
		collector: collector,
		alerter:   alerter,
		cfg:       cfg,
	}
}

// Run starts the periodic check loop. It blocks until ctx is cancelled.
func (c *Checker) Run(ctx context.Context) {
	interval := time.Duration(c.cfg.CheckIntervalSecs) * time.Second
	if interval <= 0 {
		interval = 5 * time.Minute
	}

	log := zap.L().With(zap.String("component", "monitoring.checker"))
	log.Info("starting alert checker",
		zap.Duration("interval", interval),
		zap.Int("lookback_hours", c.cfg.LookbackWindowHours),
	)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Info("alert checker stopped")
			return
		case <-ticker.C:
			c.check(ctx, log)
		}
	}
}

func (c *Checker) check(ctx context.Context, log *zap.Logger) {
	snap, err := c.collector.Collect(ctx, c.cfg.LookbackWindowHours)
	if err != nil {
		log.Error("monitoring: failed to collect metrics", zap.Error(err))
		return
	}

	alerts := c.alerter.Evaluate(snap)
	if len(alerts) == 0 {
		log.Debug("monitoring: no alerts triggered")
		return
	}

	sent := c.alerter.SendAlerts(ctx, alerts)
	log.Info("monitoring: alert check complete",
		zap.Int("alerts_triggered", len(alerts)),
		zap.Int("alerts_sent", sent),
	)
}
