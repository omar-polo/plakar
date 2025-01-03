package backup

import (
	"github.com/PlakarKorp/plakar/context"
	"github.com/PlakarKorp/plakar/events"
	"github.com/PlakarKorp/plakar/logger"
	"github.com/charmbracelet/lipgloss"
)

var (
	checkMark = lipgloss.NewStyle().Foreground(lipgloss.Color("#00FF00")).SetString("✓")
	crossMark = lipgloss.NewStyle().Foreground(lipgloss.Color("#FF0000")).SetString("✘")
)

func eventsProcessorStdio(ctx *context.Context, quiet bool) chan struct{} {
	done := make(chan struct{})
	go func() {
		for event := range ctx.Events().Listen() {
			switch event := event.(type) {
			case events.PathError:
				logger.Warn("%x: KO %s %s: %s", event.SnapshotID[:4], crossMark, event.Pathname, event.Message)
			case events.DirectoryOK:
				if !quiet {
					logger.Info("%x: OK %s %s", event.SnapshotID[:4], checkMark, event.Pathname)
				}
			case events.FileOK:
				if !quiet {
					logger.Info("%x: OK %s %s", event.SnapshotID[:4], checkMark, event.Pathname)
				}
			default:
				//logger.Warn("unknown event: %T", event)
			}
		}
		done <- struct{}{}
	}()
	return done
}
