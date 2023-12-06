package config

// Logging configures overall and logger-specific log levels. Level values are
// case-insensitive and include the following in order of importance: "fatal",
// "panic", "dpanic", ""error", "warn", "info", "debug"
type Logging struct {
	// Level sets the log level for all loggers that do not have a setting in
	// Loggers. The default value is "info".
	Level string
	// Loggers sets log levels for individual loggers. Example:
	//    Loggers: map[string]string{"p2p-config": "warn"}
	Loggers map[string]string
}

// NewLogging returns Logging with values set to their defaults.
func NewLogging() Logging {
	return Logging{
		Level: "info",
	}
}

// populateUnset replaces zero-values in the config with default values.
func (c *Logging) populateUnset() {
	def := NewLogging()

	if c.Level == "" {
		c.Level = def.Level
	}
}
