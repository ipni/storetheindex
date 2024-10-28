package config

// Logging configures overall and logger-specific log levels. Level values are
// case-insensitive and include the following in order of importance: "fatal",
// "panic", "dpanic", ""error", "warn", "info", "debug"
type Logging struct {
	// Level sets the log level for all loggers that do not have a setting in
	// Loggers. The default value is "info".
	Level string
	// Loggers sets log levels for individual loggers.
	Loggers map[string]string
}

// NewLogging returns Logging with values set to their defaults.
func NewLogging() Logging {
	return Logging{
		Level: "info",
		Loggers: map[string]string{
			"basichost": "warn",
			"bootstrap": "warn",
		},
	}
}

// populateUnset replaces zero-values in the config with default values.
func (c *Logging) populateUnset() {
	def := NewLogging()

	if c.Level == "" {
		c.Level = def.Level

		// If no overall logging level was set, and no level set for loggers,
		// then set levels for certain loggers.
		if len(c.Loggers) == 0 {
			c.Loggers = def.Loggers
		}
	}
}
