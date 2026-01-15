package streams

type Config struct {
	ApplicationID    string
	BootstrapServers []string
	Logger           Logger
}

type ConfigOption func(*Config)

func WithApplicationID(id string) ConfigOption {
	return func(c *Config) {
		c.ApplicationID = id
	}
}

func WithBootstrapServers(servers []string) ConfigOption {
	return func(c *Config) {
		c.BootstrapServers = servers
	}
}

func WithLogger(logger Logger) ConfigOption {
	return func(c *Config) {
		c.Logger = logger
	}
}

func defaultConfig() Config {
	return Config{
		ApplicationID:    "go-streams-app",
		BootstrapServers: []string{"localhost:9092"},
		Logger:           &noopLogger{},
	}
}
