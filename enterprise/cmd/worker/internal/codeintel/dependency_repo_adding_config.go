package codeintel

import (
	"time"

	"github.com/sourcegraph/sourcegraph/internal/env"
)

type dependencyRepoAddingConfig struct {
	env.BaseConfig

	DependencyRepoAddingPollInterval time.Duration
	DependencyRepoAddingConcurrency  int
}

var dependencyRepoAddingConfigInst = &dependencyRepoAddingConfig{}

func (c *dependencyRepoAddingConfig) Load() {
	c.DependencyRepoAddingPollInterval = c.GetInterval("PRECISE_CODE_INTEL_DEPENDENCY_REPO_ADDING_POLL_INTERVAL", "1s", "Interval between queries to the dependency indexing job queue.")
	c.DependencyRepoAddingConcurrency = c.GetInt("PRECISE_CODE_INTEL_DEPENDENCY_REPO_ADDING_CONCURRENCY", "1", "The maximum number of dependency graphs that can be processed concurrently.")
}
