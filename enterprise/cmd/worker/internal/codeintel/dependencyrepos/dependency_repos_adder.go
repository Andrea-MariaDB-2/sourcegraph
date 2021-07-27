package dependencyrepos

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/hashicorp/go-multierror"

	"github.com/sourcegraph/sourcegraph/enterprise/internal/codeintel/stores/dbstore"
	"github.com/sourcegraph/sourcegraph/internal/actor"
	"github.com/sourcegraph/sourcegraph/internal/database"
	"github.com/sourcegraph/sourcegraph/internal/workerutil"
	"github.com/sourcegraph/sourcegraph/internal/workerutil/dbworker"
	dbworkerstore "github.com/sourcegraph/sourcegraph/internal/workerutil/dbworker/store"
)

func NewDependencyRepoAdder(
	dbStore DBStore,
	workerStore dbworkerstore.Store,
	pollInterval time.Duration,
	numProcessorRoutines int,
	workerMetrics workerutil.WorkerMetrics,
) *workerutil.Worker {
	rootContext := actor.WithActor(context.Background(), &actor.Actor{Internal: true})

	handler := &dependencyRepoAddingHandler{
		dbStore: dbStore,
	}

	return dbworker.NewWorker(rootContext, workerStore, handler, workerutil.WorkerOptions{
		Name:              "precise_code_intel_dependency_repo_adding_worker",
		NumHandlers:       numProcessorRoutines,
		Interval:          pollInterval,
		Metrics:           workerMetrics,
		HeartbeatInterval: 1 * time.Second,
	})
}

type dependencyRepoAddingHandler struct {
	dbStore DBStore
}

var _ workerutil.Handler = &dependencyRepoAddingHandler{}

type DependencyRepoInfo struct {
	Identifier, Version, Scheme string
}

func (h *dependencyRepoAddingHandler) Handle(ctx context.Context, record workerutil.Record) error {
	job := record.(dbstore.DependencyRepoAddingJob)

	scanner, err := h.dbStore.ReferencesForUpload(ctx, job.UploadID)
	if err != nil {
		return errors.Wrap(err, "dbstore.ReferencesForUpload")
	}
	defer func() {
		if closeErr := scanner.Close(); closeErr != nil {
			err = multierror.Append(err, errors.Wrap(closeErr, "dbstore.ReferenceIDsAndFilters.Close"))
		}
	}()

	var (
		dependencies []dbstore.DependencyRepoInfo
		kinds        []string
		errs         []error
	)

	for {
		packageReference, exists, err := scanner.Next()
		if err != nil {
			return errors.Wrap(err, "dbstore.ReferencesForUpload.Next")
		}
		if !exists {
			break
		}

		transformer, ok := transformerForScheme[packageReference.Scheme]
		if !ok {
			return &ReferenceSchemeError{Scheme: packageReference.Scheme}
		}

		result, kind, err := transformer(packageReference.Package)
		if err != nil {
			errs = append(errs, errors.Wrap(err, fmt.Sprintf("dependencyrepo.PackageInformationParser: failed to parse package information data %#v", packageReference)))
			continue
		}

		if !kindExists(kinds, kind) {
			kinds = append(kinds, kind)
		}

		dependencies = append(dependencies, dbstore.DependencyRepoInfo{
			Identifier: result.Identifier,
			Version:    result.Version,
			Scheme:     result.Scheme,
		})
	}

	if len(errs) == 1 {
		return errs[0]
	}

	if err = h.dbStore.InsertCloneableDependencyRepos(ctx, dependencies); err != nil {
		return errors.Wrap(err, "dbstore.InsertCloneableDependencyRepos")
	}

	externalServices, err := h.dbStore.ListExternalServices(ctx, database.ExternalServicesListOptions{
		Kinds: kinds,
	})
	if err != nil {
		return errors.Wrap(err, "dbstore.List")
	}

	for _, externalService := range externalServices {
		externalService.NextSyncAt = time.Now()
		if err := h.dbStore.Upsert(ctx, externalService); err != nil {
			return errors.Wrapf(err, "dbstore.Upsert: error setting next_sync_at for external service %d - %s", externalService.ID, externalService.DisplayName)
		}
	}

	return err
}

func kindExists(kinds []string, kind string) bool {
	for _, k := range kinds {
		if k == kind {
			return true
		}
	}
	return false
}

type ReferenceSchemeError struct {
	Scheme string
}

func (e *ReferenceSchemeError) Error() string {
	return fmt.Sprintf("no parser found for schema %s", e.Scheme)
}
