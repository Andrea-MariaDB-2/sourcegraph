package graphqlbackend

import (
	"context"

	"github.com/graph-gophers/graphql-go"
	"github.com/graph-gophers/graphql-go/relay"

	"github.com/sourcegraph/sourcegraph/cmd/frontend/backend"
	"github.com/sourcegraph/sourcegraph/internal/actor"
	"github.com/sourcegraph/sourcegraph/internal/database"
	"github.com/sourcegraph/sourcegraph/internal/database/dbutil"
	"github.com/sourcegraph/sourcegraph/internal/extsvc"
)

type externalAccountResolver struct {
	db      dbutil.DB
	account extsvc.Account
}

func externalAccountByID(ctx context.Context, db dbutil.DB, id graphql.ID) (*externalAccountResolver, error) {
	externalAccountID, err := unmarshalExternalAccountID(id)
	if err != nil {
		return nil, err
	}
	account, err := database.ExternalAccounts(db).Get(ctx, externalAccountID)
	if err != nil {
		return nil, err
	}

	// 🚨 SECURITY: Only the user and site admins should be able to see a user's external accounts.
	if err := backend.CheckSiteAdminOrSameUser(ctx, db, account.UserID); err != nil {
		return nil, err
	}

	return &externalAccountResolver{db: db, account: *account}, nil
}

func marshalExternalAccountID(repo int32) graphql.ID { return relay.MarshalID("ExternalAccount", repo) }

func unmarshalExternalAccountID(id graphql.ID) (externalAccountID int32, err error) {
	err = relay.UnmarshalSpec(id, &externalAccountID)
	return
}

func (r *externalAccountResolver) ID() graphql.ID { return marshalExternalAccountID(r.account.ID) }
func (r *externalAccountResolver) User(ctx context.Context) (*UserResolver, error) {
	return UserByIDInt32(ctx, r.db, r.account.UserID)
}
func (r *externalAccountResolver) ServiceType() string { return r.account.ServiceType }
func (r *externalAccountResolver) ServiceID() string   { return r.account.ServiceID }
func (r *externalAccountResolver) ClientID() string    { return r.account.ClientID }
func (r *externalAccountResolver) AccountID() string   { return r.account.AccountID }
func (r *externalAccountResolver) CreatedAt() DateTime { return DateTime{Time: r.account.CreatedAt} }
func (r *externalAccountResolver) UpdatedAt() DateTime { return DateTime{Time: r.account.UpdatedAt} }

func (r *externalAccountResolver) RefreshURL() *string {
	// TODO(sqs): Not supported.
	return nil
}

func (r *externalAccountResolver) AccountData(ctx context.Context) (*JSONValue, error) {
	// 🚨 SECURITY: It is only safe to assume account data of GitHub and GitLab do
	// not contain sensitive information that is not known to the user (which is
	// accessible via APIs by users themselves). We cannot take the same assumption
	// for other types of external accounts.
	//
	// Therefore, the site admins and the user can view account data of GitHub and
	// GitLab, but only site admins can view account data for all other types.
	var err error
	if r.account.ServiceType == extsvc.TypeGitHub || r.account.ServiceType == extsvc.TypeGitLab {
		err = backend.CheckSiteAdminOrSameUser(ctx, r.db, actor.FromContext(ctx).UID)
	} else {
		err = backend.CheckUserIsSiteAdmin(ctx, r.db, actor.FromContext(ctx).UID)
	}
	if err != nil {
		return nil, err
	}

	if r.account.Data != nil {
		return &JSONValue{r.account.Data}, nil
	}
	return nil, nil
}
