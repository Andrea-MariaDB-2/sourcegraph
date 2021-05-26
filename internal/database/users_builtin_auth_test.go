package database

import (
	"context"
	"testing"
	"time"

	"github.com/sourcegraph/sourcegraph/internal/conf"
	"github.com/sourcegraph/sourcegraph/internal/database/dbtest"
	"github.com/sourcegraph/sourcegraph/internal/extsvc"
	"github.com/sourcegraph/sourcegraph/schema"
)

func TestUsers_BuiltinAuth(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	t.Parallel()
	db := dbtest.NewDB(t, "")
	ctx := context.Background()

	if _, err := Users(db).Create(ctx, NewUser{
		Email:       "foo@bar.com",
		Username:    "foo",
		DisplayName: "foo",
		Password:    "asdfasdf",
	}); err == nil {
		t.Fatal("user created without email verification code or admin-verified status")
	}

	usr, err := Users(db).Create(ctx, NewUser{
		Email:                 "foo@bar.com",
		Username:              "foo",
		DisplayName:           "foo",
		Password:              "right-password",
		EmailVerificationCode: "email-code",
	})
	if err != nil {
		t.Fatal(err)
	}
	_, verified, err := UserEmails(db).GetPrimaryEmail(ctx, usr.ID)
	if err != nil {
		t.Fatal(err)
	}
	if verified {
		t.Fatal("new user should not be verified")
	}
	if isValid, err := UserEmails(db).Verify(ctx, usr.ID, "foo@bar.com", "wrong_email-code"); err == nil && isValid {
		t.Fatal("should not validate email with wrong code")
	}
	if isValid, err := UserEmails(db).Verify(ctx, usr.ID, "foo@bar.com", "email-code"); err != nil || !isValid {
		t.Fatal("couldn't vaidate email")
	}
	usr, err = Users(db).GetByID(ctx, usr.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, verified, err := UserEmails(db).GetPrimaryEmail(ctx, usr.ID); err != nil {
		t.Fatal(err)
	} else if !verified {
		t.Fatal("user should not be verified")
	}
	if isPassword, err := Users(db).IsPassword(ctx, usr.ID, "right-password"); err != nil || !isPassword {
		t.Fatal("didn't accept correct password")
	}
	if isPassword, err := Users(db).IsPassword(ctx, usr.ID, "wrong-password"); err == nil && isPassword {
		t.Fatal("accepted wrong password")
	}
	if _, err := Users(db).RenewPasswordResetCode(ctx, 193092309); err == nil {
		t.Fatal("no error renewing password reset for non-existent users")
	}
	resetCode, err := Users(db).RenewPasswordResetCode(ctx, usr.ID)
	if err != nil {
		t.Fatal(err)
	}
	if success, err := Users(db).SetPassword(ctx, usr.ID, "wrong-code", "new-password"); err == nil && success {
		t.Fatal("password updated without right reset code")
	}
	if success, err := Users(db).SetPassword(ctx, usr.ID, "", "new-password"); err == nil && success {
		t.Fatal("password updated without reset code")
	}
	if isPassword, err := Users(db).IsPassword(ctx, usr.ID, "right-password"); err != nil || !isPassword {
		t.Fatal("password changed")
	}
	if success, err := Users(db).SetPassword(ctx, usr.ID, resetCode, "new-password"); err != nil || !success {
		t.Fatalf("failed to update user password with code: %s", err)
	}
	if isPassword, err := Users(db).IsPassword(ctx, usr.ID, "new-password"); err != nil || !isPassword {
		t.Fatalf("new password doesn't work: %s", err)
	}
	if isPassword, err := Users(db).IsPassword(ctx, usr.ID, "right-password"); err == nil && isPassword {
		t.Fatal("old password still works")
	}
}

func TestUsers_BuiltinAuth_VerifiedEmail(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	t.Parallel()
	db := dbtest.NewDB(t, "")
	ctx := context.Background()

	user, err := Users(db).Create(ctx, NewUser{
		Email:           "foo@bar.com",
		Username:        "foo",
		Password:        "asdf",
		EmailIsVerified: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	_, verified, err := UserEmails(db).GetPrimaryEmail(ctx, user.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !verified {
		t.Error("!verified")
	}
}

func TestUsers_BuiltinAuthPasswordResetRateLimit(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	t.Parallel()
	db := dbtest.NewDB(t, "")
	ctx := context.Background()

	oldPasswordResetRateLimit := passwordResetRateLimit
	defer func() {
		passwordResetRateLimit = oldPasswordResetRateLimit
	}()

	passwordResetRateLimit = "24 hours"
	usr, err := Users(db).Create(ctx, NewUser{
		Email:                 "foo@bar.com",
		Username:              "foo",
		DisplayName:           "foo",
		Password:              "right-password",
		EmailVerificationCode: "email-code",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Users(db).RenewPasswordResetCode(ctx, usr.ID); err != nil {
		t.Fatalf("unexpected password reset error: %s", err)
	}
	if _, err := Users(db).RenewPasswordResetCode(ctx, usr.ID); err != ErrPasswordResetRateLimit {
		t.Fatal("expected to hit rate limit")
	}

	passwordResetRateLimit = "0 hours"
	if _, err := Users(db).RenewPasswordResetCode(ctx, usr.ID); err != nil {
		t.Fatalf("unexpected password reset error: %s", err)
	}
	if _, err := Users(db).RenewPasswordResetCode(ctx, usr.ID); err != nil {
		t.Fatalf("unexpected password reset error: %s", err)
	}
}

func TestUsers_UpdatePassword(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	t.Parallel()
	db := dbtest.NewDB(t, "")
	ctx := context.Background()

	usr, err := Users(db).Create(ctx, NewUser{
		Email:                 "foo@bar.com",
		Username:              "foo",
		Password:              "right-password",
		EmailVerificationCode: "c",
	})
	if err != nil {
		t.Fatal(err)
	}

	if isPassword, err := Users(db).IsPassword(ctx, usr.ID, "right-password"); err != nil || !isPassword {
		t.Fatal("didn't accept correct password")
	}
	if isPassword, err := Users(db).IsPassword(ctx, usr.ID, "wrong-password"); err == nil && isPassword {
		t.Fatal("accepted wrong password")
	}
	if err := Users(db).UpdatePassword(ctx, usr.ID, "wrong-password", "new-password"); err == nil {
		t.Fatal("accepted wrong old password")
	}
	if isPassword, err := Users(db).IsPassword(ctx, usr.ID, "right-password"); err != nil || !isPassword {
		t.Fatal("didn't accept correct password")
	}
	if isPassword, err := Users(db).IsPassword(ctx, usr.ID, "wrong-password"); err == nil && isPassword {
		t.Fatal("accepted wrong password")
	}

	if err := Users(db).UpdatePassword(ctx, usr.ID, "right-password", "new-password"); err != nil {
		t.Fatal(err)
	}
	if isPassword, err := Users(db).IsPassword(ctx, usr.ID, "new-password"); err != nil || !isPassword {
		t.Fatal("didn't accept correct password")
	}
	if isPassword, err := Users(db).IsPassword(ctx, usr.ID, "wrong-password"); err == nil && isPassword {
		t.Fatal("accepted wrong password")
	}
	if isPassword, err := Users(db).IsPassword(ctx, usr.ID, "right-password"); err == nil && isPassword {
		t.Fatal("accepted wrong (old) password")
	}
}

func TestUsers_CreatePassword(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	t.Parallel()
	db := dbtest.NewDB(t, "")
	ctx := context.Background()

	// User without a password
	usr1, err := Users(db).Create(ctx, NewUser{
		Email:                 "usr1@bar.com",
		Username:              "usr1",
		Password:              "",
		EmailVerificationCode: "c",
	})
	if err != nil {
		t.Fatal(err)
	}

	// Allowed since the user has no password or external accounts
	if err := Users(db).CreatePassword(ctx, usr1.ID, "the-new-password"); err != nil {
		t.Fatal(err)
	}

	// User with an existing password
	usr2, err := Users(db).Create(ctx, NewUser{
		Email:                 "usr2@bar.com",
		Username:              "usr2",
		Password:              "has-a-password",
		EmailVerificationCode: "c",
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := Users(db).CreatePassword(ctx, usr2.ID, "the-new-password"); err == nil {
		t.Fatal("Should fail, password already exists")
	}

	// A new user with an external account can't create a password
	newID, err := ExternalAccounts(db).CreateUserAndSave(ctx, NewUser{
		Email:                 "usr3@bar.com",
		Username:              "usr3",
		Password:              "",
		EmailVerificationCode: "c",
	},
		extsvc.AccountSpec{
			ServiceType: extsvc.TypeGitHub,
			ServiceID:   "123",
			ClientID:    "456",
			AccountID:   "789",
		},
		extsvc.AccountData{
			AuthData: nil,
			Data:     nil,
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	if err := Users(db).CreatePassword(ctx, newID, "the-new-password"); err == nil {
		t.Fatal("Should fail, user has external account")
	}
}

func TestUsers_PasswordResetExpiry(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	t.Parallel()
	db := dbtest.NewDB(t, "")
	ctx := context.Background()

	user, err := Users(db).Create(ctx, NewUser{
		Email:                 "foo@bar.com",
		Username:              "foo",
		Password:              "right-password",
		EmailVerificationCode: "c",
	})
	if err != nil {
		t.Fatal(err)
	}

	resetCode, err := Users(db).RenewPasswordResetCode(ctx, user.ID)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second) // the lowest expiry is 1 second

	t.Run("expired link", func(t *testing.T) {
		conf.Mock(&conf.Unified{
			SiteConfiguration: schema.SiteConfiguration{
				AuthPasswordResetLinkExpiry: 1,
			},
		})
		defer conf.Mock(nil)

		success, err := Users(db).SetPassword(ctx, user.ID, resetCode, "new-password")
		if err != nil {
			t.Fatal(err)
		}
		if success {
			t.Fatal("accepted an expired password reset")
		}
	})

	t.Run("valid link", func(t *testing.T) {
		conf.Mock(&conf.Unified{
			SiteConfiguration: schema.SiteConfiguration{
				AuthPasswordResetLinkExpiry: 3600,
			},
		})
		defer conf.Mock(nil)

		success, err := Users(db).SetPassword(ctx, user.ID, resetCode, "new-password")
		if err != nil {
			t.Fatal(err)
		}
		if !success {
			t.Fatal("did not accept a valid password reset")
		}
	})
}
