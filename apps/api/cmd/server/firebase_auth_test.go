package main

import (
	"context"
	"errors"
	"reflect"
	"testing"

	firebaseauth "firebase.google.com/go/v4/auth"
)

type fakeFirebaseProviderUserClient struct {
	user        *firebaseauth.UserRecord
	updateErr   error
	updateCalls int
}

func (f *fakeFirebaseProviderUserClient) GetUser(context.Context, string) (*firebaseauth.UserRecord, error) {
	return f.user, nil
}

func (f *fakeFirebaseProviderUserClient) UpdateUser(context.Context, string, *firebaseauth.UserToUpdate) (*firebaseauth.UserRecord, error) {
	f.updateCalls++
	return f.user, f.updateErr
}

func TestFirebaseProviderAccountUsesOnlySupportedLiveProviderRecords(t *testing.T) {
	user := &firebaseauth.UserRecord{
		UserInfo: &firebaseauth.UserInfo{UID: "firebase-user", ProviderID: "firebase", Email: "profile@example.com"},
		ProviderUserInfo: []*firebaseauth.UserInfo{
			{ProviderID: "password", UID: "profile@example.com"},
			{ProviderID: "google.com", UID: "google-subject"},
			{ProviderID: "github.com", UID: "github-subject"},
			{ProviderID: "phone", UID: "+15555550100"},
			{ProviderID: "facebook.com", UID: "facebook-subject"},
		},
	}
	account, err := firebaseProviderAccountFromUser(user, "firebase-user")
	if err != nil {
		t.Fatal(err)
	}
	if !account.EmailProvider || account.UID != "firebase-user" || !reflect.DeepEqual(account.ProviderSubjects, map[string]string{
		"google.com": "google-subject", "github.com": "github-subject",
	}) {
		t.Fatalf("provider account: %+v", account)
	}

	profileOnly, err := firebaseProviderAccountFromUser(&firebaseauth.UserRecord{
		UserInfo: &firebaseauth.UserInfo{UID: "profile-only", ProviderID: "firebase", Email: "profile@example.com"},
	}, "profile-only")
	if err != nil || profileOnly.EmailProvider || len(profileOnly.ProviderSubjects) != 0 {
		t.Fatalf("profile email counted as provider: %+v %v", profileOnly, err)
	}
}

func TestFirebaseProviderAccountFailsClosedOnIdentityAmbiguity(t *testing.T) {
	if _, err := firebaseProviderAccountFromUser(nil, "uid"); err == nil {
		t.Fatal("nil Firebase record accepted")
	}
	if _, err := firebaseProviderAccountFromUser(&firebaseauth.UserRecord{
		UserInfo: &firebaseauth.UserInfo{UID: "other"},
	}, "uid"); err == nil {
		t.Fatal("mismatched Firebase UID accepted")
	}
	if _, err := firebaseProviderAccountFromUser(&firebaseauth.UserRecord{
		UserInfo: &firebaseauth.UserInfo{UID: "uid"},
		ProviderUserInfo: []*firebaseauth.UserInfo{
			{ProviderID: "github.com", UID: "first"},
			{ProviderID: "github.com", UID: "second"},
		},
	}, "uid"); err == nil {
		t.Fatal("ambiguous provider subjects accepted")
	}
}

func TestFirebaseAdminProviderLifecycleRestrictsBackendMutation(t *testing.T) {
	client := &fakeFirebaseProviderUserClient{user: &firebaseauth.UserRecord{UserInfo: &firebaseauth.UserInfo{UID: "uid"}}}
	lifecycle := &firebaseAdminProviderLifecycle{client: client}
	if err := lifecycle.DeleteProvider(context.Background(), "uid", "facebook.com"); err == nil || client.updateCalls != 0 {
		t.Fatalf("unsupported provider mutation: err=%v calls=%d", err, client.updateCalls)
	}
	client.updateErr = errors.New("admin failure")
	if err := lifecycle.DeleteProvider(context.Background(), "uid", "github.com"); !errors.Is(err, client.updateErr) || client.updateCalls != 1 {
		t.Fatalf("supported provider mutation: err=%v calls=%d", err, client.updateCalls)
	}
}
