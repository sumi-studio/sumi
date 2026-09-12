package agentevents

import (
	"context"
	"errors"
	"testing"
)

func TestLocalRuntimeRecoveryRequiresExistingExactNonterminalAuthority(t *testing.T) {
	ctx := context.Background()
	_, gateway := openLocalControlTestGateway(t, privateRuntimeDir(t))
	authorization := localControlAuthorization(localControlTestBearer, localControlTestPAID, 7, "boot-a")
	control, _ := newLocalControlHTTPServer(t, gateway, authorization)
	if err := control.RecoverLocalRuntimeAuthorization(ctx, authorization); err == nil {
		t.Fatal("recovery initialized absent durable authority")
	}
	if _, err := control.publishRuntimeState(ctx, startupPublication("startup", localControlTestPAID, 7, "boot-a")); err != nil {
		t.Fatal(err)
	}
	if err := control.RemoveLocalRuntimeAuthorization(localControlTestPAID); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*LocalRuntimeAuthorization){
		func(a *LocalRuntimeAuthorization) { a.Generation++ },
		func(a *LocalRuntimeAuthorization) { a.Generation-- },
		func(a *LocalRuntimeAuthorization) { a.RPCBootNonce = "another-boot" },
		func(a *LocalRuntimeAuthorization) { a.PersonalityAgentID = localControlOtherPAID },
	} {
		wrong := authorization
		mutate(&wrong)
		if err := control.RecoverLocalRuntimeAuthorization(ctx, wrong); err == nil || errors.Is(err, ErrLocalRuntimeEpochTerminal) {
			t.Fatal("mismatched runtime authority was recovered")
		}
	}
	if err := control.RecoverLocalRuntimeAuthorization(ctx, authorization); err != nil {
		t.Fatal(err)
	}
	if err := control.FenceLocalRuntimeAuthorization(ctx, localControlTestPAID, 7, "boot-a"); err != nil {
		t.Fatal(err)
	}
	if err := control.RecoverLocalRuntimeAuthorization(ctx, authorization); !errors.Is(err, ErrLocalRuntimeEpochTerminal) {
		t.Fatal("recovery revived a terminal runtime")
	}
}
