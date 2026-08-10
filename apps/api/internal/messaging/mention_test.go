package messaging

import (
	"testing"
)

const (
	testHumanID  = "018f3f8d-7b2c-7a10-8f9e-00000000ab01"
	testHuman2ID = "018f3f8d-7b2c-7a10-8f9e-00000000ab02"
	testAgentID  = "018f3f8d-7b2c-7a10-8f9e-00000000aa01"
	testAgent2ID = "018f3f8d-7b2c-7a10-8f9e-00000000aa02"
)

func member(ref ParticipantRef, name string) MemberProfile {
	return MemberProfile{Participant: ref, DisplayName: name}
}

func keys(refs []ParticipantRef) []string {
	out := make([]string, len(refs))
	for i, r := range refs {
		out[i] = r.Key()
	}
	return out
}

func TestResolveMentionsBindsActiveMembersOnly(t *testing.T) {
	members := []MemberProfile{
		member(Human(testHumanID), "Yohaku"),
		member(PersonalityAgent(testAgentID), "Kuro"),
	}
	got := resolveMentions("@Kuro おはよう。@Unknown は無視、@Yohaku!", members)
	want := []string{"personality_agent:" + testAgentID, "human:" + testHumanID}
	if len(got) != 2 || got[0].Key() != want[0] || got[1].Key() != want[1] {
		t.Fatalf("resolved %v, want %v", keys(got), want)
	}
}

func TestResolveMentionsRequiresBoundaryAfterName(t *testing.T) {
	members := []MemberProfile{member(PersonalityAgent(testAgentID), "Kuro")}
	if got := resolveMentions("@Kuroda さん", members); len(got) != 0 {
		t.Fatalf("@Kuroda must not bind Kuro, resolved %v", keys(got))
	}
	// End of content is a boundary.
	if got := resolveMentions("よろしく @Kuro", members); len(got) != 1 {
		t.Fatalf("mention at end of content must bind, resolved %v", keys(got))
	}
}

func TestResolveMentionsPrefersLongestName(t *testing.T) {
	members := []MemberProfile{
		member(PersonalityAgent(testAgentID), "Kuro"),
		member(PersonalityAgent(testAgent2ID), "Kuro Prod"),
	}
	got := resolveMentions("@Kuro Prod デプロイお願い", members)
	if len(got) != 1 || got[0].Key() != "personality_agent:"+testAgent2ID {
		t.Fatalf("expected only Kuro Prod to bind, resolved %v", keys(got))
	}
	got = resolveMentions("@Kuro と @Kuro Prod", members)
	if len(got) != 2 {
		t.Fatalf("expected both to bind, resolved %v", keys(got))
	}
}

func TestResolveMentionsBindsEveryMemberSharingTheName(t *testing.T) {
	// Two Humans with the same visible name: an ambiguous call addresses both.
	members := []MemberProfile{
		member(Human(testHumanID), "Kai"),
		member(Human(testHuman2ID), "Kai"),
	}
	got := resolveMentions("@Kai 会議です", members)
	if len(got) != 2 {
		t.Fatalf("expected both visible Kai names to bind, resolved %v", keys(got))
	}
}

func TestResolveMentionsUsesVisibleSecretaryQualifier(t *testing.T) {
	members := []MemberProfile{
		{Participant: PersonalityAgent(testAgentID), DisplayName: "Sumi", SecretaryForDisplayName: "Yohaku"},
		{Participant: PersonalityAgent(testAgent2ID), DisplayName: "Sumi", SecretaryForDisplayName: "Haru"},
	}
	if got := resolveMentions("@Sumi 見て", members); len(got) != 0 {
		t.Fatalf("hidden ambiguous alias notified agents: %v", keys(got))
	}
	got := resolveMentions("@Sumi（Haru） 見て", members)
	if len(got) != 1 || got[0] != PersonalityAgent(testAgent2ID) {
		t.Fatalf("qualified mention resolved %v", keys(got))
	}
	members[1].SecretaryForDisplayName = "Yohaku"
	got = resolveMentions("@Sumi（Yohaku） 見て", members)
	if len(got) != 2 {
		t.Fatalf("identical visible names lost explicit ambiguity: %v", keys(got))
	}
}

func TestResolveMentionsIgnoresEmptyNamesAndPlainText(t *testing.T) {
	members := []MemberProfile{
		member(Human(testHumanID), ""),
		member(PersonalityAgent(testAgentID), "Kuro"),
	}
	if got := resolveMentions("mention の無い本文 Kuro", members); len(got) != 0 {
		t.Fatalf("plain text must not bind, resolved %v", keys(got))
	}
}
