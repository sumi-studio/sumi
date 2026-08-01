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
	// Two participants named "Sumi" (the 戸籍 default): an ambiguous call
	// addresses all of them.
	members := []MemberProfile{
		member(Human(testHumanID), "Sumi"),
		member(PersonalityAgent(testAgentID), "Sumi"),
	}
	got := resolveMentions("@Sumi 会議です", members)
	if len(got) != 2 {
		t.Fatalf("expected both Sumi to bind, resolved %v", keys(got))
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
