package messaging

import "context"

type overviewWire struct {
	Self       participantWire `json:"self"`
	Workspaces []workspaceWire `json:"workspaces"`
	Channels   []channelWire   `json:"channels"`
	DMs        []dmWire        `json:"dms"`
	// Threads carry their parent, so the agent reads a side conversation as
	// belonging to a channel rather than as a stray place beside it.
	Threads         []threadWire        `json:"threads"`
	Members         []memberWire        `json:"members"`
	ReadMarkers     []readMarkerWire    `json:"read_markers"`
	UnreadSummaries []unreadSummaryWire `json:"unread_summaries"`
}

func (s *Server) buildOverview(ctx context.Context, viewer ParticipantRef) (overviewWire, error) {
	summaries, err := s.Store.UnreadSummaries(ctx, viewer)
	if err != nil {
		return overviewWire{}, err
	}
	workspaces, err := s.Store.WorkspacesFor(ctx, viewer)
	if err != nil {
		return overviewWire{}, err
	}
	memberSet := map[string]memberWire{}
	var memberOrder []string
	addMembers := func(profiles []MemberProfile) {
		for _, p := range profiles {
			key := p.Participant.Key()
			if _, seen := memberSet[key]; seen {
				continue
			}
			memberSet[key] = memberWire{Participant: participantToWire(p.Participant), DisplayName: p.ProjectedDisplayName()}
			memberOrder = append(memberOrder, key)
		}
	}
	workspaceWires := make([]workspaceWire, len(workspaces))
	for i, workspace := range workspaces {
		workspaceWires[i] = workspaceWire{WorkspaceID: workspace.WorkspaceID, Name: workspace.Name}
		profiles, err := s.Store.WorkspaceMemberProfiles(ctx, workspace.WorkspaceID, viewer)
		if err != nil {
			return overviewWire{}, err
		}
		addMembers(profiles)
	}
	channels, dms := []channelWire{}, []dmWire{}
	readMarkers := []readMarkerWire{}
	unread := []unreadSummaryWire{}
	for _, summary := range summaries {
		place := placeToWire(summary.Place)
		readMarkers = append(readMarkers, readMarkerWire{Place: place, LastReadSeq: summary.LastReadSeq})
		unread = append(unread, unreadSummaryWire{Place: place, LatestSeq: summary.Place.LastSeq, UnreadCount: summary.UnreadCount, MentionCount: summary.MentionCount})
		if summary.Place.Kind == PlaceChannel {
			channels = append(channels, channelToWire(summary.Place))
			continue
		}
		if summary.Place.Kind == PlaceThread {
			continue
		}
		profiles, err := s.Store.ActiveMembers(ctx, summary.Place.PlaceID, viewer)
		if err != nil {
			return overviewWire{}, err
		}
		addMembers(profiles)
		participants := make([]participantWire, len(profiles))
		for i, profile := range profiles {
			participants[i] = participantToWire(profile.Participant)
		}
		dms = append(dms, dmWire{DMID: summary.Place.PlaceID, Kind: summary.Place.Kind, Participants: participants})
	}
	members := make([]memberWire, len(memberOrder))
	for i, key := range memberOrder {
		members[i] = memberSet[key]
	}
	threads, err := s.Store.ThreadsFor(ctx, viewer)
	if err != nil {
		return overviewWire{}, err
	}
	return overviewWire{Self: participantToWire(viewer), Workspaces: workspaceWires, Channels: channels, DMs: dms, Threads: threadsToWire(threads), Members: members, ReadMarkers: readMarkers, UnreadSummaries: unread}, nil
}
