package messaging

import "context"

type overviewWire struct {
	Self              participantWire     `json:"self"`
	Workspaces        []workspaceWire     `json:"workspaces"`
	Channels          []channelWire       `json:"channels"`
	DMs               []dmWire            `json:"dms"`
	Members           []memberWire        `json:"members"`
	ReadMarkers       []readMarkerWire    `json:"read_markers"`
	UnreadSummaries   []unreadSummaryWire `json:"unread_summaries"`
	ReplyLaterMarkers []replyLaterWire    `json:"reply_later_markers"`
}

func (s *Server) buildOverview(ctx context.Context, store *ScopedStore) (overviewWire, error) {
	viewer := store.Scope.Actor
	summaries, err := store.UnreadSummaries(ctx)
	if err != nil {
		return overviewWire{}, err
	}
	workspace, err := store.Workspace(ctx)
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
			memberSet[key] = memberToWire(p)
			memberOrder = append(memberOrder, key)
		}
	}
	workspaceWires := []workspaceWire{{WorkspaceID: workspace.WorkspaceID, Name: workspace.Name}}
	profiles, err := store.WorkspaceMembers(ctx)
	if err != nil {
		return overviewWire{}, err
	}
	addMembers(profiles)
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
		profiles, err := store.ActiveMembers(ctx, summary.Place.PlaceID)
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
	markers, err := store.ReplyLaterMarkersFor(ctx)
	if err != nil {
		return overviewWire{}, err
	}
	markerWires := make([]replyLaterWire, len(markers))
	for i, marker := range markers {
		markerWires[i] = replyLaterToWire(marker, viewer)
	}
	return overviewWire{
		Self:              participantToWire(viewer),
		Workspaces:        workspaceWires,
		Channels:          channels,
		DMs:               dms,
		Members:           members,
		ReadMarkers:       readMarkers,
		UnreadSummaries:   unread,
		ReplyLaterMarkers: markerWires,
	}, nil
}
