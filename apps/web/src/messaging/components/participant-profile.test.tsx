// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";
import {
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { MemberProfile, ParticipantRef, PlaceKey } from "../model";
import { participantKey } from "../model";
import {
  bindMessagingSessionIdentity,
  getMessagingSessionIdentity,
  useMessaging,
} from "../store";
import { MemberList } from "./member-list";
import { ParticipantProfilePopover } from "./participant-profile";
import { Sidebar } from "./sidebar";

const navigation = vi.hoisted(() => ({ navigate: vi.fn() }));

vi.mock("../place-route", () => ({
  usePlaceNavigate: () => navigation.navigate,
}));

const human: ParticipantRef = { kind: "human", humanId: "human-a" };
const agent: ParticipantRef = {
  kind: "personality_agent",
  personalityAgentId: "agent-a",
};
const secondAgent: ParticipantRef = {
  kind: "personality_agent",
  personalityAgentId: "agent-b",
};
const humanKey = participantKey(human);
const agentKey = participantKey(agent);

const members: MemberProfile[] = [
  { participant: human, displayName: "余白", tagline: "創業・デザイン" },
  { participant: agent, displayName: "墨", tagline: "秘書" },
  { participant: secondAgent, displayName: "筆", tagline: "編集" },
];

const startDM = vi.fn<(participants: ParticipantRef[]) => Promise<PlaceKey>>();

function setMembers() {
  useMessaging.setState({
    ready: true,
    self: human,
    selfKey: humanKey,
    membersByKey: Object.fromEntries(
      members.map((member) => [participantKey(member.participant), member]),
    ),
    statusByKey: {
      [agentKey]: {
        participant: agent,
        status: "busy",
        note: "設計中",
        expiresAt: null,
      },
    },
    startDM,
  });
}

beforeEach(() => {
  bindMessagingSessionIdentity(null);
  bindMessagingSessionIdentity("human-a");
  navigation.navigate.mockReset();
  startDM.mockReset();
  startDM.mockResolvedValue("dm:dm-a");
  setMembers();
});

afterEach(() => {
  cleanup();
  bindMessagingSessionIdentity(null);
  vi.clearAllMocks();
});

function openCard(name: string) {
  fireEvent.click(screen.getByRole("button", { name }));
}

describe("ParticipantProfilePopover", () => {
  it("表示名・tagline・自己申告ステータスを出す", async () => {
    render(
      <ParticipantProfilePopover participantKey={agentKey}>
        墨
      </ParticipantProfilePopover>,
    );
    expect(screen.queryByText("秘書")).not.toBeInTheDocument();

    openCard("墨");

    expect(await screen.findByText("秘書")).toBeInTheDocument();
    expect(screen.getByText("取り込み中")).toBeInTheDocument();
    expect(screen.getByText(/設計中/)).toBeInTheDocument();
  });

  it("人間と人格agentの別を文字として出さない", async () => {
    render(
      <ParticipantProfilePopover participantKey={agentKey}>
        墨
      </ParticipantProfilePopover>,
    );
    openCard("墨");
    await screen.findByText("秘書");

    expect(document.body.textContent).not.toMatch(
      /personality_agent|human|bot|ボット|AI/i,
    );
  });

  it("ステータス未申告なら何も推測して出さない", async () => {
    render(
      <ParticipantProfilePopover participantKey={agentKey}>
        墨
      </ParticipantProfilePopover>,
    );
    useMessaging.setState({ statusByKey: {} });
    openCard("墨");

    expect(await screen.findByText("秘書")).toBeInTheDocument();
    expect(screen.queryByText("対応可能")).not.toBeInTheDocument();
  });

  it("カードからDMを開始して、そのDMへ遷移する", async () => {
    render(
      <ParticipantProfilePopover participantKey={agentKey}>
        墨
      </ParticipantProfilePopover>,
    );
    openCard("墨");

    fireEvent.click(await screen.findByRole("button", { name: "DMを送る" }));

    await waitFor(() =>
      expect(navigation.navigate).toHaveBeenCalledWith("dm:dm-a"),
    );
    expect(startDM).toHaveBeenCalledWith([agent]);
  });

  it("DMを開けなかったら失敗を伝えて閉じない", async () => {
    startDM.mockRejectedValue(new Error("offline"));
    render(
      <ParticipantProfilePopover participantKey={agentKey}>
        墨
      </ParticipantProfilePopover>,
    );
    openCard("墨");

    fireEvent.click(await screen.findByRole("button", { name: "DMを送る" }));

    const alert = await screen.findByRole("alert");
    expect(alert).toHaveTextContent("DMを開けませんでした");
    expect(navigation.navigate).not.toHaveBeenCalled();
    expect(screen.getByText("秘書")).toBeInTheDocument();
  });

  it("storeの完了後にidentityを見直してから遷移する", async () => {
    startDM.mockImplementation(() =>
      Promise.resolve<PlaceKey>("dm:dm-a").then((place) => {
        queueMicrotask(() => {
          bindMessagingSessionIdentity(null);
          bindMessagingSessionIdentity("human-b");
        });
        return place;
      }),
    );
    render(
      <ParticipantProfilePopover participantKey={agentKey}>
        墨
      </ParticipantProfilePopover>,
    );
    openCard("墨");

    fireEvent.click(await screen.findByRole("button", { name: "DMを送る" }));

    await waitFor(() => expect(getMessagingSessionIdentity()).toBe("human-b"));
    expect(startDM).toHaveBeenCalledTimes(1);
    expect(navigation.navigate).not.toHaveBeenCalled();
  });

  it("identityが切り替わったら開いたままのカードに別人を残さない", async () => {
    render(
      <ParticipantProfilePopover participantKey={agentKey}>
        墨
      </ParticipantProfilePopover>,
    );
    openCard("墨");
    expect(await screen.findByText("秘書")).toBeInTheDocument();

    bindMessagingSessionIdentity(null);
    bindMessagingSessionIdentity("human-b");

    // 別人のカードが残らないだけでなく、枠そのものが閉じる。
    await waitFor(() =>
      expect(screen.getByRole("button", { name: "墨" })).toHaveAttribute(
        "aria-expanded",
        "false",
      ),
    );
    expect(screen.queryByText("秘書")).not.toBeInTheDocument();
    expect(
      screen.queryByText("この参加者の情報がまだありません"),
    ).not.toBeInTheDocument();
  });

  it("自分にはDM導線を出さない", async () => {
    render(
      <ParticipantProfilePopover participantKey={humanKey}>
        余白
      </ParticipantProfilePopover>,
    );
    openCard("余白");

    expect(await screen.findByText("創業・デザイン")).toBeInTheDocument();
    expect(
      screen.queryByRole("button", { name: "DMを送る" }),
    ).not.toBeInTheDocument();
  });
});

describe("MemberList のプロフィール導線", () => {
  it("アバターから同じプロフィールカードが開く", async () => {
    render(<MemberList />);

    fireEvent.click(screen.getByRole("button", { name: "墨のプロフィール" }));

    expect(await screen.findByText("秘書")).toBeInTheDocument();
    expect(
      screen.getByRole("button", { name: "DMを送る" }),
    ).toBeInTheDocument();
    expect(navigation.navigate).not.toHaveBeenCalled();
  });

  it("行そのものはこれまで通りDMを開始する", async () => {
    render(<MemberList />);

    fireEvent.click(screen.getByRole("button", { name: "墨にDMを送る" }));

    await waitFor(() =>
      expect(navigation.navigate).toHaveBeenCalledWith("dm:dm-a"),
    );
    expect(screen.queryByText("秘書")).not.toBeInTheDocument();
  });

  it("プロフィールとDMを別のbuttonに分け、入れ子にしない", () => {
    const { container } = render(<MemberList />);

    expect(container.querySelectorAll("button button")).toHaveLength(0);
  });
});

describe("Sidebar のプロフィール導線", () => {
  beforeEach(() => {
    useMessaging.setState({
      capabilities: {
        status: true,
        replyLater: false,
        reactions: false,
        notifications: false,
      },
      workspaces: [{ workspaceId: "workspace-a", name: "Sumi" }],
      channels: [],
      dms: [{ dmId: "dm-a", kind: "dm", participants: [human, agent] }],
      unreadCountByPlace: {},
      mentionCountByPlace: {},
    });
  });

  function renderSidebar() {
    return render(
      <Sidebar selectedPlaceKey={null} workspaceId="workspace-a" />,
    );
  }

  it("DM相手のアバターから同じプロフィールカードが開く", async () => {
    renderSidebar();

    fireEvent.click(screen.getByRole("button", { name: "墨のプロフィール" }));

    expect(await screen.findByText("秘書")).toBeInTheDocument();
    expect(screen.getByText("取り込み中")).toBeInTheDocument();
    expect(
      screen.getByRole("button", { name: "DMを送る" }),
    ).toBeInTheDocument();
    expect(navigation.navigate).not.toHaveBeenCalled();
  });

  it("DM行の名前はこれまで通りplace遷移のまま", () => {
    renderSidebar();

    fireEvent.click(screen.getByRole("button", { name: "墨" }));

    expect(navigation.navigate).toHaveBeenCalledWith("dm:dm-a");
    expect(screen.queryByText("秘書")).not.toBeInTheDocument();
  });

  it("グループDMのアバターから先頭参加者のプロフィールを開かない", () => {
    useMessaging.setState({
      dms: [
        {
          dmId: "group-a",
          kind: "group_dm",
          participants: [human, agent, secondAgent],
        },
      ],
    });
    renderSidebar();

    expect(
      screen.queryByRole("button", { name: "墨のプロフィール" }),
    ).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: "墨、筆" })).toBeInTheDocument();
    expect(screen.queryByText("秘書")).not.toBeInTheDocument();
  });

  it("自分のプロフィール行のアバターから同じカードが開く", async () => {
    renderSidebar();

    fireEvent.click(screen.getByRole("button", { name: "余白のプロフィール" }));

    expect(await screen.findByText("創業・デザイン")).toBeInTheDocument();
    expect(
      screen.queryByRole("button", { name: "離席中" }),
    ).not.toBeInTheDocument();
  });

  it("自分の行の名前とステータスはこれまで通りステータス変更のまま", () => {
    const setStatus = vi.fn();
    useMessaging.setState({ setStatus });
    renderSidebar();

    fireEvent.click(screen.getByRole("button", { name: "余白 対応可能" }));
    fireEvent.click(screen.getByRole("radio", { name: /取り込み中/ }));

    expect(setStatus).toHaveBeenCalledWith("busy", "取り込み中");
    expect(screen.queryByText("創業・デザイン")).not.toBeInTheDocument();
  });

  it("プロフィールと元の操作を別のbuttonに分け、入れ子にしない", () => {
    const { container } = renderSidebar();

    expect(container.querySelectorAll("button button")).toHaveLength(0);
  });
});
