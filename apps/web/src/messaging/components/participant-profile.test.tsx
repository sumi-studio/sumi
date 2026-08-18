// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";
import {
  act,
  cleanup,
  createEvent,
  fireEvent,
  render,
  screen,
  waitFor,
  within,
} from "@testing-library/react";
import { useLayoutEffect } from "react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { MockMessagingServer } from "../mock-server";
import type {
  DmSummary,
  MemberProfile,
  Message,
  ParticipantKey,
  ParticipantRef,
  PlaceKey,
} from "../model";
import { participantKey } from "../model";
import {
  bindMessagingSessionIdentity,
  getMessagingSessionIdentity,
  installMessagingBackend,
  useMessaging,
} from "../store";
import { MemberList } from "./member-list";
import { MessageItem } from "./message-item";
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
/** 今の実APIのmemberWireはtaglineを載せないので、これが本番で出る形。 */
const plain: ParticipantRef = { kind: "human", humanId: "human-b" };
const humanKey = participantKey(human);
const agentKey = participantKey(agent);
const secondAgentKey = participantKey(secondAgent);
const plainKey = participantKey(plain);

const members: MemberProfile[] = [
  { participant: human, displayName: "余白", tagline: "創業・デザイン" },
  { participant: agent, displayName: "墨", tagline: "秘書" },
  { participant: secondAgent, displayName: "筆", tagline: "編集" },
  { participant: plain, displayName: "白紙", tagline: "" },
];

const startDM = vi.fn<(participants: ParticipantRef[]) => Promise<PlaceKey>>();
const realStartDM = useMessaging.getState().startDM;

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((resolvePromise) => {
    resolve = resolvePromise;
  });
  return { promise, resolve };
}

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
        baseStatus: null,
        baseNote: "",
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
  for (const node of document.querySelectorAll(
    '[data-slot="conversation-viewport"]',
  )) {
    node.remove();
  }
  bindMessagingSessionIdentity(null);
  vi.clearAllMocks();
});

/** jsdomはscrollTopを動かさないので、面のスクロール位置を自前で持つ。 */
function trackScrollTop(element: HTMLElement) {
  let top = 0;
  Object.defineProperty(element, "scrollTop", {
    configurable: true,
    get: () => top,
    set: (value: number) => {
      top = value;
    },
  });
  return () => top;
}

/** 会話欄の代わり。render前に置いておく。 */
function conversationViewportStub() {
  const element = document.createElement("div");
  element.setAttribute("data-slot", "conversation-viewport");
  document.body.append(element);
  return trackScrollTop(element);
}

function surface(slot: string) {
  const element = document.querySelector<HTMLElement>(`[data-slot="${slot}"]`);
  if (!element) throw new Error(`no [data-slot="${slot}"]`);
  return element;
}

/** カードの覆う面を問わないテストのための、面のない呼び出し元。 */
const noSurface = () => null;

function wheelOver(element: HTMLElement) {
  const event = createEvent.wheel(element, { deltaY: 120 });
  fireEvent(element, event);
  return event;
}

function openCard(name: string) {
  fireEvent.click(screen.getByRole("button", { name }));
}

describe("ParticipantProfilePopover", () => {
  it("表示名・tagline・自己申告ステータスを出す", async () => {
    render(
      <ParticipantProfilePopover
        participantKey={agentKey}
        scrollPassthrough={noSurface}
      >
        墨
      </ParticipantProfilePopover>,
    );
    expect(screen.queryByText("秘書")).not.toBeInTheDocument();

    openCard("墨");

    expect(
      screen.getByRole("dialog", { name: "墨のプロフィール" }),
    ).toBeInTheDocument();
    expect(await screen.findByText("秘書")).toBeInTheDocument();
    expect(screen.getByText("取り込み中")).toBeInTheDocument();
    expect(screen.getByText(/設計中/)).toBeInTheDocument();
  });

  it("人間と人格agentの別を文字として出さない", async () => {
    render(
      <ParticipantProfilePopover
        participantKey={agentKey}
        scrollPassthrough={noSurface}
      >
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
      <ParticipantProfilePopover
        participantKey={agentKey}
        scrollPassthrough={noSurface}
      >
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
      <ParticipantProfilePopover
        participantKey={agentKey}
        scrollPassthrough={noSurface}
      >
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
      <ParticipantProfilePopover
        participantKey={agentKey}
        scrollPassthrough={noSurface}
      >
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
      <ParticipantProfilePopover
        participantKey={agentKey}
        scrollPassthrough={noSurface}
      >
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
      <ParticipantProfilePopover
        participantKey={agentKey}
        scrollPassthrough={noSurface}
      >
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

  // 今のmainの実APIはmemberWireにtaglineを持たない（#229が足す）。
  // ステータス未申告の相手なら、カードはアバター・表示名・DM導線だけになる。
  it("taglineもステータスも無い相手でも、カードが開いてDMを始められる", async () => {
    render(
      <ParticipantProfilePopover
        participantKey={plainKey}
        scrollPassthrough={noSurface}
      >
        白紙
      </ParticipantProfilePopover>,
    );
    openCard("白紙");

    const send = await screen.findByRole("button", { name: "DMを送る" });
    const card = within(screen.getByRole("dialog"));
    // カードの中身は表示名とDM導線だけ。職務行もステータス行も出ない。
    expect(card.getByText("白紙")).toBeInTheDocument();
    for (const label of ["対応可能", "取り込み中", "離席中"]) {
      expect(card.queryByText(label)).not.toBeInTheDocument();
    }

    fireEvent.click(send);

    await waitFor(() =>
      expect(navigation.navigate).toHaveBeenCalledWith("dm:dm-a"),
    );
    expect(startDM).toHaveBeenCalledWith([plain]);
  });

  it("自分にはDM導線を出さない", async () => {
    render(
      <ParticipantProfilePopover
        participantKey={humanKey}
        scrollPassthrough={noSurface}
      >
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

  it("行のDMが保留の間は別の参加者のカードから2本目を始められない", async () => {
    const pending = deferred<DmSummary>();
    const server = new MockMessagingServer();
    vi.spyOn(server, "ensureDM").mockReturnValue(pending.promise);
    installMessagingBackend(server);
    setMembers();
    useMessaging.setState({ startDM: realStartDM });
    render(<MemberList />);

    fireEvent.click(screen.getByRole("button", { name: "墨にDMを送る" }));
    expect(server.ensureDM).toHaveBeenCalledTimes(1);
    expect(screen.getByRole("button", { name: "墨にDMを送る" })).toBeDisabled();

    fireEvent.click(screen.getByRole("button", { name: "筆のプロフィール" }));
    const send = await screen.findByRole("button", { name: "DMを送る" });

    expect(send).toBeDisabled();
    fireEvent.click(send);
    expect(server.ensureDM).toHaveBeenCalledTimes(1);

    await act(async () => {
      pending.resolve({
        dmId: "dm-a",
        kind: "dm",
        participants: [human, agent],
      });
      await pending.promise;
    });
    await waitFor(() => expect(useMessaging.getState().startingDM).toBeNull());
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
    fireEvent.click(screen.getByRole("button", { name: "取り込み中" }));
    fireEvent.click(screen.getByRole("menuitem", { name: "解除するまで" }));

    expect(setStatus).toHaveBeenCalledWith("busy", "", null);
    expect(screen.queryByText("創業・デザイン")).not.toBeInTheDocument();
  });

  it("プロフィールと元の操作を別のbuttonに分け、入れ子にしない", () => {
    const { container } = renderSidebar();

    expect(container.querySelectorAll("button button")).toHaveLength(0);
  });

  function allowNotifications() {
    useMessaging.setState({
      capabilities: {
        status: true,
        replyLater: false,
        reactions: false,
        notifications: true,
      },
    });
  }

  it("DM行のアバターを右クリックしても行の通知メニューが開く", () => {
    allowNotifications();
    renderSidebar();

    fireEvent.contextMenu(
      screen.getByRole("button", { name: "墨のプロフィール" }),
    );

    expect(
      screen.getByRole("menu", { name: "この場所のメニュー" }),
    ).toBeInTheDocument();
  });

  // 通知パネルはportalではなく行の中にinlineで描かれる。DOM上は行の中でも、
  // 行はhostしているだけで、その中の右クリックは行のものではない。
  it("行がhostしているだけの通知パネルの中の右クリックを行が奪わない", () => {
    allowNotifications();
    renderSidebar();

    fireEvent.click(
      screen.getAllByRole("button", { name: "この場所のメニュー" })[0],
    );
    fireEvent.click(screen.getByRole("menuitem", { name: /通知設定/ }));
    const panel = screen.getByRole("menu", { name: "通知設定" });
    const target = within(panel).getByText("メンションのみ");
    const event = createEvent.contextMenu(target);
    fireEvent(target, event);

    expect(event.defaultPrevented).toBe(false);
    expect(panel).toBeInTheDocument();
  });

  // Reactの合成イベントはportalの子からもReactの親へ上がる。行が所有するのは
  // DOM上で行の中にあるターゲットだけで、行から開いたカードの中は行の外。
  it("行から開いたプロフィールカードの中の右クリックは行の通知メニューを開かない", async () => {
    allowNotifications();
    renderSidebar();

    fireEvent.click(screen.getByRole("button", { name: "墨のプロフィール" }));
    const inCard = await screen.findByRole("button", { name: "DMを送る" });
    const event = createEvent.contextMenu(inCard);
    fireEvent(inCard, event);

    expect(
      screen.queryByRole("dialog", { name: "この場所の通知設定" }),
    ).not.toBeInTheDocument();
    // ブラウザ標準のメニューも奪わない。
    expect(event.defaultPrevented).toBe(false);
  });
});

describe("カードの束縛と転送先", () => {
  // 束縛が変わったrenderで一瞬でも別人が描かれていないかを見るため、
  // 各commit直後のDOMを記録する。passive effectで後から閉じる実装だと、
  // 閉じる前の1commitがここに写る。
  function Probe({ onCommit }: { onCommit: () => void }) {
    useLayoutEffect(() => {
      onCommit();
    });
    return null;
  }

  it("束縛が変わったrenderで別参加者の内容が一瞬も描かれない", async () => {
    const commits: string[] = [];
    const record = () => {
      commits.push(document.body.textContent ?? "");
    };
    function Harness({ target }: { target: ParticipantKey }) {
      return (
        <>
          <ParticipantProfilePopover
            participantKey={target}
            scrollPassthrough={noSurface}
          >
            対象
          </ParticipantProfilePopover>
          <Probe onCommit={record} />
        </>
      );
    }
    const { rerender } = render(<Harness target={agentKey} />);
    openCard("対象");
    expect(await screen.findByText("秘書")).toBeInTheDocument();

    commits.length = 0;
    rerender(<Harness target={secondAgentKey} />);

    expect(commits.length).toBeGreaterThan(0);
    expect(commits.some((text) => text.includes("編集"))).toBe(false);
    expect(commits.some((text) => text.includes("秘書"))).toBe(false);
    expect(screen.queryByText("編集")).not.toBeInTheDocument();
  });

  it("sidebarから開いたカードのホイールは、カードが覆うplace一覧を動かす", async () => {
    const readConversation = conversationViewportStub();
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
    render(<Sidebar selectedPlaceKey={null} workspaceId="workspace-a" />);
    const readPlaces = trackScrollTop(surface("sidebar-places"));

    fireEvent.click(screen.getByRole("button", { name: "墨のプロフィール" }));
    const event = wheelOver(await screen.findByText("秘書"));

    expect(readPlaces()).toBe(120);
    expect(readConversation()).toBe(0);
    expect(event.defaultPrevented).toBe(true);
  });

  it("member listから開いたカードのホイールは、カードが覆うmember listを動かす", async () => {
    const readConversation = conversationViewportStub();
    render(<MemberList />);
    const readMembers = trackScrollTop(surface("member-list"));

    fireEvent.click(screen.getByRole("button", { name: "墨のプロフィール" }));
    const event = wheelOver(await screen.findByText("秘書"));

    expect(readMembers()).toBe(120);
    expect(readConversation()).toBe(0);
    expect(event.defaultPrevented).toBe(true);
  });

  it("会話欄から開いたカードのホイールは会話欄を動かす", async () => {
    const readTop = conversationViewportStub();
    const message: Message = {
      messageId: "m1",
      place: { kind: "channel", channelId: "c1" },
      seq: 1,
      author: agent,
      content: "こんにちは",
      mentions: [],
      urgency: "normal",
      reactions: [],
      attachments: [],
      replyTo: null,
      createdAt: Date.now(),
      editedAt: null,
      deleted: false,
    };
    render(
      <MessageItem
        message={message}
        grouped={false}
        pending={false}
        failed={false}
        selfKey={humanKey}
        membersByKey={useMessaging.getState().membersByKey}
        replyLaterBy={[]}
        allowReactions={false}
        allowReplyLater={false}
        findMessage={() => undefined}
        onReply={vi.fn()}
        onReplyLater={vi.fn()}
        onToggleReaction={vi.fn()}
        onCopyLink={vi.fn()}
        onEdit={vi.fn()}
        onDelete={vi.fn()}
        onJumpTo={vi.fn()}
        onRetry={vi.fn()}
      />,
    );

    fireEvent.click(screen.getByRole("button", { name: "墨のプロフィール" }));
    const event = wheelOver(await screen.findByText("秘書"));

    expect(readTop()).toBe(120);
    expect(event.defaultPrevented).toBe(true);
  });
});
