// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { MemberProfile, ParticipantRef } from "../model";
import { participantKey } from "../model";
import { useMessaging } from "../store";
import { MemberList } from "./member-list";
import { ParticipantProfilePopover } from "./participant-profile";

const mocks = vi.hoisted(() => ({
  navigate: vi.fn(),
  startDM: vi.fn(),
}));

vi.mock("@tanstack/react-router", () => ({
  useNavigate: () => mocks.navigate,
}));

const human: ParticipantRef = { kind: "human", humanId: "h1" };
const agent: ParticipantRef = {
  kind: "personality_agent",
  personalityAgentId: "a1",
};
const humanKey = participantKey(human);
const agentKey = participantKey(agent);

const members: MemberProfile[] = [
  { participant: human, displayName: "余白", tagline: "創業・デザイン" },
  { participant: agent, displayName: "墨", tagline: "秘書" },
];

beforeEach(() => {
  mocks.startDM.mockResolvedValue("dm:d1");
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
    startDM: mocks.startDM,
  });
});

afterEach(() => {
  cleanup();
  vi.clearAllMocks();
});

function openCard(name: string) {
  fireEvent.click(screen.getByRole("button", { name }));
}

describe("ParticipantProfilePopover", () => {
  it("表示名・tagline・自己申告ステータスを出す", () => {
    render(
      <ParticipantProfilePopover participantKey={agentKey}>
        墨
      </ParticipantProfilePopover>,
    );
    expect(screen.queryByText("秘書")).not.toBeInTheDocument();

    openCard("墨");

    expect(screen.getByText("秘書")).toBeInTheDocument();
    expect(screen.getByText("取り込み中")).toBeInTheDocument();
    expect(screen.getByText(/設計中/)).toBeInTheDocument();
  });

  it("人間と人格agentの別を文字として出さない", () => {
    render(
      <ParticipantProfilePopover participantKey={agentKey}>
        墨
      </ParticipantProfilePopover>,
    );
    openCard("墨");

    expect(document.body.textContent).not.toMatch(
      /personality_agent|human|bot|ボット|AI/i,
    );
  });

  it("ステータス未申告なら何も推測して出さない", () => {
    render(
      <ParticipantProfilePopover participantKey={agentKey}>
        墨
      </ParticipantProfilePopover>,
    );
    useMessaging.setState({ statusByKey: {} });
    openCard("墨");

    expect(screen.queryByText("対応可能")).not.toBeInTheDocument();
    expect(screen.getByText("秘書")).toBeInTheDocument();
  });

  it("カードからDMを開始して、そのDMへ遷移する", async () => {
    render(
      <ParticipantProfilePopover participantKey={agentKey}>
        墨
      </ParticipantProfilePopover>,
    );
    openCard("墨");

    fireEvent.click(screen.getByRole("button", { name: "DMを送る" }));

    await vi.waitFor(() => {
      expect(mocks.navigate).toHaveBeenCalledWith({
        to: "/dm/$dmId",
        params: { dmId: "d1" },
      });
    });
    expect(mocks.startDM).toHaveBeenCalledWith([agent]);
  });

  it("DMを開けなかったら失敗を伝えて閉じない", async () => {
    mocks.startDM.mockRejectedValue(new Error("boom"));
    render(
      <ParticipantProfilePopover participantKey={agentKey}>
        墨
      </ParticipantProfilePopover>,
    );
    openCard("墨");

    fireEvent.click(screen.getByRole("button", { name: "DMを送る" }));

    await screen.findByText("DMを開けませんでした");
    expect(mocks.navigate).not.toHaveBeenCalled();
  });

  it("自分にはDM導線を出さない", () => {
    render(
      <ParticipantProfilePopover participantKey={humanKey}>
        余白
      </ParticipantProfilePopover>,
    );
    openCard("余白");

    expect(screen.getByText("創業・デザイン")).toBeInTheDocument();
    expect(
      screen.queryByRole("button", { name: "DMを送る" }),
    ).not.toBeInTheDocument();
  });
});

describe("MemberList", () => {
  it("行をクリックすると同じプロフィールカードが開く", () => {
    render(<MemberList />);

    fireEvent.click(screen.getByRole("button", { name: /墨/ }));

    expect(screen.getByText("秘書")).toBeInTheDocument();
    expect(
      screen.getByRole("button", { name: "DMを送る" }),
    ).toBeInTheDocument();
  });
});
