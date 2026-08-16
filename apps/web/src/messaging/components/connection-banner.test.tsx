// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";
import { act, cleanup, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { useMessaging } from "../store";
import { ConnectionBanner } from "./connection-banner";

describe("ConnectionBanner", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    act(() =>
      useMessaging.setState({ connection: "connected", everConnected: true }),
    );
  });

  afterEach(() => {
    cleanup();
    vi.useRealTimers();
  });

  it("renders nothing while connected", () => {
    render(<ConnectionBanner />);
    expect(screen.queryByRole("status")).not.toBeInTheDocument();
  });

  it("does not report an initial handshake as a reconnection", () => {
    act(() =>
      useMessaging.setState({
        connection: "reconnecting",
        everConnected: false,
      }),
    );
    render(<ConnectionBanner />);

    act(() => useMessaging.setState({ connection: "connected" }));

    expect(screen.queryByRole("status")).not.toBeInTheDocument();
  });

  it("reports an interruption that began before this banner mounted", () => {
    // The shell keeps the transport alive; the user returns to Messaging while
    // the socket is already reconnecting after having been connected earlier.
    act(() =>
      useMessaging.setState({
        connection: "reconnecting",
        everConnected: true,
      }),
    );
    render(<ConnectionBanner />);
    expect(screen.queryByRole("status")).not.toBeInTheDocument();

    act(() => vi.advanceTimersByTime(1_500));
    expect(screen.getByRole("status")).toHaveTextContent("再接続中…");
  });

  it("shows a reconnecting banner while the socket is down", () => {
    render(<ConnectionBanner />);
    act(() => useMessaging.setState({ connection: "reconnecting" }));
    expect(screen.queryByRole("status")).not.toBeInTheDocument();

    act(() => vi.advanceTimersByTime(1_500));
    expect(screen.getByRole("status")).toHaveTextContent("再接続中…");

    act(() => useMessaging.setState({ connection: "disconnected" }));
    expect(screen.getByRole("status")).toHaveTextContent(
      "サーバーに接続できません",
    );
  });

  it("suppresses a reconnecting blip shorter than the delay", () => {
    render(<ConnectionBanner />);
    act(() => useMessaging.setState({ connection: "reconnecting" }));
    act(() => vi.advanceTimersByTime(1_000));
    act(() => useMessaging.setState({ connection: "connected" }));

    expect(screen.queryByRole("status")).not.toBeInTheDocument();
  });

  it("flashes a short notice after a visible reconnection, then disappears", () => {
    render(<ConnectionBanner />);
    act(() => useMessaging.setState({ connection: "reconnecting" }));
    act(() => vi.advanceTimersByTime(1_500));
    expect(screen.getByRole("status")).toHaveTextContent("再接続中…");
    act(() => useMessaging.setState({ connection: "connected" }));
    expect(screen.getByRole("status")).toHaveTextContent("再接続しました");

    act(() => vi.advanceTimersByTime(3_000));
    expect(screen.queryByRole("status")).not.toBeInTheDocument();
  });
});
