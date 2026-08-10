// @vitest-environment jsdom

import { cleanup, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it } from "vitest";
import { Badge } from "./sidebar";

afterEach(cleanup);

describe("sidebar unread badge", () => {
  it("renders a visible unread count for an unmuted place", () => {
    render(<Badge count={7} urgent={false} muted={false} />);
    expect(screen.getByText("7")).toBeTruthy();
  });

  it("renders nothing when there is no count", () => {
    const { container } = render(
      <Badge count={0} urgent={false} muted={false} />,
    );
    expect(container.textContent).toBe("");
  });

  it("suppresses the count for a muted place", () => {
    const { container } = render(
      <Badge count={7} urgent={true} muted={true} />,
    );
    expect(container.textContent).toBe("");
  });
});
