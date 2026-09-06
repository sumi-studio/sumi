"use client";

import {
  type ComponentType,
  createContext,
  type ReactNode,
  type Ref,
  useCallback,
  useContext,
  useState,
} from "react";

// undefined keeps the library's normal parent-portal/body behavior. null waits
// for a scoped host; it must never be converted into the default body target.
export const PortalContainerContext = createContext<
  HTMLElement | null | undefined
>(undefined);

export function usePortalContainer() {
  return useContext(PortalContainerContext);
}

function useRetainedContainer() {
  const [container, setContainer] = useState<HTMLDivElement | null>(null);
  const retainContainer = useCallback((element: HTMLDivElement | null) => {
    // Activity disconnects refs when hidden but retains the DOM. Keep that
    // container so an open descendant cannot escape to body during the pause.
    if (element !== null) setContainer(element);
  }, []);
  return [container, retainContainer] as const;
}

/** Keep scoped portals physically inside the same Activity visibility boundary. */
export function PortalContainerBoundary({ children }: { children: ReactNode }) {
  const [container, retainContainer] = useRetainedContainer();
  return (
    <PortalContainerContext value={container}>
      <div
        ref={retainContainer}
        data-portal-container=""
        style={{ display: "contents" }}
      >
        {children}
      </div>
    </PortalContainerContext>
  );
}

type PortalComponent = ComponentType<{
  children?: ReactNode;
  container?: HTMLElement | null;
  ref?: Ref<HTMLDivElement>;
}>;

/** Preserve nested portal DOM containment and the library's focus boundaries. */
export function ScopedPortal({
  component: Portal,
  children,
}: {
  component: PortalComponent;
  children: ReactNode;
}) {
  const parentContainer = usePortalContainer();
  const [container, retainContainer] = useRetainedContainer();
  return (
    <Portal
      container={parentContainer}
      ref={retainContainer}
      data-portal-scope=""
    >
      <PortalContainerContext value={container}>
        {children}
      </PortalContainerContext>
    </Portal>
  );
}
