import {
  PortalContainerContext,
  usePortalContainer,
} from "@sumi/ui/components/portal-container";
import {
  type MouseEvent as ReactMouseEvent,
  type ReactNode,
  type RefObject,
  useCallback,
  useEffect,
  useRef,
  useState,
} from "react";
import { createPortal } from "react-dom";
import { isImeComposing } from "../../lib/ime";

const FOCUSABLE =
  'button:not([disabled]), [href], input:not([disabled]), select:not([disabled]), textarea:not([disabled]), [tabindex]:not([tabindex="-1"])';

function isVisible(element: HTMLElement): boolean {
  if (element.closest("[hidden],[inert]")) return false;
  const visibility = getComputedStyle(element).visibility;
  if (visibility === "hidden" || visibility === "collapse") return false;
  for (
    let node: HTMLElement | null = element;
    node;
    node = node.parentElement
  ) {
    if (getComputedStyle(node).display === "none") return false;
  }
  return true;
}

function focusableChildren(element: HTMLElement): HTMLElement[] {
  return [...element.querySelectorAll<HTMLElement>(FOCUSABLE)].filter(
    (candidate) =>
      !candidate.hasAttribute("disabled") &&
      candidate.tabIndex >= 0 &&
      isVisible(candidate),
  );
}

/** A real modal: capture Escape, keep Tab inside, and restore its trigger. */
export function ModalDialog({
  label,
  onClose,
  children,
  className,
  initialFocusRef,
  finalFocusRef,
  onBackdropClick,
  testId,
}: {
  label: string;
  onClose: () => void;
  children: ReactNode;
  className: string;
  initialFocusRef?: RefObject<HTMLElement | null>;
  finalFocusRef?: RefObject<HTMLElement | null>;
  onBackdropClick?: (event: ReactMouseEvent<HTMLDivElement>) => void;
  testId?: string;
}) {
  const container = usePortalContainer();
  const portalTarget =
    container === undefined
      ? typeof document === "undefined"
        ? null
        : document.body
      : container;
  const dialogRef = useRef<HTMLDivElement>(null);
  const [dialogElement, setDialogElement] = useState<HTMLDivElement | null>(
    null,
  );
  const retainDialog = useCallback((element: HTMLDivElement | null) => {
    dialogRef.current = element;
    if (element !== null) setDialogElement(element);
  }, []);
  const returnFocusRef = useRef<HTMLElement | null>(null);

  useEffect(() => {
    if (!portalTarget) return;
    if (!returnFocusRef.current?.isConnected) {
      returnFocusRef.current =
        document.activeElement instanceof HTMLElement
          ? document.activeElement
          : null;
    }
    (initialFocusRef?.current ?? dialogRef.current)?.focus();
    return () => {
      const trigger = finalFocusRef?.current ?? returnFocusRef.current;
      if (trigger?.isConnected && isVisible(trigger)) {
        trigger.focus();
      }
    };
  }, [finalFocusRef, initialFocusRef, portalTarget]);

  useEffect(() => {
    if (!portalTarget) return;
    const onKeyDown = (event: KeyboardEvent) => {
      const dialog = dialogRef.current;
      if (!dialog) return;
      const nestedScope =
        document.activeElement instanceof Element
          ? document.activeElement.closest("[data-portal-scope]")
          : null;
      // Let a nested overlay handle its own Escape and focus boundary first.
      if (
        nestedScope &&
        nestedScope !== dialog &&
        dialog.contains(nestedScope)
      ) {
        return;
      }
      if (event.key === "Escape") {
        if (isImeComposing(event)) return;
        event.stopPropagation();
        onClose();
        return;
      }
      if (event.key !== "Tab") return;
      const focusable = focusableChildren(dialog);
      if (focusable.length === 0) {
        event.preventDefault();
        dialog.focus();
        return;
      }
      const current = document.activeElement;
      const first = focusable[0];
      const last = focusable[focusable.length - 1];
      if (
        event.shiftKey &&
        (current === first || current === dialog || !dialog.contains(current))
      ) {
        event.preventDefault();
        last.focus();
      } else if (
        !event.shiftKey &&
        (current === last || !dialog.contains(current))
      ) {
        event.preventDefault();
        first.focus();
      }
    };
    // Capture keeps Escape from reaching existing bubble-phase overlay handlers.
    document.addEventListener("keydown", onKeyDown, true);
    return () => document.removeEventListener("keydown", onKeyDown, true);
  }, [onClose, portalTarget]);

  if (!portalTarget) return null;

  return createPortal(
    // biome-ignore lint/a11y/useKeyWithClickEvents: backdrop close is redundant; Escape and the close control are keyboard exits.
    <div
      ref={retainDialog}
      data-portal-scope=""
      role="dialog"
      aria-modal="true"
      aria-label={label}
      tabIndex={-1}
      data-testid={testId}
      className={className}
      onClick={onBackdropClick}
    >
      <PortalContainerContext value={dialogElement}>
        {children}
      </PortalContainerContext>
    </div>,
    portalTarget,
  );
}
