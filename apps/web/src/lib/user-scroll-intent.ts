export interface UserScrollDelta {
  left: number;
  top: number;
}

type UserScrollTarget = (delta: UserScrollDelta) => void;

const registeredTargets = new WeakMap<HTMLElement, UserScrollTarget>();

/**
 * Lets a scroll owner cancel reconciliation or follow state before an
 * out-of-tree control applies a user-authored delta.
 */
export function registerUserScrollTarget(
  target: HTMLElement,
  apply: UserScrollTarget,
): () => void {
  registeredTargets.set(target, apply);
  return () => {
    if (registeredTargets.get(target) === apply)
      registeredTargets.delete(target);
  };
}

/** Applies user scroll intent without synthesizing another wheel event. */
export function applyUserScrollDelta(
  target: HTMLElement,
  delta: UserScrollDelta,
): void {
  const registered = registeredTargets.get(target);
  if (registered) {
    registered(delta);
    return;
  }
  target.scrollTop += delta.top;
  target.scrollLeft += delta.left;
}
