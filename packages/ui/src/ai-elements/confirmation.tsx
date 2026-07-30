import { ShieldAlertIcon } from "lucide-react";
import {
  type ComponentProps,
  createContext,
  type HTMLAttributes,
  useContext,
} from "react";
import { Alert, AlertDescription, AlertTitle } from "../components/alert";
import { Button } from "../components/button";
import { cn } from "../lib/utils";

export type ConfirmationState =
  | "approval-requested"
  | "approval-responded"
  | "output-available"
  | "output-denied";

const ConfirmationContext =
  createContext<ConfirmationState>("approval-requested");

/** AI Elements Confirmationに倣い、承認の状態と部品を分離した外枠。 */
export function Confirmation({
  className,
  children,
  state = "approval-requested",
  ...props
}: ComponentProps<typeof Alert> & { state?: ConfirmationState }) {
  return (
    <ConfirmationContext.Provider value={state}>
      <Alert
        className={cn("flex flex-col gap-2 rounded-2xl p-3.5", className)}
        {...props}
      >
        <ShieldAlertIcon className="size-4.5 text-muted-foreground" />
        {children}
      </Alert>
    </ConfirmationContext.Provider>
  );
}

export function ConfirmationTitle({
  className,
  ...props
}: ComponentProps<typeof AlertTitle>) {
  return <AlertTitle className={cn("text-[15px]", className)} {...props} />;
}

export function ConfirmationRequest({
  className,
  ...props
}: ComponentProps<typeof AlertDescription>) {
  const state = useContext(ConfirmationContext);
  if (state !== "approval-requested") {
    return null;
  }
  return <AlertDescription className={cn("inline", className)} {...props} />;
}

export function ConfirmationAccepted({
  className,
  ...props
}: ComponentProps<typeof AlertDescription>) {
  const state = useContext(ConfirmationContext);
  if (state !== "approval-responded" && state !== "output-available") {
    return null;
  }
  return <AlertDescription className={cn("inline", className)} {...props} />;
}

export function ConfirmationRejected({
  className,
  ...props
}: ComponentProps<typeof AlertDescription>) {
  const state = useContext(ConfirmationContext);
  if (state !== "output-denied") {
    return null;
  }
  return <AlertDescription className={cn("inline", className)} {...props} />;
}

export function ConfirmationActions({
  className,
  ...props
}: HTMLAttributes<HTMLDivElement>) {
  const state = useContext(ConfirmationContext);
  if (state !== "approval-requested") {
    return null;
  }
  return (
    <div
      className={cn("col-start-2 mt-2 flex flex-col gap-2", className)}
      {...props}
    />
  );
}

export function ConfirmationAction({
  className,
  size = "lg",
  ...props
}: ComponentProps<typeof Button>) {
  return (
    <Button
      type="button"
      size={size}
      className={cn("w-full justify-start rounded-xl", className)}
      {...props}
    />
  );
}
