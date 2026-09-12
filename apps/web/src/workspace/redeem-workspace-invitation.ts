import { WorkspaceAPIError } from "./api-client";
import type { WorkspaceMembership } from "./model";
import { workspaceInvitationIdentityProof } from "./workspace-invitation-proof";

/** A backend proof challenge may retry only while the same recipient owns the action. */
export async function redeemWorkspaceInvitation(
  redeem: (idToken?: string) => Promise<WorkspaceMembership>,
  isCurrent: () => boolean,
): Promise<WorkspaceMembership> {
  try {
    return await redeem();
  } catch (reason) {
    if (
      !(reason instanceof WorkspaceAPIError) ||
      reason.status !== 403 ||
      reason.code !== "invitation_email_verification_required"
    )
      throw reason;
    if (!isCurrent()) throw new Error("invitation_identity_changed");
    const proof = await workspaceInvitationIdentityProof();
    if (!isCurrent()) throw new Error("invitation_identity_changed");
    return redeem(proof);
  }
}
