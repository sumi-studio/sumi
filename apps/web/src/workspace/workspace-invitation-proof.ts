import { getIdToken } from "firebase/auth";
import { getFirebaseAuth } from "../auth/firebase";
/** Only the already signed-in identity may prove an email-bound invitation. */
export async function workspaceInvitationIdentityProof(): Promise<string> {
  let auth: ReturnType<typeof getFirebaseAuth>;
  try {
    auth = getFirebaseAuth();
  } catch {
    throw new Error("invitation_identity_unavailable");
  }
  const user = auth.currentUser;
  if (!user) throw new Error("invitation_identity_unavailable");
  const token = await getIdToken(user, true);
  if (auth.currentUser?.uid !== user.uid)
    throw new Error("invitation_identity_unavailable");
  return token;
}
