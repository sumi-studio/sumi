import { z } from "zod";

// Declarative UI node. The agent emits a tree of these; each `type` must
// resolve to a component in the catalog registry (never to executable code).
export interface SduiNode {
  type: string;
  props?: Record<string, unknown>;
  children?: SduiNode[];
}

export const sduiNodeSchema: z.ZodType<SduiNode> = z.lazy(() =>
  z.object({
    type: z.string(),
    props: z.record(z.string(), z.unknown()).optional(),
    children: z.array(sduiNodeSchema).optional(),
  }),
);
