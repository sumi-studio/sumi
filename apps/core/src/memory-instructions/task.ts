export interface MemoryOrganizationTask {
  transition: "l0_to_l1" | "l1_to_l2" | "l2_to_l2";
  chunk_seq: number;
  first_seq: number;
  last_seq: number;
}

const TASK_MARKER = "[memory_organization]";

/** Task identity is separate from its editable natural-language instructions. */
export function memoryTaskHeader(task: MemoryOrganizationTask): string {
  return `${TASK_MARKER}\n${JSON.stringify(task)}`;
}

export function memoryTaskFromInstruction(
  content: string,
): MemoryOrganizationTask | null {
  const [marker, metadata] = content.split("\n", 2);
  if (marker !== TASK_MARKER || !metadata) return null;
  try {
    const task = JSON.parse(metadata) as MemoryOrganizationTask;
    if (
      !["l0_to_l1", "l1_to_l2", "l2_to_l2"].includes(task?.transition) ||
      ![task.chunk_seq, task.first_seq, task.last_seq].every(
        (n) => Number.isSafeInteger(n) && n > 0,
      ) ||
      task.last_seq < task.first_seq
    )
      return null;
    return task;
  } catch {
    return null;
  }
}
