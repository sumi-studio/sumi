import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { createFileRoute } from "@tanstack/react-router";
import { useEffect, useMemo, useState } from "react";
import {
  createTodo,
  deleteTodo,
  listTodos,
  startDevelopmentSession,
  type Todo,
  TodoAPIError,
  type TodoDueInput,
  type TodoPriority,
  updateTodo,
} from "../lib/todo-api";

export const Route = createFileRoute("/todos")({ component: TodosPage });

type View = "inbox" | "today" | "upcoming" | "overdue" | "done";
type DueKind = "none" | "date" | "datetime";

const viewCopy: Record<View, { title: string; subtitle: string }> = {
  inbox: { title: "My tasks", subtitle: "いま取り組むTodoをひとつに" },
  today: { title: "Today", subtitle: "今日が期限のTodo" },
  upcoming: { title: "Upcoming", subtitle: "これから期限を迎えるTodo" },
  overdue: { title: "Overdue", subtitle: "期限を過ぎた未完了Todo" },
  done: { title: "Completed", subtitle: "完了したTodo" },
};

const priorityCopy: Record<
  TodoPriority,
  { label: string; dot: string; badge: string }
> = {
  none: { label: "優先度なし", dot: "bg-zinc-300", badge: "text-zinc-500" },
  low: { label: "低", dot: "bg-sky-400", badge: "text-sky-700" },
  medium: { label: "中", dot: "bg-amber-400", badge: "text-amber-700" },
  high: { label: "高", dot: "bg-rose-500", badge: "text-rose-700" },
};

function TodosPage() {
  const queryClient = useQueryClient();
  const [view, setView] = useState<View>("inbox");
  const [searchInput, setSearchInput] = useState("");
  const [search, setSearch] = useState("");
  const [sort, setSort] = useState<"updated_at" | "due">("due");
  const [selectedID, setSelectedID] = useState<string | null>(null);
  const [mobileNav, setMobileNav] = useState(false);
  const [notice, setNotice] = useState<string | null>(null);

  useEffect(() => {
    const timer = window.setTimeout(() => setSearch(searchInput.trim()), 250);
    return () => window.clearTimeout(timer);
  }, [searchInput]);

  const filters = useMemo(() => {
    const common = { q: search || undefined, sort, limit: 100 } as const;
    if (view === "done") return { ...common, status: "done" as const };
    if (view === "overdue")
      return { ...common, status: "open" as const, overdue: true };
    return { ...common, status: "open" as const };
  }, [search, sort, view]);

  const todosQuery = useQuery({
    queryKey: ["todos", filters],
    queryFn: () => listTodos(filters),
    retry: false,
  });
  const summaryQuery = useQuery({
    queryKey: ["todos", "summary"],
    queryFn: () => listTodos({ sort: "due", limit: 100 }),
    retry: false,
  });

  const visibleTodos = useMemo(() => {
    const items = todosQuery.data?.items ?? [];
    if (view === "today") return items.filter(isDueToday);
    if (view === "upcoming")
      return items.filter(
        (todo) => todo.due && !isDueToday(todo) && !isOverdue(todo),
      );
    return items;
  }, [todosQuery.data, view]);

  const selectedTodo =
    visibleTodos.find((todo) => todo.id === selectedID) ??
    summaryQuery.data?.items.find((todo) => todo.id === selectedID) ??
    null;
  const summary = summarize(summaryQuery.data?.items ?? []);
  const unauthorized =
    (todosQuery.error instanceof TodoAPIError &&
      todosQuery.error.status === 401) ||
    (summaryQuery.error instanceof TodoAPIError &&
      summaryQuery.error.status === 401);

  const refresh = async () => {
    await queryClient.invalidateQueries({ queryKey: ["todos"] });
  };

  const sessionMutation = useMutation({
    mutationFn: startDevelopmentSession,
    onSuccess: refresh,
    onError: (error) => setNotice(errorMessage(error)),
  });
  const createMutation = useMutation({
    mutationFn: createTodo,
    onSuccess: async (todo) => {
      setNotice("Todoを追加しました");
      setSelectedID(todo.id);
      await refresh();
    },
    onError: (error) => setNotice(errorMessage(error)),
  });
  const updateMutation = useMutation({
    mutationFn: ({
      id,
      input,
    }: {
      id: string;
      input: Parameters<typeof updateTodo>[1];
    }) => updateTodo(id, input),
    onSuccess: async (todo) => {
      setNotice("変更を保存しました");
      setSelectedID(todo.id);
      await refresh();
    },
    onError: (error) => {
      setNotice(errorMessage(error));
      if (error instanceof TodoAPIError && error.status === 409) void refresh();
    },
  });
  const deleteMutation = useMutation({
    mutationFn: ({ id, version }: { id: string; version: number }) =>
      deleteTodo(id, version),
    onSuccess: async () => {
      setSelectedID(null);
      setNotice("Todoを削除しました");
      await refresh();
    },
    onError: (error) => setNotice(errorMessage(error)),
  });

  const setActiveView = (next: View) => {
    setView(next);
    setSelectedID(null);
    setMobileNav(false);
  };

  return (
    <div className="flex h-dvh min-w-0 overflow-hidden bg-[#f7f7f9] text-zinc-900">
      <WorkspaceRail />
      <TodoSidebar
        view={view}
        summary={summary}
        open={mobileNav}
        onClose={() => setMobileNav(false)}
        onSelect={setActiveView}
      />

      <main className="relative flex min-w-0 flex-1 flex-col bg-white">
        <header className="flex h-14 shrink-0 items-center gap-3 border-b border-zinc-200 px-4 md:px-6">
          <button
            type="button"
            aria-label="ナビゲーションを開く"
            className="rounded-lg p-2 text-zinc-600 hover:bg-zinc-100 md:hidden"
            onClick={() => setMobileNav(true)}
          >
            <Icon name="menu" />
          </button>
          <div className="relative mx-auto w-full max-w-xl">
            <span className="pointer-events-none absolute inset-y-0 left-3 flex items-center text-zinc-400">
              <Icon name="search" size={17} />
            </span>
            <input
              aria-label="Todoを検索"
              value={searchInput}
              onChange={(event) => setSearchInput(event.target.value)}
              placeholder="Todoを検索"
              className="h-9 w-full rounded-lg border border-zinc-200 bg-zinc-100/80 pl-10 pr-10 text-sm outline-none transition focus:border-violet-400 focus:bg-white focus:ring-2 focus:ring-violet-100"
            />
            <kbd className="absolute right-3 top-2 rounded border border-zinc-300 bg-white px-1.5 py-0.5 text-[10px] text-zinc-400">
              ⌘K
            </kbd>
          </div>
          <div className="hidden h-8 w-8 shrink-0 items-center justify-center rounded-full bg-gradient-to-br from-violet-500 to-fuchsia-500 text-xs font-bold text-white sm:flex">
            TK
          </div>
        </header>

        {unauthorized ? (
          <DevelopmentSignIn
            pending={sessionMutation.isPending}
            onStart={() => sessionMutation.mutate()}
          />
        ) : (
          <div className="flex min-h-0 flex-1">
            <section className="flex min-w-0 flex-1 flex-col">
              <div className="flex flex-wrap items-end justify-between gap-4 border-b border-zinc-100 px-5 py-5 md:px-8">
                <div>
                  <div className="mb-1 flex items-center gap-2 text-xs font-medium text-violet-600">
                    <span className="h-1.5 w-1.5 rounded-full bg-violet-500" />
                    TASKS
                  </div>
                  <h1 className="text-2xl font-bold tracking-tight">
                    {viewCopy[view].title}
                  </h1>
                  <p className="mt-1 text-sm text-zinc-500">
                    {viewCopy[view].subtitle}
                  </p>
                </div>
                <label className="flex items-center gap-2 text-xs font-medium text-zinc-500">
                  並び順
                  <select
                    value={sort}
                    onChange={(event) =>
                      setSort(event.target.value as "updated_at" | "due")
                    }
                    className="rounded-lg border border-zinc-200 bg-white px-3 py-2 text-sm text-zinc-700 outline-none focus:border-violet-400"
                  >
                    <option value="due">期限が近い順</option>
                    <option value="updated_at">更新が新しい順</option>
                  </select>
                </label>
              </div>

              {view !== "done" && (
                <QuickAdd
                  pending={createMutation.isPending}
                  onCreate={(input) => createMutation.mutate(input)}
                />
              )}

              <div className="min-h-0 flex-1 overflow-y-auto px-4 pb-10 md:px-8">
                {todosQuery.isPending ? (
                  <TaskSkeleton />
                ) : todosQuery.isError ? (
                  <RequestError
                    message={errorMessage(todosQuery.error)}
                    onRetry={() => void todosQuery.refetch()}
                  />
                ) : visibleTodos.length === 0 ? (
                  <EmptyState view={view} hasSearch={Boolean(search)} />
                ) : (
                  <div className="mx-auto max-w-4xl divide-y divide-zinc-100 border-t border-zinc-100">
                    {visibleTodos.map((todo) => (
                      <TaskRow
                        key={todo.id}
                        todo={todo}
                        selected={selectedID === todo.id}
                        pending={updateMutation.isPending}
                        onSelect={() => setSelectedID(todo.id)}
                        onToggle={() =>
                          updateMutation.mutate({
                            id: todo.id,
                            input: {
                              expected_version: todo.version,
                              status: todo.status === "open" ? "done" : "open",
                            },
                          })
                        }
                      />
                    ))}
                  </div>
                )}
              </div>
            </section>

            {selectedTodo && (
              <TaskDetails
                todo={selectedTodo}
                saving={updateMutation.isPending}
                deleting={deleteMutation.isPending}
                onClose={() => setSelectedID(null)}
                onSave={(input) =>
                  updateMutation.mutate({ id: selectedTodo.id, input })
                }
                onDelete={() => {
                  if (
                    window.confirm(
                      `「${selectedTodo.title}」を完全に削除しますか？`,
                    )
                  ) {
                    deleteMutation.mutate({
                      id: selectedTodo.id,
                      version: selectedTodo.version,
                    });
                  }
                }}
              />
            )}
          </div>
        )}

        {notice && (
          <div
            role="status"
            className="absolute bottom-5 left-1/2 z-50 flex -translate-x-1/2 items-center gap-2 rounded-lg bg-zinc-900 px-4 py-2.5 text-sm text-white shadow-xl"
          >
            <Icon name="sparkles" size={16} />
            {notice}
            <button
              type="button"
              aria-label="通知を閉じる"
              className="ml-2 text-zinc-400 hover:text-white"
              onClick={() => setNotice(null)}
            >
              ×
            </button>
          </div>
        )}
      </main>
    </div>
  );
}

function WorkspaceRail() {
  return (
    <aside className="hidden w-[68px] shrink-0 flex-col items-center bg-[#2f1646] py-3 text-white md:flex">
      <div className="mb-5 flex h-10 w-10 items-center justify-center rounded-xl bg-white text-base font-black text-[#4a215f] shadow-sm">
        S
      </div>
      <div
        aria-hidden="true"
        className="relative mb-2 flex h-11 w-11 items-center justify-center rounded-xl bg-white text-[#4a215f] shadow"
      >
        <Icon name="checkSquare" size={21} />
        <span className="absolute -left-3 h-7 w-1 rounded-r bg-white" />
      </div>
      <div className="mt-auto flex h-9 w-9 items-center justify-center rounded-full bg-gradient-to-br from-violet-400 to-fuchsia-400 text-[11px] font-bold">
        TK
      </div>
    </aside>
  );
}

function TodoSidebar({
  view,
  summary,
  open,
  onClose,
  onSelect,
}: {
  view: View;
  summary: ReturnType<typeof summarize>;
  open: boolean;
  onClose: () => void;
  onSelect: (view: View) => void;
}) {
  const items: Array<{
    id: View;
    label: string;
    icon: IconName;
    count?: number;
  }> = [
    { id: "inbox", label: "My tasks", icon: "inbox", count: summary.open },
    { id: "today", label: "Today", icon: "sun", count: summary.today },
    { id: "upcoming", label: "Upcoming", icon: "calendar" },
    {
      id: "overdue",
      label: "Overdue",
      icon: "alert",
      count: summary.overdue,
    },
    { id: "done", label: "Completed", icon: "check", count: summary.done },
  ];
  return (
    <>
      {open && (
        <button
          type="button"
          aria-label="ナビゲーションを閉じる"
          className="fixed inset-0 z-30 bg-black/25 md:hidden"
          onClick={onClose}
        />
      )}
      <aside
        className={`${
          open ? "translate-x-0" : "-translate-x-full"
        } fixed inset-y-0 left-0 z-40 flex w-64 shrink-0 flex-col border-r border-[#ded6e3] bg-[#f4eff6] transition-transform md:static md:translate-x-0`}
      >
        <div className="flex h-14 items-center justify-between border-b border-[#ded6e3] px-4">
          <div className="min-w-0">
            <div className="flex items-center gap-1.5 font-bold text-[#32123f]">
              Todo workspace
              <Icon name="chevronDown" size={14} />
            </div>
            <p className="truncate text-[11px] text-[#715c78]">
              Personal tasks
            </p>
          </div>
          <button
            type="button"
            aria-label="ナビゲーションを閉じる"
            className="rounded p-1 text-zinc-500 hover:bg-black/5 md:hidden"
            onClick={onClose}
          >
            <Icon name="close" />
          </button>
        </div>
        <nav className="p-3">
          <p className="mb-2 px-2 text-[11px] font-bold uppercase tracking-[0.14em] text-[#806a86]">
            Tasks
          </p>
          <div className="space-y-0.5">
            {items.map((item) => (
              <button
                key={item.id}
                type="button"
                onClick={() => onSelect(item.id)}
                className={`flex w-full items-center gap-2.5 rounded-md px-2.5 py-2 text-left text-sm transition ${
                  item.id === view
                    ? "bg-[#4a215f] font-medium text-white shadow-sm"
                    : "text-[#49364f] hover:bg-[#e8dfec]"
                }`}
              >
                <Icon name={item.icon} size={17} />
                <span className="flex-1">{item.label}</span>
                {item.count !== undefined && item.count > 0 && (
                  <span
                    className={`rounded-full px-2 py-0.5 text-[11px] ${
                      item.id === view ? "bg-white/15" : "bg-[#e3d8e8]"
                    }`}
                  >
                    {item.count}
                  </span>
                )}
              </button>
            ))}
          </div>
        </nav>
        <div className="mt-auto m-3 rounded-lg border border-[#ddd0e2] bg-white/70 p-3">
          <div className="flex items-start gap-2">
            <span className="mt-0.5 text-violet-600">
              <Icon name="sparkles" size={16} />
            </span>
            <div>
              <p className="text-xs font-semibold text-[#49364f]">
                Sumiからの操作
              </p>
              <p className="mt-1 text-[11px] leading-4 text-[#715c78]">
                Sumi経由で更新されたTodoには「Sumi」マークが表示されます。
              </p>
            </div>
          </div>
        </div>
      </aside>
    </>
  );
}

function QuickAdd({
  pending,
  onCreate,
}: {
  pending: boolean;
  onCreate: (input: Parameters<typeof createTodo>[0]) => void;
}) {
  const [title, setTitle] = useState("");
  const [expanded, setExpanded] = useState(false);
  const [priority, setPriority] = useState<TodoPriority>("none");
  const [dueKind, setDueKind] = useState<DueKind>("none");
  const [dueValue, setDueValue] = useState("");

  const submit = () => {
    const cleanTitle = title.trim();
    if (!cleanTitle) return;
    const due = buildDue(dueKind, dueValue);
    if (dueKind !== "none" && !due) return;
    onCreate({ title: cleanTitle, priority, due });
    setTitle("");
    setPriority("none");
    setDueKind("none");
    setDueValue("");
    setExpanded(false);
  };

  return (
    <div className="px-4 py-4 md:px-8">
      <form
        className="mx-auto max-w-4xl rounded-xl border border-zinc-200 bg-white shadow-[0_2px_12px_rgba(63,35,76,0.06)] transition focus-within:border-violet-300 focus-within:shadow-[0_4px_18px_rgba(91,53,112,0.12)]"
        onSubmit={(event) => {
          event.preventDefault();
          submit();
        }}
      >
        <div className="flex items-center gap-3 px-4 py-3">
          <span className="flex h-7 w-7 shrink-0 items-center justify-center rounded-lg bg-violet-100 text-violet-700">
            <Icon name="plus" size={17} />
          </span>
          <input
            value={title}
            onChange={(event) => setTitle(event.target.value)}
            onFocus={() => setExpanded(true)}
            maxLength={200}
            placeholder="Todoを追加する…"
            className="min-w-0 flex-1 bg-transparent text-sm outline-none placeholder:text-zinc-400"
          />
          <button
            type="submit"
            disabled={!title.trim() || pending}
            className="rounded-lg bg-[#4a215f] px-3 py-1.5 text-xs font-semibold text-white disabled:opacity-35"
          >
            {pending ? "追加中…" : "追加"}
          </button>
        </div>
        {expanded && (
          <div className="flex flex-wrap items-center gap-2 border-t border-zinc-100 px-4 py-2.5">
            <select
              aria-label="優先度"
              value={priority}
              onChange={(event) =>
                setPriority(event.target.value as TodoPriority)
              }
              className="rounded-md border border-zinc-200 bg-zinc-50 px-2.5 py-1.5 text-xs text-zinc-600 outline-none"
            >
              {Object.entries(priorityCopy).map(([value, copy]) => (
                <option value={value} key={value}>
                  優先度: {copy.label}
                </option>
              ))}
            </select>
            <select
              aria-label="期限の種類"
              value={dueKind}
              onChange={(event) => {
                setDueKind(event.target.value as DueKind);
                setDueValue("");
              }}
              className="rounded-md border border-zinc-200 bg-zinc-50 px-2.5 py-1.5 text-xs text-zinc-600 outline-none"
            >
              <option value="none">期限なし</option>
              <option value="date">日付</option>
              <option value="datetime">日時</option>
            </select>
            {dueKind !== "none" && (
              <input
                aria-label="期限"
                required
                type={dueKind === "date" ? "date" : "datetime-local"}
                value={dueValue}
                onChange={(event) => setDueValue(event.target.value)}
                className="rounded-md border border-zinc-200 bg-zinc-50 px-2.5 py-1 text-xs text-zinc-600 outline-none"
              />
            )}
            <span className="ml-auto hidden text-[11px] text-zinc-400 sm:inline">
              Enterで追加
            </span>
          </div>
        )}
      </form>
    </div>
  );
}

function TaskRow({
  todo,
  selected,
  pending,
  onSelect,
  onToggle,
}: {
  todo: Todo;
  selected: boolean;
  pending: boolean;
  onSelect: () => void;
  onToggle: () => void;
}) {
  const overdue = isOverdue(todo);
  return (
    <article
      className={`flex items-start gap-3 px-2 py-4 transition hover:bg-violet-50/40 md:px-3 ${
        selected ? "bg-violet-50" : ""
      }`}
    >
      <button
        type="button"
        aria-label={todo.status === "open" ? "完了にする" : "未完了に戻す"}
        disabled={pending}
        onClick={(event) => {
          event.stopPropagation();
          onToggle();
        }}
        className={`mt-0.5 flex h-5 w-5 shrink-0 items-center justify-center rounded-md border transition disabled:opacity-50 ${
          todo.status === "done"
            ? "border-violet-600 bg-violet-600 text-white"
            : "border-zinc-300 bg-white text-transparent hover:border-violet-500 hover:text-violet-300"
        }`}
      >
        <Icon name="check" size={13} />
      </button>
      <button
        type="button"
        className="group min-w-0 flex-1 text-left"
        onClick={onSelect}
      >
        <div className="flex min-w-0 items-start justify-between gap-3">
          <span
            className={`truncate text-sm font-medium leading-5 ${
              todo.status === "done"
                ? "text-zinc-400 line-through"
                : "text-zinc-800"
            }`}
          >
            {todo.title}
          </span>
          <Icon
            name="chevronRight"
            size={16}
            className="mt-0.5 shrink-0 text-zinc-300 opacity-0 transition group-hover:opacity-100"
          />
        </div>
        {todo.description && (
          <p className="mt-0.5 truncate text-xs text-zinc-500">
            {todo.description}
          </p>
        )}
        <div className="mt-2 flex flex-wrap items-center gap-x-3 gap-y-1 text-[11px] text-zinc-500">
          {todo.due && (
            <span
              className={`flex items-center gap-1 ${
                overdue ? "font-semibold text-rose-600" : ""
              }`}
            >
              <Icon name={overdue ? "alert" : "calendar"} size={13} />
              {formatDue(todo)}
            </span>
          )}
          {todo.priority !== "none" && (
            <span
              className={`flex items-center gap-1 ${priorityCopy[todo.priority].badge}`}
            >
              <span
                className={`h-1.5 w-1.5 rounded-full ${priorityCopy[todo.priority].dot}`}
              />
              {priorityCopy[todo.priority].label}
            </span>
          )}
          {todo.via_agent && (
            <span className="flex items-center gap-1 text-violet-600">
              <Icon name="sparkles" size={12} /> Sumi
            </span>
          )}
        </div>
      </button>
    </article>
  );
}

function TaskDetails({
  todo,
  saving,
  deleting,
  onClose,
  onSave,
  onDelete,
}: {
  todo: Todo;
  saving: boolean;
  deleting: boolean;
  onClose: () => void;
  onSave: (input: Parameters<typeof updateTodo>[1]) => void;
  onDelete: () => void;
}) {
  const [title, setTitle] = useState(todo.title);
  const [description, setDescription] = useState(todo.description);
  const [priority, setPriority] = useState<TodoPriority>(todo.priority);
  const [dueKind, setDueKind] = useState<DueKind>(todo.due?.kind ?? "none");
  const [dueValue, setDueValue] = useState(dueInputValue(todo));

  useEffect(() => {
    setTitle(todo.title);
    setDescription(todo.description);
    setPriority(todo.priority);
    setDueKind(todo.due?.kind ?? "none");
    setDueValue(dueInputValue(todo));
  }, [todo]);

  const save = () => {
    const due = buildDue(dueKind, dueValue);
    if (!title.trim() || (dueKind !== "none" && !due)) return;
    onSave({
      expected_version: todo.version,
      title: title.trim(),
      description,
      priority,
      due,
    });
  };

  return (
    <aside className="absolute inset-y-0 right-0 z-20 flex w-full max-w-md flex-col border-l border-zinc-200 bg-white shadow-2xl md:static md:w-[380px] md:shadow-none xl:w-[420px]">
      <div className="flex h-14 shrink-0 items-center justify-between border-b border-zinc-200 px-4">
        <div className="flex items-center gap-2 text-sm font-semibold">
          <Icon name="checkSquare" size={17} />
          Todo details
        </div>
        <button
          type="button"
          aria-label="詳細を閉じる"
          className="rounded-lg p-2 text-zinc-500 hover:bg-zinc-100"
          onClick={onClose}
        >
          <Icon name="close" size={18} />
        </button>
      </div>
      <form
        className="min-h-0 flex-1 overflow-y-auto p-5"
        onSubmit={(event) => {
          event.preventDefault();
          save();
        }}
      >
        <label className="block">
          <span className="mb-1.5 block text-xs font-semibold text-zinc-500">
            タイトル
          </span>
          <input
            value={title}
            maxLength={200}
            onChange={(event) => setTitle(event.target.value)}
            className="w-full rounded-lg border border-zinc-200 px-3 py-2.5 text-sm font-medium outline-none focus:border-violet-400 focus:ring-2 focus:ring-violet-100"
          />
        </label>
        <label className="mt-5 block">
          <span className="mb-1.5 block text-xs font-semibold text-zinc-500">
            説明
          </span>
          <textarea
            value={description}
            onChange={(event) => setDescription(event.target.value)}
            placeholder="メモや詳細を追加…"
            rows={6}
            className="w-full resize-none rounded-lg border border-zinc-200 px-3 py-2.5 text-sm leading-6 outline-none focus:border-violet-400 focus:ring-2 focus:ring-violet-100"
          />
        </label>
        <div className="mt-5 grid grid-cols-2 gap-3">
          <label>
            <span className="mb-1.5 block text-xs font-semibold text-zinc-500">
              優先度
            </span>
            <select
              value={priority}
              onChange={(event) =>
                setPriority(event.target.value as TodoPriority)
              }
              className="w-full rounded-lg border border-zinc-200 bg-white px-3 py-2.5 text-sm outline-none focus:border-violet-400"
            >
              {Object.entries(priorityCopy).map(([value, copy]) => (
                <option value={value} key={value}>
                  {copy.label}
                </option>
              ))}
            </select>
          </label>
          <label>
            <span className="mb-1.5 block text-xs font-semibold text-zinc-500">
              期限
            </span>
            <select
              value={dueKind}
              onChange={(event) => {
                setDueKind(event.target.value as DueKind);
                setDueValue("");
              }}
              className="w-full rounded-lg border border-zinc-200 bg-white px-3 py-2.5 text-sm outline-none focus:border-violet-400"
            >
              <option value="none">なし</option>
              <option value="date">日付</option>
              <option value="datetime">日時</option>
            </select>
          </label>
        </div>
        {dueKind !== "none" && (
          <label className="mt-3 block">
            <span className="sr-only">期限の値</span>
            <input
              required
              type={dueKind === "date" ? "date" : "datetime-local"}
              value={dueValue}
              onChange={(event) => setDueValue(event.target.value)}
              className="w-full rounded-lg border border-zinc-200 px-3 py-2.5 text-sm outline-none focus:border-violet-400"
            />
          </label>
        )}

        <dl className="mt-7 space-y-3 border-t border-zinc-100 pt-5 text-xs">
          <div className="flex justify-between gap-4">
            <dt className="text-zinc-400">状態</dt>
            <dd className="font-medium text-zinc-600">
              {todo.status === "open" ? "未完了" : "完了"}
            </dd>
          </div>
          <div className="flex justify-between gap-4">
            <dt className="text-zinc-400">更新</dt>
            <dd className="text-zinc-600">
              {formatTimestamp(todo.updated_at)}
            </dd>
          </div>
          <div className="flex justify-between gap-4">
            <dt className="text-zinc-400">Version</dt>
            <dd className="font-mono text-zinc-600">v{todo.version}</dd>
          </div>
        </dl>

        <div className="mt-7 flex gap-2">
          <button
            type="submit"
            disabled={!title.trim() || saving}
            className="flex-1 rounded-lg bg-[#4a215f] px-4 py-2.5 text-sm font-semibold text-white hover:bg-[#391849] disabled:opacity-40"
          >
            {saving ? "保存中…" : "変更を保存"}
          </button>
          <button
            type="button"
            aria-label="Todoを削除"
            disabled={deleting}
            onClick={onDelete}
            className="rounded-lg border border-zinc-200 px-3 text-zinc-400 hover:border-rose-200 hover:bg-rose-50 hover:text-rose-600 disabled:opacity-40"
          >
            <Icon name="trash" size={17} />
          </button>
        </div>
      </form>
    </aside>
  );
}

function DevelopmentSignIn({
  pending,
  onStart,
}: {
  pending: boolean;
  onStart: () => void;
}) {
  return (
    <div className="flex flex-1 items-center justify-center p-6">
      <div className="w-full max-w-md rounded-2xl border border-zinc-200 bg-white p-8 text-center shadow-sm">
        <div className="mx-auto flex h-12 w-12 items-center justify-center rounded-2xl bg-violet-100 text-violet-700">
          <Icon name="lock" size={22} />
        </div>
        <h1 className="mt-5 text-xl font-bold">セッションが必要です</h1>
        <p className="mt-2 text-sm leading-6 text-zinc-500">
          ComposeのローカルユーザーとしてTodoを試します。このボタンは開発環境でのみ有効です。
        </p>
        <button
          type="button"
          onClick={onStart}
          disabled={pending}
          className="mt-6 w-full rounded-lg bg-[#4a215f] px-4 py-2.5 text-sm font-semibold text-white disabled:opacity-50"
        >
          {pending ? "開始中…" : "ローカルセッションを開始"}
        </button>
      </div>
    </div>
  );
}

function EmptyState({ view, hasSearch }: { view: View; hasSearch: boolean }) {
  return (
    <div className="mx-auto mt-16 max-w-sm text-center">
      <div className="mx-auto flex h-12 w-12 items-center justify-center rounded-2xl bg-violet-50 text-violet-500">
        <Icon
          name={hasSearch ? "search" : view === "done" ? "check" : "sparkles"}
          size={22}
        />
      </div>
      <p className="mt-4 text-sm font-semibold text-zinc-700">
        {hasSearch ? "一致するTodoはありません" : "ここは空です"}
      </p>
      <p className="mt-1 text-xs leading-5 text-zinc-400">
        {hasSearch
          ? "検索語を変えてもう一度試してください。"
          : view === "done"
            ? "完了したTodoがここに並びます。"
            : "上の入力欄から最初のTodoを追加しましょう。"}
      </p>
    </div>
  );
}

function RequestError({
  message,
  onRetry,
}: {
  message: string;
  onRetry: () => void;
}) {
  return (
    <div className="mx-auto mt-12 max-w-md rounded-xl border border-rose-100 bg-rose-50 p-5 text-center">
      <p className="text-sm font-semibold text-rose-700">
        Todoを読み込めませんでした
      </p>
      <p className="mt-1 text-xs text-rose-600">{message}</p>
      <button
        type="button"
        onClick={onRetry}
        className="mt-4 rounded-lg bg-white px-3 py-2 text-xs font-semibold text-rose-700 shadow-sm"
      >
        再試行
      </button>
    </div>
  );
}

function TaskSkeleton() {
  return (
    <div className="mx-auto max-w-4xl animate-pulse space-y-1 border-t border-zinc-100">
      {[0, 1, 2, 3].map((item) => (
        <div className="flex gap-3 px-3 py-4" key={item}>
          <span className="h-5 w-5 rounded bg-zinc-100" />
          <div className="flex-1">
            <div className="h-4 w-2/3 rounded bg-zinc-100" />
            <div className="mt-2 h-3 w-1/4 rounded bg-zinc-100" />
          </div>
        </div>
      ))}
    </div>
  );
}

function summarize(items: Todo[]) {
  return {
    open: items.filter((todo) => todo.status === "open").length,
    today: items.filter((todo) => todo.status === "open" && isDueToday(todo))
      .length,
    overdue: items.filter((todo) => todo.status === "open" && isOverdue(todo))
      .length,
    done: items.filter((todo) => todo.status === "done").length,
  };
}

function buildDue(kind: DueKind, value: string): TodoDueInput | null {
  if (kind === "none") return null;
  if (!value) return null;
  const timezone =
    Intl.DateTimeFormat().resolvedOptions().timeZone || "Asia/Tokyo";
  if (kind === "date") return { kind: "date", date: value, timezone };
  const date = new Date(value);
  if (Number.isNaN(date.valueOf())) return null;
  return { kind: "datetime", at: date.toISOString(), timezone };
}

function dueInputValue(todo: Todo) {
  if (!todo.due) return "";
  if (todo.due.kind === "date") return todo.due.date;
  const date = new Date(todo.due.at);
  const offset = date.getTimezoneOffset() * 60_000;
  return new Date(date.valueOf() - offset).toISOString().slice(0, 16);
}

function zonedDate(timezone: string) {
  const parts = new Intl.DateTimeFormat("en-CA", {
    timeZone: timezone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).formatToParts();
  const value = Object.fromEntries(
    parts.map((part) => [part.type, part.value]),
  );
  return `${value.year}-${value.month}-${value.day}`;
}

function isDueToday(todo: Todo) {
  if (!todo.due) return false;
  if (todo.due.kind === "date")
    return todo.due.date === zonedDate(todo.due.timezone);
  return (
    zonedDate(todo.due.timezone) ===
    zonedDateForInstant(todo.due.at, todo.due.timezone)
  );
}

function zonedDateForInstant(instant: string, timezone: string) {
  const parts = new Intl.DateTimeFormat("en-CA", {
    timeZone: timezone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).formatToParts(new Date(instant));
  const value = Object.fromEntries(
    parts.map((part) => [part.type, part.value]),
  );
  return `${value.year}-${value.month}-${value.day}`;
}

function isOverdue(todo: Todo) {
  if (todo.status === "done" || !todo.due) return false;
  if (todo.due.kind === "datetime")
    return new Date(todo.due.at).valueOf() < Date.now();
  return zonedDate(todo.due.timezone) > todo.due.date;
}

function formatDue(todo: Todo) {
  if (!todo.due) return "";
  if (todo.due.kind === "date") {
    const [year, month, day] = todo.due.date.split("-").map(Number);
    return new Intl.DateTimeFormat("ja-JP", {
      month: "short",
      day: "numeric",
      weekday: "short",
    }).format(new Date(Date.UTC(year, month - 1, day)));
  }
  return new Intl.DateTimeFormat("ja-JP", {
    timeZone: todo.due.timezone,
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  }).format(new Date(todo.due.at));
}

function formatTimestamp(value: string) {
  return new Intl.DateTimeFormat("ja-JP", {
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  }).format(new Date(value));
}

function errorMessage(error: unknown) {
  if (error instanceof TodoAPIError) {
    if (error.code === "version_conflict")
      return `別の更新が先に保存されました（現在 v${error.currentVersion ?? "?"}）。最新状態を読み直しました。`;
    return error.message;
  }
  return error instanceof Error
    ? error.message
    : "予期しないエラーが発生しました";
}

type IconName =
  | "alert"
  | "calendar"
  | "check"
  | "checkSquare"
  | "chevronDown"
  | "chevronRight"
  | "close"
  | "inbox"
  | "lock"
  | "menu"
  | "plus"
  | "search"
  | "sparkles"
  | "sun"
  | "trash";

function Icon({
  name,
  size = 18,
  className,
}: {
  name: IconName;
  size?: number;
  className?: string;
}) {
  const paths: Record<IconName, React.ReactNode> = {
    alert: (
      <>
        <path d="M10.3 2.8 2.2 17a2 2 0 0 0 1.7 3h16.2a2 2 0 0 0 1.7-3L13.7 2.8a2 2 0 0 0-3.4 0Z" />
        <path d="M12 9v4M12 17h.01" />
      </>
    ),
    calendar: (
      <>
        <rect width="18" height="18" x="3" y="4" rx="2" />
        <path d="M16 2v4M8 2v4M3 10h18" />
      </>
    ),
    check: <path d="m5 12 4 4L19 6" />,
    checkSquare: (
      <>
        <rect width="18" height="18" x="3" y="3" rx="3" />
        <path d="m7.5 12 3 3 6-7" />
      </>
    ),
    chevronDown: <path d="m7 10 5 5 5-5" />,
    chevronRight: <path d="m9 18 6-6-6-6" />,
    close: <path d="M18 6 6 18M6 6l12 12" />,
    inbox: (
      <>
        <path d="M4 4h16v13a3 3 0 0 1-3 3H7a3 3 0 0 1-3-3V4Z" />
        <path d="M4 13h4l2 3h4l2-3h4" />
      </>
    ),
    lock: (
      <>
        <rect width="16" height="12" x="4" y="10" rx="2" />
        <path d="M8 10V7a4 4 0 0 1 8 0v3" />
      </>
    ),
    menu: <path d="M4 7h16M4 12h16M4 17h16" />,
    plus: <path d="M12 5v14M5 12h14" />,
    search: (
      <>
        <circle cx="11" cy="11" r="7" />
        <path d="m20 20-4-4" />
      </>
    ),
    sparkles: (
      <>
        <path d="m12 3-1.2 3.2L8 7.5l2.8 1.3L12 12l1.2-3.2L16 7.5l-2.8-1.3L12 3Z" />
        <path d="m18 14-.7 1.8-1.8.7 1.8.7L18 19l.7-1.8 1.8-.7-1.8-.7L18 14ZM5 12l-.9 2.1L2 15l2.1.9L5 18l.9-2.1L8 15l-2.1-.9L5 12Z" />
      </>
    ),
    sun: (
      <>
        <circle cx="12" cy="12" r="4" />
        <path d="M12 2v2M12 20v2M4.93 4.93l1.42 1.42M17.66 17.66l1.41 1.41M2 12h2M20 12h2M4.93 19.07l1.42-1.41M17.66 6.34l1.41-1.41" />
      </>
    ),
    trash: (
      <>
        <path d="M4 7h16M10 11v6M14 11v6M6 7l1 14h10l1-14M9 7V4h6v3" />
      </>
    ),
  };
  return (
    <svg
      aria-hidden="true"
      width={size}
      height={size}
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.8"
      strokeLinecap="round"
      strokeLinejoin="round"
      className={className}
    >
      {paths[name]}
    </svg>
  );
}
