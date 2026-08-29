/**
 * クリップボードへの書き込み。
 *
 * navigator.clipboard は https でないoriginや権限拒否、フォーカスを失った
 * タイミングで例外を投げる。UIは「コピーしました」を出す前に成否を知る
 * 必要があるので、必ず boolean を返す形にし、使えない環境では選択+
 * execCommand の古い経路へ落とす（deprecatedだが現状の代替がない）。
 */
export async function copyText(text: string): Promise<boolean> {
  try {
    if (typeof navigator !== "undefined" && navigator.clipboard?.writeText) {
      await navigator.clipboard.writeText(text);
      return true;
    }
  } catch {
    // 権限拒否・非セキュアorigin。下の経路を試す。
  }
  return copyWithSelection(text);
}

function copyWithSelection(text: string): boolean {
  if (typeof document === "undefined") return false;
  const holder = document.createElement("textarea");
  holder.value = text;
  holder.setAttribute("readonly", "");
  holder.setAttribute("aria-hidden", "true");
  // 画面外へ置く。display:none だと選択できない。
  holder.style.position = "fixed";
  holder.style.top = "-1000px";
  holder.style.opacity = "0";
  document.body.appendChild(holder);
  try {
    holder.select();
    holder.setSelectionRange(0, text.length);
    return document.execCommand("copy");
  } catch {
    return false;
  } finally {
    holder.remove();
  }
}
