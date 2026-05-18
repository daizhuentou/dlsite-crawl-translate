import asyncio
import sys
import time
from datetime import datetime

import aiohttp

from generate import (
    ASMR_SUBTITLE_MAX_RETRIES,
    ASMR_SUBTITLE_RETRY_DELAY,
    HEADERS,
    WORKS_DIR,
    WORK_KIND_AUDIO_ASMR,
    build_asmr_subtitle_api_url,
    get_cached_asmr_subtitle,
    has_valid_asmr_subtitle_result,
    load_asmr_subtitle_cache,
    parse_html_file,
    save_asmr_subtitle_cache,
    set_cached_asmr_subtitle,
)


DEFAULT_CONCURRENCY = 3
OFFICIAL_SUBTITLED_WORKS_CONCURRENCY = 3
OFFICIAL_SUBTITLED_WORKS_PAGE_SIZE = 20
OFFICIAL_SUBTITLED_WORKS_API_TEMPLATE = (
    "https://api.asmr-200.com/api/works"
    "?order=create_date&sort=desc&page={page}&pageSize={page_size}&subtitle=1"
)
OFFICIAL_SUBTITLED_WORKS_ITEM_ID_KEY = "works_api_item_id"
OFFICIAL_SUBTITLED_WORKS_SYNCED_AT_KEY = "works_api_synced_at"
AUTO_CONCURRENCY = "auto"
AUTO_MIN_CONCURRENCY = 1
AUTO_MAX_CONCURRENCY = 8
AUTO_START_CONCURRENCY = 2
AUTO_WINDOW_SIZE = 20
RATE_LIMIT_MIN_WAIT = 10
AUTO_RATE_LIMIT_COOLDOWN_WINDOWS = 3
AUTO_RAMP_UP_CLEAN_WINDOWS = 3


class ProgressBar:
    def __init__(self, total, label, width=32):
        self.total = max(1, total)
        self.label = label
        self.width = width
        self.current = 0
        self.start_time = time.time()
        self.last_line_length = 0
        self.fill_char, self.empty_char = self.get_bar_chars()

    @staticmethod
    def get_bar_chars():
        encoding = sys.stdout.encoding or "utf-8"
        try:
            "█░".encode(encoding)
            return "█", "░"
        except UnicodeEncodeError:
            return "#", "-"

    def update(self, current, subtitled, unsubtitled, failed, rate_limited=0):
        self.current = current
        elapsed = max(0.001, time.time() - self.start_time)
        speed = current / elapsed
        remaining = max(0, self.total - current)
        eta = remaining / speed if speed > 0 else 0
        ratio = min(1, current / self.total)
        filled = int(self.width * ratio)
        bar = self.fill_char * filled + self.empty_char * (self.width - filled)
        line = (
            f"\r{self.label}: |{bar}| {current}/{self.total} "
            f"{ratio * 100:5.1f}% "
            f"有字幕:{subtitled} 无字幕:{unsubtitled} 失败:{failed} 429:{rate_limited} "
            f"{speed:.2f}/s ETA:{format_duration(eta)}"
        )
        padding = " " * max(0, self.last_line_length - len(line))
        print(line + padding, end="", flush=True)
        self.last_line_length = len(line)

    def message(self, text):
        if self.last_line_length:
            print("\r" + " " * self.last_line_length + "\r", end="", flush=True)
            self.last_line_length = 0
        print(text, flush=True)

    def close(self):
        print()


def format_duration(seconds):
    seconds = int(max(0, seconds))
    if seconds < 60:
        return f"{seconds}s"
    minutes, seconds = divmod(seconds, 60)
    if minutes < 60:
        return f"{minutes}m{seconds}s"
    hours, minutes = divmod(minutes, 60)
    return f"{hours}h{minutes}m"


def read_input(prompt):
    try:
        return input(prompt)
    except EOFError:
        return ""


def prompt_choice():
    print("请选择要执行的功能：")
    print("  1. 补查未确认的音声 ASMR")
    print("  2. 复查已标为无字幕的音声 ASMR")
    print("  3. 同步官方有字幕作品目录")
    while True:
        value = read_input("输入 1、2 或 3: ").strip()
        if value in ("1", "2", "3"):
            return value
        if not value:
            print("未选择功能，已取消。")
            return ""
        print("输入无效，请输入 1、2 或 3。")


def prompt_int(label, default, minimum=1, allow_auto=False):
    auto_hint = "，输入 auto=自动调整" if allow_auto else ""
    value = read_input(f"{label}(留空={default}{auto_hint}): ").strip()
    if allow_auto and value.lower() == AUTO_CONCURRENCY:
        return AUTO_CONCURRENCY
    if not value:
        return default
    try:
        number = int(value)
    except ValueError:
        print(f"输入无效，使用默认值 {default}")
        return default
    if number < minimum:
        print(f"不能小于 {minimum}，使用默认值 {default}")
        return default
    return number


def get_retry_wait_seconds(retry_after, attempt):
    fallback = ASMR_SUBTITLE_RETRY_DELAY * attempt
    try:
        value = float(retry_after) if retry_after else fallback
    except ValueError:
        value = fallback
    return max(RATE_LIMIT_MIN_WAIT, value)


def build_official_subtitled_works_url(page, page_size=OFFICIAL_SUBTITLED_WORKS_PAGE_SIZE):
    return OFFICIAL_SUBTITLED_WORKS_API_TEMPLATE.format(page=page, page_size=page_size)


def normalize_work_id(value):
    if not isinstance(value, str):
        return ""
    work_id = value.strip().upper()
    if len(work_id) < 3:
        return ""
    if not (work_id.startswith("RJ") or work_id.startswith("VJ")):
        return ""
    if not work_id[2:].isdigit():
        return ""
    return work_id


def extract_catalog_related_work_ids(work):
    work_ids = []
    seen = set()

    def add_work_id(value):
        work_id = normalize_work_id(value)
        if not work_id or work_id in seen:
            return
        seen.add(work_id)
        work_ids.append(work_id)

    add_work_id(work.get("source_id"))
    add_work_id(work.get("original_workno"))

    translation_info = work.get("translation_info")
    if isinstance(translation_info, dict):
        add_work_id(translation_info.get("parent_workno"))
        add_work_id(translation_info.get("original_workno"))
        for child_workno in translation_info.get("child_worknos", []):
            add_work_id(child_workno)

    for edition in work.get("language_editions", []):
        if isinstance(edition, dict):
            add_work_id(edition.get("workno"))
            add_work_id(edition.get("source_id"))

    for edition in work.get("other_language_editions_in_db", []):
        if isinstance(edition, dict):
            add_work_id(edition.get("workno"))
            add_work_id(edition.get("source_id"))

    return work_ids


def get_catalog_primary_work_id(work):
    primary_work_id = normalize_work_id(work.get("source_id"))
    if primary_work_id:
        return primary_work_id

    related_ids = extract_catalog_related_work_ids(work)
    return related_ids[0] if related_ids else ""


def is_catalog_work_already_synced(cache, work):
    primary_work_id = get_catalog_primary_work_id(work)
    if not primary_work_id:
        return False

    entry = cache.get(primary_work_id)
    if not isinstance(entry, dict):
        return False

    current_item_id = work.get("id")
    if current_item_id is None:
        return False

    return str(entry.get(OFFICIAL_SUBTITLED_WORKS_ITEM_ID_KEY, "")) == str(current_item_id)


def mark_catalog_work_as_subtitled(cache, work):
    checked_at = datetime.now().isoformat(timespec="seconds")
    primary_work_id = get_catalog_primary_work_id(work)
    related_work_ids = extract_catalog_related_work_ids(work)
    if primary_work_id and primary_work_id not in related_work_ids:
        related_work_ids.insert(0, primary_work_id)

    cache_writes = 0
    new_cache_entries = 0
    item_id = str(work.get("id", ""))
    source_url = work.get("source_url") if isinstance(work.get("source_url"), str) else ""
    create_date = work.get("create_date") if isinstance(work.get("create_date"), str) else ""

    for work_id in related_work_ids:
        existing_entry = cache.get(work_id)
        if isinstance(existing_entry, dict):
            entry = dict(existing_entry)
        else:
            entry = {}
            new_cache_entries += 1

        before = dict(entry)
        entry["has_subtitle"] = True
        entry["status"] = "ok"
        entry["checked_at"] = checked_at
        entry[OFFICIAL_SUBTITLED_WORKS_SYNCED_AT_KEY] = checked_at
        if create_date:
            entry["works_api_create_date"] = create_date
        if work_id == primary_work_id and item_id:
            entry[OFFICIAL_SUBTITLED_WORKS_ITEM_ID_KEY] = item_id
        if work_id == primary_work_id and source_url:
            entry["works_api_source_url"] = source_url

        if entry != before:
            cache[work_id] = entry
            cache_writes += 1
        elif work_id not in cache:
            cache[work_id] = entry

    return {
        "primary_work_id": primary_work_id,
        "related_work_ids": related_work_ids,
        "new_cache_entries": new_cache_entries,
        "cache_writes": cache_writes,
    }


def collect_audio_asmr_ids():
    work_ids = []
    html_files = sorted(WORKS_DIR.glob("RJ*.html"))
    total = len(html_files)

    for idx, path in enumerate(html_files, start=1):
        if idx % 1000 == 0:
            print(f"扫描作品 HTML: {idx}/{total}")
        try:
            work = parse_html_file(path)
        except Exception as e:
            print(f"  跳过解析失败: {path.name} - {e}")
            continue
        if work.get("work_kind") == WORK_KIND_AUDIO_ASMR:
            work_ids.append(work["product_id"])

    return work_ids


async def query_subtitle_api(session, work_id, log=print, on_rate_limit=None):
    url = build_asmr_subtitle_api_url(work_id)
    saw_rate_limit = False
    notified_rate_limit = False
    for attempt in range(1, ASMR_SUBTITLE_MAX_RETRIES + 1):
        try:
            async with session.get(url, timeout=20) as resp:
                if resp.status == 429 and attempt < ASMR_SUBTITLE_MAX_RETRIES:
                    saw_rate_limit = True
                    if on_rate_limit and not notified_rate_limit:
                        on_rate_limit()
                        notified_rate_limit = True
                    retry_after = resp.headers.get("Retry-After")
                    wait_seconds = get_retry_wait_seconds(retry_after, attempt)
                    log(f"  字幕 API 限流: {work_id}，{wait_seconds:.1f} 秒后重试")
                    await asyncio.sleep(wait_seconds)
                    continue

                if resp.status != 200:
                    log(f"  字幕 API 状态异常: {work_id} HTTP {resp.status}")
                    return None, saw_rate_limit

                data = await resp.json(content_type=None)
                return has_valid_asmr_subtitle_result(data, work_id), saw_rate_limit
        except Exception as e:
            if attempt < ASMR_SUBTITLE_MAX_RETRIES:
                wait_seconds = ASMR_SUBTITLE_RETRY_DELAY * attempt
                log(f"  字幕 API 查询失败: {work_id} - {e}，{wait_seconds:.1f} 秒后重试")
                await asyncio.sleep(wait_seconds)
                continue
            log(f"  字幕 API 查询失败: {work_id} - {e}")
            return None, saw_rate_limit

    return None, saw_rate_limit


async def fetch_official_subtitled_works_page(session, page, log=print, on_rate_limit=None):
    url = build_official_subtitled_works_url(page)
    saw_rate_limit = False
    notified_rate_limit = False
    for attempt in range(1, ASMR_SUBTITLE_MAX_RETRIES + 1):
        try:
            async with session.get(url, timeout=20) as resp:
                if resp.status == 429 and attempt < ASMR_SUBTITLE_MAX_RETRIES:
                    saw_rate_limit = True
                    if on_rate_limit and not notified_rate_limit:
                        on_rate_limit()
                        notified_rate_limit = True
                    wait_seconds = get_retry_wait_seconds(resp.headers.get("Retry-After"), attempt)
                    log(f"  官方字幕目录 API 限流: 第 {page} 页，{wait_seconds:.1f} 秒后重试")
                    await asyncio.sleep(wait_seconds)
                    continue

                if resp.status != 200:
                    log(f"  官方字幕目录 API 状态异常: 第 {page} 页 HTTP {resp.status}")
                    return None, saw_rate_limit

                data = await resp.json(content_type=None)
                works = data.get("works")
                if not isinstance(works, list):
                    log(f"  官方字幕目录 API 返回异常: 第 {page} 页缺少 works 列表")
                    return None, saw_rate_limit
                return works, saw_rate_limit
        except Exception as e:
            if attempt < ASMR_SUBTITLE_MAX_RETRIES:
                wait_seconds = ASMR_SUBTITLE_RETRY_DELAY * attempt
                log(f"  官方字幕目录 API 查询失败: 第 {page} 页 - {e}，{wait_seconds:.1f} 秒后重试")
                await asyncio.sleep(wait_seconds)
                continue
            log(f"  官方字幕目录 API 查询失败: 第 {page} 页 - {e}")
            return None, saw_rate_limit

    return None, saw_rate_limit


async def update_targets(target_ids, label, concurrency):
    cache = load_asmr_subtitle_cache()
    auto_mode = concurrency == AUTO_CONCURRENCY
    target_concurrency = AUTO_START_CONCURRENCY if auto_mode else concurrency
    target_concurrency = max(AUTO_MIN_CONCURRENCY, min(AUTO_MAX_CONCURRENCY, target_concurrency))
    active_tasks = set()
    pending_ids = list(target_ids)
    completed = 0
    changed_to_subtitled = 0
    confirmed_unsubtitled = 0
    failed = 0
    rate_limited = 0
    window_completed = 0
    window_rate_limited = 0
    window_failed = 0
    cooldown_windows = 0
    clean_windows = 0
    progress = ProgressBar(len(target_ids), label)
    started_at = time.time()

    def log_message(message):
        progress.message(message)

    def adjust_for_rate_limit():
        nonlocal target_concurrency, cooldown_windows, clean_windows
        if not auto_mode:
            return
        old = target_concurrency
        target_concurrency = max(AUTO_MIN_CONCURRENCY, target_concurrency - 1)
        cooldown_windows = AUTO_RATE_LIMIT_COOLDOWN_WINDOWS
        clean_windows = 0
        if target_concurrency != old:
            log_message(f"  自动并发调整: {old} -> {target_concurrency}（检测到 429，进入冷却）")

    def tune_auto_concurrency():
        nonlocal target_concurrency, window_completed, window_rate_limited, window_failed
        nonlocal cooldown_windows, clean_windows
        if not auto_mode or window_completed < AUTO_WINDOW_SIZE:
            return

        old = target_concurrency
        if window_rate_limited > 0:
            cooldown_windows = AUTO_RATE_LIMIT_COOLDOWN_WINDOWS
            clean_windows = 0
        elif window_failed >= max(3, AUTO_WINDOW_SIZE // 4):
            target_concurrency = max(AUTO_MIN_CONCURRENCY, target_concurrency - 1)
            cooldown_windows = max(cooldown_windows, 1)
            clean_windows = 0
        elif window_failed == 0 and window_rate_limited == 0:
            if cooldown_windows > 0:
                cooldown_windows -= 1
            else:
                clean_windows += 1
                if clean_windows >= AUTO_RAMP_UP_CLEAN_WINDOWS:
                    target_concurrency = min(AUTO_MAX_CONCURRENCY, target_concurrency + 1)
                    clean_windows = 0
        else:
            clean_windows = 0

        if target_concurrency != old:
            log_message(f"  自动并发调整: {old} -> {target_concurrency}")

        window_completed = 0
        window_rate_limited = 0
        window_failed = 0

    async with aiohttp.ClientSession(headers=HEADERS) as session:
        async def worker(work_id):
            on_rate_limit = adjust_for_rate_limit if auto_mode else None
            return work_id, await query_subtitle_api(session, work_id, log_message, on_rate_limit)

        while pending_ids or active_tasks:
            while pending_ids and len(active_tasks) < target_concurrency:
                active_tasks.add(asyncio.create_task(worker(pending_ids.pop(0))))

            done, active_tasks = await asyncio.wait(active_tasks, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                work_id, (result, saw_rate_limit) = await task
                completed += 1
                window_completed += 1

                if saw_rate_limit:
                    rate_limited += 1
                    window_rate_limited += 1

                if result is None:
                    failed += 1
                    window_failed += 1
                else:
                    set_cached_asmr_subtitle(cache, work_id, result)
                    if result:
                        changed_to_subtitled += 1
                    else:
                        confirmed_unsubtitled += 1

                if completed % 20 == 0 or completed == len(target_ids):
                    save_asmr_subtitle_cache(cache)
                progress.update(completed, changed_to_subtitled, confirmed_unsubtitled, failed, rate_limited)
                tune_auto_concurrency()

    save_asmr_subtitle_cache(cache)
    progress.close()
    elapsed = time.time() - started_at
    return {
        "processed": completed,
        "subtitled": changed_to_subtitled,
        "unsubtitled": confirmed_unsubtitled,
        "failed": failed,
        "rate_limited": rate_limited,
        "elapsed": elapsed,
        "speed": completed / elapsed if elapsed > 0 else 0,
        "auto_mode": auto_mode,
        "final_concurrency": target_concurrency,
    }


async def sync_official_subtitled_works():
    cache = load_asmr_subtitle_cache()
    started_at = time.time()
    page = 1
    pages_fetched = 0
    works_seen = 0
    new_primary_works = 0
    new_cache_entries = 0
    cache_writes = 0
    skipped_without_work_id = 0
    rate_limited = 0
    failed_pages = 0
    stop_reason = ""

    def log_message(message):
        print(message, flush=True)

    def note_rate_limit():
        nonlocal rate_limited
        rate_limited += 1

    async with aiohttp.ClientSession(headers=HEADERS) as session:
        while True:
            page_batch = list(range(page, page + OFFICIAL_SUBTITLED_WORKS_CONCURRENCY))
            results = await asyncio.gather(
                *(
                    fetch_official_subtitled_works_page(
                        session,
                        page_number,
                        log=log_message,
                        on_rate_limit=note_rate_limit,
                    )
                    for page_number in page_batch
                )
            )

            reached_end = False

            for page_number, (works, _saw_rate_limit) in zip(page_batch, results):
                if works is None:
                    failed_pages += 1
                    stop_reason = f"第 {page_number} 页连续重试后仍失败"
                    reached_end = True
                    break

                if not works:
                    stop_reason = f"第 {page_number} 页开始没有更多作品"
                    reached_end = True
                    break

                pages_fetched += 1

                for work in works:
                    primary_work_id = get_catalog_primary_work_id(work)
                    if not primary_work_id:
                        skipped_without_work_id += 1
                        continue

                    if is_catalog_work_already_synced(cache, work):
                        stop_reason = f"第 {page_number} 页遇到已同步作品 {primary_work_id}"
                        reached_end = True
                        break

                    works_seen += 1
                    update_result = mark_catalog_work_as_subtitled(cache, work)
                    new_primary_works += 1
                    new_cache_entries += update_result["new_cache_entries"]
                    cache_writes += update_result["cache_writes"]

                if pages_fetched % 5 == 0:
                    save_asmr_subtitle_cache(cache)

                print(
                    f"同步官方有字幕目录: 第 {page_number} 页 "
                    f"(新作品 {new_primary_works}, 新缓存项 {new_cache_entries}, "
                    f"写入 {cache_writes}, 429 {rate_limited})",
                    flush=True,
                )

                if reached_end:
                    break

            if reached_end:
                break

            page += OFFICIAL_SUBTITLED_WORKS_CONCURRENCY

    save_asmr_subtitle_cache(cache)
    elapsed = time.time() - started_at
    return {
        "pages_fetched": pages_fetched,
        "works_seen": works_seen,
        "new_primary_works": new_primary_works,
        "new_cache_entries": new_cache_entries,
        "cache_writes": cache_writes,
        "skipped_without_work_id": skipped_without_work_id,
        "rate_limited": rate_limited,
        "failed_pages": failed_pages,
        "elapsed": elapsed,
        "speed": works_seen / elapsed if elapsed > 0 else 0,
        "stop_reason": stop_reason,
    }


async def main():
    choice = prompt_choice()
    if not choice:
        return 0
    concurrency = prompt_int("并发查询数", DEFAULT_CONCURRENCY, allow_auto=True)
    limit = prompt_int("最多处理多少个作品，0=全部", 0, minimum=0)

    cache = load_asmr_subtitle_cache()
    audio_ids = collect_audio_asmr_ids()
    print(f"本地音声 ASMR: {len(audio_ids)} 个")

    if choice == "1":
        target_ids = [work_id for work_id in audio_ids if get_cached_asmr_subtitle(cache, work_id) is None]
        label = "补查未确认"
    else:
        target_ids = [work_id for work_id in audio_ids if get_cached_asmr_subtitle(cache, work_id) is False]
        label = "复查无字幕"

    if limit > 0:
        target_ids = target_ids[:limit]

    print(f"{label}目标: {len(target_ids)} 个")
    if not target_ids:
        print("没有需要处理的作品。")
        return 0



    summary = await update_targets(
        target_ids,
        label,
        concurrency,
    )
    print("\n总结：")
    print(f"  - 执行功能: {label}")
    print(f"  - 本次处理: {summary['processed']} / {len(target_ids)}")
    print(f"  - 更新为有字幕: {summary['subtitled']}")
    print(f"  - 确认为无字幕: {summary['unsubtitled']}")
    print(f"  - 查询失败/仍被限流: {summary['failed']}")
    print(f"  - 遇到 429 限流: {summary['rate_limited']}")
    if summary["auto_mode"]:
        print(f"  - 自动并发最终值: {summary['final_concurrency']}")
    print(f"  - 耗时: {format_duration(summary['elapsed'])}")
    print(f"  - 平均速度: {summary['speed']:.2f} 个/秒")
    print("下一步运行 python generate.py 重新生成网页，作品类型会根据缓存更新。")
    return 0


async def main():
    choice = prompt_choice()
    if not choice:
        return 0

    if choice == "3":
        summary = await sync_official_subtitled_works()
        print("\n总结：")
        print("  - 执行功能: 同步官方有字幕作品目录")
        print(f"  - 并发页数: {OFFICIAL_SUBTITLED_WORKS_CONCURRENCY}")
        print(f"  - 抓取页数: {summary['pages_fetched']}")
        print(f"  - 新同步作品: {summary['new_primary_works']}")
        print(f"  - 新增缓存项: {summary['new_cache_entries']}")
        print(f"  - 缓存写入次数: {summary['cache_writes']}")
        print(f"  - 缺少可用 RJ/VJ 的条目: {summary['skipped_without_work_id']}")
        print(f"  - 遇到 429 限流: {summary['rate_limited']}")
        print(f"  - 失败页数: {summary['failed_pages']}")
        if summary["stop_reason"]:
            print(f"  - 停止原因: {summary['stop_reason']}")
        print(f"  - 耗时: {format_duration(summary['elapsed'])}")
        print(f"  - 平均同步速度: {summary['speed']:.2f} 个作品/秒")
        print("下一步运行 python generate.py，网页里的 ASMR 字幕类型会按最新缓存刷新。")
        return 0

    concurrency = prompt_int("并发查询数", DEFAULT_CONCURRENCY, allow_auto=True)
    limit = prompt_int("最多处理多少个作品？0=全部", 0, minimum=0)

    cache = load_asmr_subtitle_cache()
    audio_ids = collect_audio_asmr_ids()
    print(f"本地音声 ASMR: {len(audio_ids)} 个")

    if choice == "1":
        target_ids = [work_id for work_id in audio_ids if get_cached_asmr_subtitle(cache, work_id) is None]
        label = "补查未确认"
    else:
        target_ids = [work_id for work_id in audio_ids if get_cached_asmr_subtitle(cache, work_id) is False]
        label = "复查无字幕"

    if limit > 0:
        target_ids = target_ids[:limit]

    print(f"{label}目标: {len(target_ids)} 个")
    if not target_ids:
        print("没有需要处理的作品。")
        return 0

    summary = await update_targets(
        target_ids,
        label,
        concurrency,
    )
    print("\n总结：")
    print(f"  - 执行功能: {label}")
    print(f"  - 本次处理: {summary['processed']} / {len(target_ids)}")
    print(f"  - 更新为有字幕: {summary['subtitled']}")
    print(f"  - 确认为无字幕: {summary['unsubtitled']}")
    print(f"  - 查询失败/仍被限流: {summary['failed']}")
    print(f"  - 遇到 429 限流: {summary['rate_limited']}")
    if summary["auto_mode"]:
        print(f"  - 自动并发最终值: {summary['final_concurrency']}")
    print(f"  - 耗时: {format_duration(summary['elapsed'])}")
    print(f"  - 平均速度: {summary['speed']:.2f} 个/秒")
    print("下一步运行 python generate.py 重新生成网页，作品类型会根据缓存更新。")
    return 0


if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    raise SystemExit(asyncio.run(main()))
