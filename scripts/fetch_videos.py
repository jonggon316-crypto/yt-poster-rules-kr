"""
fetch_videos.py
Jordan Peterson Rules for Life YouTube 채널의 새 영상을
인생의 규칙 (Firebase Firestore rules-posts-kr 컬렉션)에 한국어로 자동 포스팅.
Claude AI가 각 영상의 핵심 내용을 한국어 에세이로 변환합니다.
"""

import os
import re
import sys
import json
import datetime
import xml.etree.ElementTree as ET
import urllib.request
import urllib.error

# ── 설정 ─────────────────────────────────────────────────────────────
CHANNEL_ID    = "UC5ddsy9laN8sGjV1kTkvHAw"
RSS_URL       = f"https://www.youtube.com/feeds/videos.xml?channel_id={CHANNEL_ID}"
PROJECT_ID    = "healing-space-dbfb2"
FIRESTORE_URL = f"https://firestore.googleapis.com/v1/projects/{PROJECT_ID}/databases/(default)/documents/rules-posts-kr"
POSTED_FILE   = os.path.join(os.path.dirname(__file__), "..", "posted_ids.json")
MAX_NEW_POSTS = 5

# ── 설명 정제 ────────────────────────────────────────────────────────
JUNK_CONTAINS = [
    "dailywire", "dwplus", "feedlink.io", "preborn.com",
    "petersonacademy.com", "arcforum.com", "linktr.ee",
    "instagram.com", "twitter.com", "x.com/", "facebook.com", "tiktok.com",
    "would you like to join", "share a question here",
    "unlock the ad-free", "ad-free experience", "start watching now",
    "exclusive bonus content", "all links", "// links",
    "jordanbpeterson.com/books", "subscribe", "patreon.com",
]
JUNK_SECTION_RE = [
    re.compile(r"^\|\s*sponsors?\s*\|$", re.I),
    re.compile(r"^//\s*(links?|social|connect)\s*//$", re.I),
]
CHAPTER_SECTION_RE = [
    re.compile(r"^\|\s*chapters?\s*\|$", re.I),
    re.compile(r"^chapters?:?\s*$", re.I),
    re.compile(r"^episode\s+chapters?:?\s*$", re.I),
]
TIMESTAMP_RE = re.compile(r"^\(?(\d+:\d{2}(?::\d{2})?)\)?\s+(.+)")


def extract_description(raw: str) -> tuple[str, list[str]]:
    if not raw:
        return "", []

    lines = raw.strip().split("\n")
    content_lines: list[str] = []
    chapter_titles: list[str] = []

    in_junk    = False
    in_chapter = False

    for line in lines:
        s = line.strip()
        lower = s.lower()

        if any(r.match(lower) for r in CHAPTER_SECTION_RE):
            in_junk = False; in_chapter = True; continue
        if any(r.match(lower) for r in JUNK_SECTION_RE):
            in_junk = True; in_chapter = False; continue

        if in_chapter:
            if not s:
                continue
            m = TIMESTAMP_RE.match(s)
            if m:
                title = m.group(2).strip()
                if title.lower() != "intro":
                    chapter_titles.append(title)
            continue

        if in_junk:
            continue
        if not s:
            content_lines.append(""); continue

        skip = (
            (s.startswith("(") and len(s) > 80)
            or all(w.startswith("#") for w in s.split() if w)
            or bool(re.match(r"^Ep\.?\s*\d+", s, re.I))
            or bool(re.match(r"^https?://", s))
            or any(kw in lower for kw in JUNK_CONTAINS)
        )
        if not skip:
            content_lines.append(s)

    merged: list[str] = []
    prev_blank = False
    for l in content_lines:
        if l == "":
            if not prev_blank and merged:
                merged.append("")
            prev_blank = True
        else:
            merged.append(l); prev_blank = False
    while merged and merged[-1] == "":
        merged.pop()

    return "\n".join(merged), chapter_titles


# ── Claude AI 한국어 콘텐츠 생성 ──────────────────────────────────────
SYSTEM_PROMPT = """당신은 "인생의 규칙" — 조던 B. 피터슨의 핵심 가르침을 다루는 한국어 웹 저널을 위해 글을 씁니다.

피터슨의 목소리는:
- 긴박하고 도덕적으로 진지함 — 그는 진정으로 중요한 것처럼 말한다
- 직접적이고 대립적이며, 독자를 "당신"으로 부르며 요구한다
- 열정적이고 강렬함 — 반복하고, 주장하고, 밀어붙인다
- 진화심리학, 융의 원형, 성경 서사에 근거함
- 고통에 솔직함 — 부드럽게 다루거나 이유 없이 낙관하지 않음
- 목록이나 글머리 기호 없이 — 항상 흐르는 산문, 단락 후 단락

반드시 자연스럽고 유창한 한국어로 작성하세요."""


def generate_content_with_claude(title: str, description: str,
                                  chapters: list[str], pub_date_str: str) -> str:
    api_key = os.environ.get("ANTHROPIC_API_KEY", "").strip()

    if not api_key:
        return build_fallback_content(description, chapters, pub_date_str)

    chapter_block = ""
    if chapters:
        chapter_block = "\n\n다루는 주제:\n" + "\n".join(
            f"— {c}" for c in chapters
        )

    user_prompt = f"""이 조던 B. 피터슨 영상 클립에 대해 피터슨 본인의 목소리와 어조로 한국어 에세이를 써주세요.

영상 제목: {title}
설명: {description}{chapter_block}

2~3개의 집중된 단락으로 피터슨이 이 영상에서 말하는 핵심을 담아주세요. 포함할 내용:

1. 피터슨이 다루는 중심 규칙이나 진리 — 그가 가져오는 긴박감으로 서술
2. 그가 사용하는 심리적 또는 진화론적 틀 (융, 성경, 랍스터 위계 등)
3. 독자에 대한 실질적인 요구: 무엇을 해야 하는가, 그리고 하지 않을 경우의 대가

흐르는 단락으로 작성하고, 헤더나 글머리 기호 없이. 직접적인 요구를 할 때는 "당신"으로 독자를 지칭하세요.

중요: 첫 번째 단락의 첫 단어로 바로 시작하세요. 제목이나 헤더를 추가하지 마세요.
중요: 모든 문장은 문법적으로 완전해야 합니다. 마침표로 끝내세요."""

    body = json.dumps({
        "model":      "claude-haiku-4-5-20251001",
        "max_tokens": 600,
        "system":     SYSTEM_PROMPT,
        "messages":   [{"role": "user", "content": user_prompt}],
    }).encode()

    req = urllib.request.Request(
        "https://api.anthropic.com/v1/messages",
        data=body,
        headers={
            "Content-Type":    "application/json",
            "x-api-key":       api_key,
            "anthropic-version": "2023-06-01",
        },
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=45) as resp:
            result = json.loads(resp.read())
            essay  = result["content"][0]["text"].strip()

            essay_lines = [l for l in essay.split("\n") if not l.strip().startswith("#")]
            essay = "\n".join(essay_lines).strip()

            if essay and essay[-1] not in ('.', '!', '?', '»', '"'):
                last_end = max(essay.rfind("."), essay.rfind("!"), essay.rfind("?"))
                if last_end > len(essay) // 2:
                    essay = essay[:last_end + 1].strip()

            print(f"[ai] ✅ content generated ({len(essay)} chars)")
            return essay + f"\n\n—\n\n조던 B. 피터슨  ·  {pub_date_str}"
    except Exception as e:
        print(f"[ai] ⚠️  Claude call failed: {e}  — using fallback")
        return build_fallback_content(description, chapters, pub_date_str)


def build_fallback_content(description: str, chapters: list[str], pub_date_str: str) -> str:
    parts: list[str] = []
    if description:
        parts.append(description)
    if chapters:
        if parts:
            parts.append("")
        for c in chapters:
            parts.append(f"  · {c}")
    if parts:
        parts.append("")
    parts.append(f"조던 B. 피터슨  ·  {pub_date_str}")
    return "\n".join(parts)


# ── 포스팅 기록 관리 ──────────────────────────────────────────────────
def load_posted_ids() -> set:
    if os.path.exists(POSTED_FILE):
        with open(POSTED_FILE, "r", encoding="utf-8") as f:
            return set(json.load(f).get("posted_ids", []))
    return set()

def save_posted_ids(ids: set):
    with open(POSTED_FILE, "w", encoding="utf-8") as f:
        json.dump({"posted_ids": sorted(ids)}, f, indent=2, ensure_ascii=False)


# ── YouTube RSS 파싱 ──────────────────────────────────────────────────
def fetch_rss_videos() -> list[dict]:
    print(f"[fetch] {RSS_URL}")
    req = urllib.request.Request(RSS_URL, headers={"User-Agent": "Mozilla/5.0"})
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            raw = resp.read()
    except urllib.error.HTTPError as e:
        print(f"[fetch] ⚠️  YouTube RSS 일시 오류 ({e.code}) — 다음 실행에 재시도합니다.")
        return []
    except Exception as e:
        print(f"[fetch] ⚠️  RSS 요청 실패: {e} — 다음 실행에 재시도합니다.")
        return []

    root = ET.fromstring(raw)
    ns = {
        "atom":  "http://www.w3.org/2005/Atom",
        "yt":    "http://www.youtube.com/xml/schemas/2015",
        "media": "http://search.yahoo.com/mrss/",
    }

    videos = []
    for entry in root.findall("atom:entry", ns):
        video_id  = entry.findtext("yt:videoId",  namespaces=ns) or ""
        title     = entry.findtext("atom:title",   namespaces=ns) or ""
        published = entry.findtext("atom:published", namespaces=ns) or ""
        raw_desc  = ""
        group = entry.find("media:group", ns)
        if group is not None:
            el = group.find("media:description", ns)
            if el is not None and el.text:
                raw_desc = el.text
        videos.append({"videoId": video_id, "title": title,
                       "published": published, "rawDesc": raw_desc})

    print(f"[fetch] {len(videos)} videos found")
    return videos


# ── Firestore REST API 포스팅 ─────────────────────────────────────────
def post_to_firestore(video: dict, firebase_key: str) -> bool:
    vid       = video["videoId"]
    title     = video["title"]
    published = video["published"]
    yt_url    = f"https://www.youtube.com/watch?v={vid}"

    try:
        pub_dt       = datetime.datetime.fromisoformat(published.replace("Z", "+00:00"))
        order        = -int(pub_dt.timestamp())
        pub_date_str = pub_dt.strftime("%Y년 %m월 %d일")
    except Exception:
        order = 0; pub_date_str = published[:10]

    description, chapters = extract_description(video["rawDesc"])
    content = generate_content_with_claude(title, description, chapters, pub_date_str)

    document = {
        "fields": {
            "title":       {"stringValue": title},
            "content":     {"stringValue": content},
            "image":       {"stringValue": f"https://img.youtube.com/vi/{vid}/maxresdefault.jpg"},
            "youtubeId":   {"stringValue": vid},
            "youtubeUrl":  {"stringValue": yt_url},
            "source":      {"stringValue": "youtube"},
            "order":       {"integerValue": str(order)},
            "createdAt":   {"timestampValue": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")},
            "publishedAt": {"stringValue": published},
        }
    }

    url  = f"{FIRESTORE_URL}?key={firebase_key}"
    body = json.dumps(document).encode("utf-8")
    req  = urllib.request.Request(
        url, data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            print(f"[post] ✅  [{resp.status}] {title[:72]}")
            return True
    except urllib.error.HTTPError as e:
        print(f"[post] ❌  {e.code}: {e.read().decode()[:200]}")
        return False


# ── 메인 ─────────────────────────────────────────────────────────────
def main():
    firebase_key = os.environ.get("FIREBASE_API_KEY", "").strip()
    if not firebase_key:
        print("[error] FIREBASE_API_KEY not set.")
        sys.exit(1)

    posted_ids = load_posted_ids()
    print(f"[info] already posted: {len(posted_ids)}")

    videos   = fetch_rss_videos()
    if not videos:
        print("[info] No videos fetched. Done.")
        return

    new_vids = [v for v in videos if v["videoId"] not in posted_ids]
    print(f"[info] new videos: {len(new_vids)}")

    if not new_vids:
        print("[info] Nothing new. Done.")
        return

    new_vids = list(reversed(new_vids))[:MAX_NEW_POSTS]

    ok_count = 0
    for v in new_vids:
        if post_to_firestore(v, firebase_key):
            posted_ids.add(v["videoId"])
            ok_count += 1

    save_posted_ids(posted_ids)
    print(f"[done] {ok_count}/{len(new_vids)} posted.")


if __name__ == "__main__":
    main()
