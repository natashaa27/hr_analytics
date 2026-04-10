"""
Reddit Finance & Payments Scraper
----------------------------------
Scrapes posts from finance and payments-related subreddits where users
are asking for help, insights, or advice about personal finances / payment apps.

Usage:
    python reddit_finance_scraper.py              # live scrape (needs network)
    python reddit_finance_scraper.py --sample     # generate mock data (no network)

Authentication (optional — enables higher rate limits via PRAW):
    export REDDIT_CLIENT_ID=<your_app_id>
    export REDDIT_CLIENT_SECRET=<your_app_secret>

    Create a Reddit app at: https://www.reddit.com/prefs/apps
    (choose "script" type, no redirect URI needed for read-only use)
"""

import os
import sys
import time
import random
import requests
import pandas as pd
from datetime import datetime, timezone, timedelta


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

TARGET_SUBREDDITS = [
    "personalfinance",
    "povertyfinance",
    "financialindependence",
    "CashApp",
    "venmo",
    "paypal",
    "banking",
    "creditcards",
    "Debt",
    "frugal",
    "PaymentProcessing",
    "fintech",
]

# Keywords that signal the user is seeking help / insights
HELP_KEYWORDS = [
    "help", "advice", "question", "how do i", "how to", "can someone",
    "confused", "struggling", "need help", "not sure", "anyone know",
    "what should", "should i", "recommend", "suggestion", "insight",
    "understand", "explain", "tip", "guide", "newbie", "beginner",
    "first time", "lost", "unsure", "problem", "issue", "trouble",
    "can't figure", "cannot figure", "track", "budget", "overspending",
    "fees", "charge", "transfer", "payment failed", "declined",
    "interest rate", "debt", "save money", "spending",
]

POSTS_PER_SUBREDDIT = 100   # max posts to fetch per subreddit (per sort)
REQUEST_DELAY      = 1.5    # seconds between requests (be polite to the API)
OUTPUT_CSV         = "reddit_finance_posts.csv"

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; finance-research-bot/1.0)"}


# ---------------------------------------------------------------------------
# Reddit Public JSON API
# ---------------------------------------------------------------------------

def fetch_posts_public(subreddit: str, sort: str = "hot", limit: int = 100) -> list[dict]:
    """Fetch posts via Reddit's public JSON endpoint (no auth required)."""
    url = f"https://www.reddit.com/r/{subreddit}/{sort}.json"
    params = {"limit": min(limit, 100), "raw_json": 1}
    posts, after = [], None

    while len(posts) < limit:
        if after:
            params["after"] = after
        try:
            resp = requests.get(url, headers=HEADERS, params=params, timeout=15)
            if resp.status_code == 429:
                print(f"  [rate-limited] waiting 10 s …")
                time.sleep(10)
                continue
            if resp.status_code != 200:
                print(f"  [warning] r/{subreddit} returned HTTP {resp.status_code}")
                break
            data = resp.json()
        except Exception as e:
            print(f"  [error] r/{subreddit}: {e}")
            break

        children = data.get("data", {}).get("children", [])
        if not children:
            break

        for child in children:
            posts.append(child.get("data", {}))

        after = data.get("data", {}).get("after")
        if not after or len(children) < 25:
            break

        time.sleep(REQUEST_DELAY)

    return posts


def fetch_posts_praw(subreddit: str, limit: int = 100) -> list[dict]:
    """Fetch posts via PRAW (requires env vars REDDIT_CLIENT_ID / SECRET)."""
    import praw
    reddit = praw.Reddit(
        client_id=os.environ["REDDIT_CLIENT_ID"],
        client_secret=os.environ["REDDIT_CLIENT_SECRET"],
        user_agent="finance-research-bot/1.0",
    )
    sub = reddit.subreddit(subreddit)
    posts = []
    for s in sub.hot(limit=limit):
        posts.append({
            "id": s.id, "title": s.title, "selftext": s.selftext,
            "score": s.score, "upvote_ratio": s.upvote_ratio,
            "num_comments": s.num_comments, "created_utc": s.created_utc,
            "permalink": s.permalink, "author": str(s.author),
            "is_self": s.is_self, "link_flair_text": s.link_flair_text,
        })
    return posts


# ---------------------------------------------------------------------------
# Filtering & normalisation
# ---------------------------------------------------------------------------

def is_help_seeking(title: str, body: str) -> bool:
    combined = (title + " " + body).lower()
    return any(kw in combined for kw in HELP_KEYWORDS)


def normalize_post(raw: dict, subreddit: str) -> dict | None:
    title = raw.get("title", "").strip()
    body  = raw.get("selftext", "").strip()
    if not title:
        return None
    if body in ("[removed]", "[deleted]"):
        body = ""
    if not is_help_seeking(title, body):
        return None

    created_utc = raw.get("created_utc", 0)
    try:
        created_dt = datetime.fromtimestamp(created_utc, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        created_dt = ""

    return {
        "subreddit":    subreddit,
        "post_id":      raw.get("id", ""),
        "title":        title,
        "body":         body[:2000],
        "score":        raw.get("score", 0),
        "upvote_ratio": raw.get("upvote_ratio"),
        "num_comments": raw.get("num_comments", 0),
        "created_utc":  created_dt,
        "author":       raw.get("author", "[deleted]"),
        "url":          "https://www.reddit.com" + raw.get("permalink", ""),
        "flair":        raw.get("link_flair_text", ""),
        "is_self_post": raw.get("is_self", True),
    }


# ---------------------------------------------------------------------------
# Live scrape
# ---------------------------------------------------------------------------

def scrape(use_praw: bool = False) -> pd.DataFrame:
    records, total_fetched = [], 0

    for subreddit in TARGET_SUBREDDITS:
        print(f"\nScraping r/{subreddit} …")

        if use_praw:
            raw_posts = fetch_posts_praw(subreddit, limit=POSTS_PER_SUBREDDIT)
            matched = [p for p in raw_posts if is_help_seeking(p.get("title", ""), p.get("selftext", ""))]
            records.extend(matched)
            print(f"  fetched {len(raw_posts)} → {len(matched)} matched")
        else:
            for sort in ("hot", "new"):
                raw_posts = fetch_posts_public(subreddit, sort=sort, limit=POSTS_PER_SUBREDDIT)
                matched = 0
                for raw in raw_posts:
                    record = normalize_post(raw, subreddit)
                    if record:
                        records.append(record)
                        matched += 1
                total_fetched += len(raw_posts)
                print(f"  [{sort}] fetched {len(raw_posts)} → {matched} matched")
                time.sleep(REQUEST_DELAY)

    return _build_df(records, total_fetched)


# ---------------------------------------------------------------------------
# Sample / mock data (for environments without Reddit network access)
# ---------------------------------------------------------------------------

MOCK_POSTS = [
    # personalfinance
    ("personalfinance", "Need help understanding my first paycheck deductions",
     "Just started my first job and noticed a huge chunk taken out. Can someone explain what FICA, federal withholding, and state taxes are? I have no idea how to budget now.", 1842, 0.97, 143),
    ("personalfinance", "How do I start budgeting? Completely lost",
     "I'm 23 and have been spending everything I earn. I want to save but don't know where to begin. Any tips or apps you'd recommend for tracking spending?", 3201, 0.98, 289),
    ("personalfinance", "Should I pay off debt or build an emergency fund first?",
     "I have $8k in credit card debt at 22% APR and $500 in savings. I keep getting conflicting advice. What should I prioritize?", 5644, 0.96, 512),
    ("personalfinance", "Payment failed on my rent — bank declined the ACH transfer, help!",
     "My landlord tried to pull rent via ACH and it was declined even though I have the money. Not sure if it's a bank issue or what. Anyone know how to fix this fast?", 743, 0.91, 98),
    ("personalfinance", "Any advice for someone drowning in student loan debt?",
     "I owe $78k at 6.8% and make $45k/year. I can barely afford groceries. Is income-driven repayment worth it? I'm confused about all the plan options.", 9120, 0.95, 876),

    # povertyfinance
    ("povertyfinance", "Need help — transfer to cash app failed and I can't pay my electric bill",
     "I tried sending money to my mom via Cash App but it keeps saying transfer failed. She needs to pay her electric bill today. Any idea why this happens?", 1203, 0.94, 167),
    ("povertyfinance", "How do you guys track spending when you're living paycheck to paycheck?",
     "I'm making $1,800/month and bills eat almost all of it. Looking for insights on how to even start saving anything. Every budgeting app I try feels like it's for people who already have money.", 4532, 0.97, 398),
    ("povertyfinance", "Is there any way to avoid the fees on Venmo and Cash App?",
     "I'm getting charged every time I transfer to my bank account. These fees are killing me. Is there a way around it or a better app to use?", 2187, 0.93, 234),

    # CashApp
    ("CashApp", "Cash App keeps declining my payment — need help",
     "I've tried sending $50 three times and it keeps getting declined. My balance is fine. Is this a known issue? Anyone else having this problem today?", 876, 0.88, 132),
    ("CashApp", "How do I get a refund from Cash App? Someone charged me twice",
     "A vendor charged me twice for the same order. I need the money back ASAP. Does anyone know the process to dispute a charge?", 1543, 0.90, 201),
    ("CashApp", "Understanding Cash App fees — can someone explain the 1.5% instant transfer fee?",
     "I just noticed I'm being charged 1.5% every time I send to my bank instantly. Is there a way to avoid this? Seems like a lot.", 988, 0.92, 145),

    # venmo
    ("venmo", "Venmo payment pending for 3 days — not sure what to do",
     "I sent $200 to a friend and it's been pending for 3 days. They say they didn't receive it. Should I be worried? Can Venmo hold payments?", 1123, 0.89, 178),
    ("venmo", "Need advice: Venmo froze my account right before I needed to pay rent",
     "Venmo randomly froze my account and I have $600 in there I need for rent. I submitted ID verification but no response. Anyone dealt with this?", 3210, 0.93, 445),

    # paypal
    ("paypal", "PayPal charged me a fee I didn't expect — can someone explain goods & services vs friends?",
     "I sold something on Facebook Marketplace and the buyer sent via goods & services. PayPal took 3.49%. Is that normal? How do I avoid this?", 2045, 0.91, 267),
    ("paypal", "How do I dispute a PayPal charge? Seller sent wrong item",
     "Ordered electronics, got something completely different. I opened a dispute but I'm confused about the process. Any tips to make sure I win?", 1876, 0.94, 312),

    # creditcards
    ("creditcards", "First credit card — confused about utilization, need advice",
     "Just got my first card with a $500 limit. People say to keep utilization under 30% but I'm not sure if that means each month or overall. Any insights?", 4231, 0.97, 489),
    ("creditcards", "Help understanding my credit card statement — minimum payment vs full balance",
     "The minimum payment is $25 but the full balance is $340. If I pay the minimum am I OK? I don't understand how interest works.", 6543, 0.96, 678),
    ("creditcards", "Should I close my old credit cards? Need advice",
     "I have 3 cards I don't use. Someone told me closing them hurts my score, others say it's fine. I'm confused about what to do.", 3102, 0.95, 356),

    # banking
    ("banking", "Bank declined my international wire transfer — any advice?",
     "Trying to send money to family overseas but my bank keeps declining. They say it's a compliance hold. Has anyone dealt with this? Any tips?", 987, 0.88, 134),
    ("banking", "How do I understand my bank's overdraft fees? I keep getting hit",
     "Every month I get a $35 overdraft fee at least once. I thought I had enough. Is there any way to avoid these or set up better alerts?", 2341, 0.92, 287),

    # fintech
    ("fintech", "Which budgeting app actually gives useful insights? Need recommendations",
     "I've tried Mint, YNAB, and Copilot. Mint is dead, YNAB is confusing, Copilot seems promising. Any insights on what actually helps people control spending?", 1876, 0.93, 223),
    ("fintech", "Are neobanks safe? Need advice before switching from traditional bank",
     "Thinking of switching to Chime or Revolut. I'm not sure how safe they are compared to a regular bank. Can someone explain FDIC insurance for these?", 2987, 0.95, 367),
]


def generate_sample_data() -> pd.DataFrame:
    """Generate realistic mock Reddit posts for demonstration / offline use."""
    random.seed(42)
    base_time = datetime.now(tz=timezone.utc) - timedelta(days=30)
    records = []

    for i, (sub, title, body, score, ratio, comments) in enumerate(MOCK_POSTS):
        created = base_time + timedelta(hours=i * 7 + random.randint(0, 6))
        records.append({
            "subreddit":    sub,
            "post_id":      f"mock_{i:04d}",
            "title":        title,
            "body":         body,
            "score":        score,
            "upvote_ratio": ratio,
            "num_comments": comments,
            "created_utc":  created.strftime("%Y-%m-%d %H:%M:%S"),
            "author":       f"user_{random.randint(10000, 99999)}",
            "url":          f"https://www.reddit.com/r/{sub}/comments/mock_{i:04d}/",
            "flair":        "",
            "is_self_post": True,
        })

    df = pd.DataFrame(records).sort_values("score", ascending=False).reset_index(drop=True)
    print(f"\n[Sample mode] Generated {len(df)} mock posts across {df['subreddit'].nunique()} subreddits.")
    return df


# ---------------------------------------------------------------------------
# Summary & save
# ---------------------------------------------------------------------------

def print_summary(df: pd.DataFrame, total_fetched: int = 0) -> None:
    print(f"\n{'='*60}")
    if total_fetched:
        print(f"Total posts scraped  : {total_fetched}")
    print(f"Help-seeking posts   : {len(df)}")
    print(f"Subreddits covered   : {df['subreddit'].nunique()}")
    print(f"{'='*60}")
    print("\nPost count by subreddit:")
    print(df["subreddit"].value_counts().to_string())
    print("\nTop 10 posts by score:")
    cols = ["subreddit", "score", "num_comments", "title"]
    print(df[cols].head(10).to_string(index=False, max_colwidth=65))


def save(df: pd.DataFrame, path: str = OUTPUT_CSV) -> None:
    df.to_csv(path, index=False)
    print(f"\nSaved {len(df)} posts → '{path}'")


def _build_df(records: list[dict], total_fetched: int = 0) -> pd.DataFrame:
    if not records:
        print("\nNo matching posts found.")
        return pd.DataFrame()
    df = pd.DataFrame(records)
    df = df.drop_duplicates(subset="post_id", keep="first")
    df = df.sort_values("score", ascending=False).reset_index(drop=True)
    print_summary(df, total_fetched)
    return df


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    sample_mode = "--sample" in sys.argv

    if sample_mode:
        print("Running in sample mode — generating mock data (no network needed).")
        df = generate_sample_data()
        print_summary(df)
    else:
        use_praw = bool(os.environ.get("REDDIT_CLIENT_ID") and os.environ.get("REDDIT_CLIENT_SECRET"))
        if use_praw:
            print("REDDIT_CLIENT_ID detected — using PRAW (authenticated).")
        else:
            print("No Reddit credentials found — using public JSON API (read-only).")
            print("Tip: set REDDIT_CLIENT_ID + REDDIT_CLIENT_SECRET for higher rate limits.\n")

        df = scrape(use_praw=use_praw)

    if not df.empty:
        save(df)
