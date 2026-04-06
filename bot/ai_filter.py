"""
AI filtering and caption generation via Gemini (primary) with Groq fallback.

passes_filter()  — scores content 1-10 for niche fit.
generate_post()  — rewrites caption in Mother's Joy warm village voice.
"""

import json
import logging
import random
import re
import time
from datetime import datetime, timezone

from bot import config

log = logging.getLogger(__name__)


class AIUnavailable(Exception):
    """All AI providers temporarily down. NOT a crash — callers handle gracefully."""
    pass


# ── Local heuristic fallback (when all AI is down) ───────────────────────────

_POSITIVE_KEYWORDS = [
    "parenting", "gentle", "mum", "mom", "baby", "toddler", "motherhood",
    "newborn", "supportive", "wellness", "bedtime", "feeding", "milestone",
    "postnatal", "nappy", "cot", "breastfeed", "sleep", "teething",
]
_NEGATIVE_KEYWORDS = [
    "discount", "#ad", "#sponsored", "giveaway", "link in bio", "promo",
    "swipe up", "shop now", "use code", "dm to order", "paid partnership",
    "controversial", "rage", "fight", "custody battle",
]


def _local_heuristic_score(content: dict) -> tuple[bool, int, str]:
    """Keyword-based scoring when all AI providers are down.
    Trust discovery: items already came from parenting search terms/niches.
    Reject only clearly bad content (negative keywords). Pass everything else."""
    caption = (content.get("caption", "") or "").lower()

    neg_count = sum(1 for kw in _NEGATIVE_KEYWORDS if kw in caption)
    if neg_count > 0:
        return False, 2, f"local_heuristic: {neg_count} negative keyword(s)"

    pos_count = sum(1 for kw in _POSITIVE_KEYWORDS if kw in caption)
    if pos_count >= 2:
        return True, 7, f"local_heuristic: {pos_count} positive parenting signals"

    # Trust discovery process — these items came from parenting niches
    return True, 6, "local_heuristic: no negatives, trusting discovery source"


# Primary: Gemini
_GEMINI_MODEL = "gemini-2.5-flash"
_GEMINI_MODEL_PRO = "gemini-2.5-pro"
# Fallback: Groq
_GROQ_MODEL = "llama-3.3-70b-versatile"

# Retry settings for rate limits
_MAX_RETRIES = 3
_BASE_BACKOFF_SECONDS = 5

# Circuit breaker: skip Gemini entirely when rate-limited until this time
_gemini_circuit_open_until: float = 0
_CIRCUIT_BREAKER_DURATION = 120  # seconds — skip Gemini for 2 min after confirmed 429


def _chat(prompt: str, pro: bool = False) -> str:
    """
    Try Gemini first (with retry on 429); fall back to Groq.
    Circuit breaker: skips Gemini entirely for 2 minutes after confirmed rate limit,
    avoiding ~70s of wasted retry time per call.
    """
    global _gemini_circuit_open_until

    last_gemini_exc = None

    # ── Circuit breaker: skip Gemini if recently rate-limited ──
    if time.time() < _gemini_circuit_open_until:
        log.debug("Gemini circuit breaker active — going straight to Groq")
    else:
        # ── Attempt 1: Gemini (primary) with retry on rate limits ──
        gemini_key = config.get_gemini_key()
        fallback_key = config.get_gemini_fallback_key()
        keys_to_try = [k for k in [gemini_key, fallback_key] if k]

        for key_idx, api_key in enumerate(keys_to_try):
            for attempt in range(_MAX_RETRIES):
                try:
                    from google import genai
                    client = genai.Client(api_key=api_key)
                    model = _GEMINI_MODEL_PRO if pro else _GEMINI_MODEL
                    response = client.models.generate_content(model=model, contents=prompt)
                    # Success — reset circuit breaker
                    _gemini_circuit_open_until = 0
                    return response.text.strip()
                except Exception as exc:
                    last_gemini_exc = exc
                    exc_str = str(exc)
                    is_rate_limit = "429" in exc_str or "RESOURCE_EXHAUSTED" in exc_str
                    if is_rate_limit and attempt < _MAX_RETRIES - 1:
                        wait = _BASE_BACKOFF_SECONDS * (2 ** attempt) + random.uniform(0, 2)
                        log.warning(
                            "Gemini rate-limited (key %d, attempt %d/%d), retrying in %.1fs",
                            key_idx + 1, attempt + 1, _MAX_RETRIES, wait,
                        )
                        time.sleep(wait)
                        continue
                    if is_rate_limit:
                        log.warning("Gemini key %d exhausted after %d retries", key_idx + 1, _MAX_RETRIES)
                    else:
                        log.warning("Gemini key %d failed: %s", key_idx + 1, exc)
                    break

        if last_gemini_exc:
            exc_str = str(last_gemini_exc)
            if "429" in exc_str or "RESOURCE_EXHAUSTED" in exc_str:
                _gemini_circuit_open_until = time.time() + _CIRCUIT_BREAKER_DURATION
                log.info("Gemini circuit breaker activated — using Groq for next %ds", _CIRCUIT_BREAKER_DURATION)
            log.warning("Gemini failed (%s), trying Groq", last_gemini_exc)

    # ── Attempt 2: Groq fallback ──
    try:
        from groq import Groq
        client = Groq(api_key=config.get_groq_key())
        response = client.chat.completions.create(
            model=_GROQ_MODEL,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.7,
            max_tokens=2048,
        )
        return response.choices[0].message.content.strip()
    except Exception as groq_exc:
        log.error("Groq fallback also failed: %s", groq_exc)

    raise AIUnavailable(f"All AI providers temporarily down. Gemini: {last_gemini_exc}")


# ── Filter ───────────────────────────────────────────────────────────────────

_FILTER_PROMPT = """You are a content curator for Mother's Joy — a warm, supportive parenting community \
(mothersjoy.app). We share gentle, positive parenting content that feels like advice \
from a wise, reassuring friend. Our audience: UK parents (especially new mums), \
postnatal wellness, gentle parenting, baby/toddler tips.

Evaluate this Instagram post for our feed. Score 1-10 and decide PASS or FAIL.

PASS criteria (score ≥ 6):
- Warm, positive, supportive parenting content
- Practical tips for parents (sleep, feeding, milestones, self-care)
- Emotional support, reassurance, "you're doing great" energy
- Gentle/respectful parenting philosophy
- Postnatal wellness, mum mental health
- UK-relevant content preferred but not required
- We ONLY post video/reel content — all items will be videos

FAIL criteria (any = automatic fail):
- Advertisements, product promotions, discount codes
- Controversial / judgmental / "mommy wars" content
- Medical advice that could be harmful
- Political content
- Sexually explicit or violent content
- Low effort / engagement bait / rage bait
- Content targeting older children (teens+)
- Generic lifestyle, aesthetics, or motivational fluff that is not clearly about parenting or family care
- Outdated seasonal or time-sensitive content whose moment has already passed

IMPORTANT — Recency:
- Today's date: {current_date}
- Post age: {age_hours} hours
- Be strict about stale topical references. If the post mentions a specific event, holiday, campaign, or seasonal moment \
that no longer feels current, FAIL it even if the tone is warm

IMPORTANT — Diversity & Inclusion:
- Evaluate ONLY on content quality and parenting value
- Do NOT consider or factor in the creator's race, ethnicity, skin colour, nationality, \
religion, disability, body type, or appearance
- Parenting wisdom comes from ALL communities — score diverse voices equally
- Content from creators of any background must be judged purely on warmth, helpfulness, \
and fit with our community values
- Do NOT favour or penalise any demographic group
- Do NOT infer anyone's race or ethnicity from visuals. If diversity context matters, use only explicit textual context \
such as captions, hashtags, or the discovery source metadata below

Post to evaluate:
---
Account: {account}
Caption: {caption}
Likes: {likes}
Media type: {media_type}
Discovery group: {discovery_group}
Discovery tag: {discovery_term}
---

Respond in EXACTLY this JSON format, nothing else:
{{"pass": true/false, "score": 1-10, "reason": "one sentence explanation"}}
"""


def passes_filter(content: dict) -> tuple[bool, int, str]:
    """
    AI-score content for niche fit.
    Returns (passes: bool, score: 1-10, reason: str).
    """
    taken_at = int(content.get("taken_at", 0) or 0)
    if taken_at:
        age_hours = round((datetime.now(timezone.utc).timestamp() - taken_at) / 3600, 1)
    else:
        age_hours = "unknown"

    prompt = _FILTER_PROMPT.format(
        account=content.get("account", "unknown"),
        caption=content.get("caption", "")[:1500],
        likes=content.get("likes", 0),
        media_type=content.get("media_type", "image"),
        discovery_group=content.get("discovery_group", "unknown"),
        discovery_term=content.get("discovery_term", "unknown"),
        age_hours=age_hours,
        current_date=datetime.now(timezone.utc).strftime("%B %d, %Y"),
    )

    try:
        text = _chat(prompt)

        # Extract JSON from response (handle markdown code blocks)
        json_match = re.search(r"\{.*\}", text, re.DOTALL)
        if not json_match:
            log.warning("AI filter returned non-JSON: %s", text[:200])
            return False, 0, "Failed to parse AI response"

        result = json.loads(json_match.group())
        passes = bool(result.get("pass", False))
        score = int(result.get("score", 0))
        reason = str(result.get("reason", ""))

        log.info("AI filter: %s (score=%d) — %s", "PASS" if passes else "FAIL", score, reason)
        try:
            from bot.health import get_registry
            get_registry().report_success("ai_filter")
        except Exception:
            pass
        return passes, score, reason

    except AIUnavailable:
        log.warning("All AI providers down — using local heuristic for %s", content.get("source_url", ""))
        try:
            from bot.health import get_registry
            get_registry().report_failure("ai_filter", "all_providers_down")
        except Exception:
            pass
        return _local_heuristic_score(content)

    except Exception as exc:
        log.error("AI filter error: %s", exc)
        return False, 0, f"AI error: {exc}"


# ── Caption generation ───────────────────────────────────────────────────────

_CTA_INSTRUCTIONS = {
    "follow_ask": 'Include a natural "Follow {handle} for daily parenting warmth 💜" line after your sign-off.',
    "save_prompt": 'Include a "💾 Save this one — you\'ll want it at 2am" or similar save/bookmark prompt.',
    "tag_friend": 'Include a "Tag a mama who needs to hear this today 💜" prompt.',
    "share_prompt": 'Include a "Share this with your village — someone needs it right now 💜" prompt.',
    "comment_question": "End with a specific, warm engagement question that invites comments (e.g. \"What's your bedtime survival trick?\" or \"How did you handle this stage?\").",
}

_GENERATE_PROMPT = """You are the voice of Mother's Joy — a warm parenting community at mothersjoy.app.

Rewrite this Instagram post caption in our brand voice. Rules:

VOICE:
- Warm, reassuring, like a wise friend sitting with you over tea 💜
- Gentle encouragement — "you're doing beautifully", "it's okay to rest"
- Simple language, no jargon, no lecturing
- Speak TO the parent, not AT them
- British English spelling (mum, nappies, cot, etc.)

FORMAT:
- Opening hook (1 engaging line, can use emoji sparingly 💜🌱✨)
- 2-4 short paragraphs of warm, practical content
- Encouraging sign-off
{cta_block}
- Line break then: 🌿 More support → mothersjoy.app
- Line break then: Follow {handle} for more 💜
- Line break then: 5-8 relevant hashtags (mix popular + niche)

ENGAGEMENT (critical for growth — do ALL of these):
- ALWAYS mention {handle} naturally in the caption body or sign-off
- End with a specific, warm QUESTION that invites parents to comment
  (e.g. "What's your go-to bedtime trick, mama?" or "How old was your little one when this started?")
- The question should feel conversational, not forced

RULES:
- Under {max_chars} characters total
- No medical claims, no "you should/must", no guilt
- Credit original creator naturally if account name is provided
- Include 💜 at least once — it's our signature
- Do NOT copy the original caption — rewrite entirely in our voice
- Hashtags must include #mothersjoy
- Use inclusive language that welcomes parents of ALL backgrounds, races, and cultures
- Never assume the audience is any particular ethnicity — our community is beautifully diverse

Original post:
---
Account: @{account}
Caption: {caption}
Media type: {media_type}
---

Write the new caption now (just the caption text, no extra commentary):
"""


def generate_post(content: dict) -> dict:
    """
    Generate an Instagram post in Mother's Joy voice.
    Returns dict with 'caption' key.
    """

    # Pick 2 random CTA styles for variety
    chosen_ctas = random.sample(config.CTA_STYLES, min(2, len(config.CTA_STYLES)))
    cta_lines = []
    for cta in chosen_ctas:
        instruction = _CTA_INSTRUCTIONS.get(cta, "")
        if instruction:
            cta_lines.append(f"- {instruction.format(handle=config.BRAND_HANDLE)}")
    cta_block = "\n".join(cta_lines) if cta_lines else ""

    prompt = _GENERATE_PROMPT.format(
        account=content.get("account", ""),
        caption=content.get("caption", "")[:1500],
        media_type=content.get("media_type", "image"),
        max_chars=config.MAX_CAPTION_LENGTH,
        cta_block=cta_block,
        handle=config.BRAND_HANDLE,
    )

    try:
        caption = _chat(prompt, pro=True)

        # Trim to max length
        if len(caption) > config.MAX_CAPTION_LENGTH:
            caption = caption[: config.MAX_CAPTION_LENGTH - 3] + "..."

        # Ensure mothersjoy.app link is present
        if config.BRAND_LINK not in caption:
            caption += f"\n\n🌿 More support → {config.BRAND_LINK}"

        # Ensure #mothersjoy hashtag
        if "#mothersjoy" not in caption.lower():
            caption += " #mothersjoy"

        log.info("Generated caption (%d chars) for %s", len(caption), content.get("source_url"))
        return {"caption": caption}

    except AIUnavailable:
        log.warning("All AI providers down — using original caption for %s", content.get("source_url", ""))
        try:
            from bot.health import get_registry
            get_registry().report_failure("ai_filter", "all_providers_down")
        except Exception:
            pass
        # Use original caption as fallback so item still gets queued
        original = (content.get("caption", "") or "").strip()
        if original:
            fallback = original
            # Ensure brand link and hashtag
            if config.BRAND_LINK not in fallback:
                fallback += f"\n\n🌿 More support → {config.BRAND_LINK}"
            if "#mothersjoy" not in fallback.lower():
                fallback += " #mothersjoy"
            return {"caption": fallback, "ai_deferred": True}
        return {"caption": "", "ai_deferred": True}

    except Exception as exc:
        log.error("Caption generation error: %s", exc)
        return {"caption": ""}


# ── Engagement comment generation ────────────────────────────────────────

_ENGAGEMENT_COMMENT_PROMPT = """You are commenting as @mothersjoyapp, a warm parenting community on Instagram.
Write a genuine, supportive comment on this Instagram post.

Rules:
- 1-2 sentences MAX (15-40 words)
- Warm, encouraging, specific to the content
- Use 1 emoji maximum (💜 preferred)
- NEVER be generic ("great post!" "love this!" "amazing!")
- NEVER promote anything or mention links
- NEVER use hashtags in comments
- Sound like a real mum who genuinely resonates with this
- British English

Post by @{account}:
{caption}

Write ONLY the comment text (nothing else):
"""


def generate_engagement_comment(caption: str, account: str) -> str:
    """Generate a warm, genuine comment for an Instagram post."""
    prompt = _ENGAGEMENT_COMMENT_PROMPT.format(
        account=account,
        caption=(caption or "")[:800],
    )
    try:
        comment = _chat(prompt)
        comment = comment.strip('"').strip("'")
        if len(comment) > 150:
            comment = comment[:147] + "..."
        log.info("Generated engagement comment (%d chars) for @%s", len(comment), account)
        return comment
    except AIUnavailable:
        log.warning("AI down — skipping engagement comment for @%s", account)
        return ""
    except Exception as exc:
        log.error("Engagement comment generation error: %s", exc)
        return ""


# ── Comment reply generation ─────────────────────────────────────────────

_REPLY_PROMPT = """You are replying to a comment on @mothersjoyapp's Instagram post.
You are the Mother's Joy community — warm, supportive, like a village elder.

Rules:
- 1-2 sentences MAX (10-30 words)
- Warm and personal — use the commenter's energy
- Use 1 emoji max (💜 preferred)
- NEVER be generic
- NEVER promote anything
- British English

Our post caption: {post_caption}
Comment by @{comment_author}: {comment_text}

Write ONLY the reply (nothing else):
"""


def generate_comment_reply(comment_text: str, comment_author: str, post_caption: str) -> str:
    """Generate a warm reply to a comment on our post."""
    prompt = _REPLY_PROMPT.format(
        post_caption=(post_caption or "")[:500],
        comment_author=comment_author,
        comment_text=(comment_text or "")[:300],
    )
    try:
        reply = _chat(prompt)
        reply = reply.strip('"').strip("'")
        if len(reply) > 150:
            reply = reply[:147] + "..."
        log.info("Generated reply (%d chars) for @%s", len(reply), comment_author)
        return reply
    except AIUnavailable:
        log.warning("AI down — skipping reply for @%s", comment_author)
        return ""
    except Exception as exc:
        log.error("Comment reply generation error: %s", exc)
        return ""
