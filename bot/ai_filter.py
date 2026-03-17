"""
Gemini AI filtering and caption generation.

passes_filter()  — scores content 1-10 for niche fit.
generate_post()  — rewrites caption in Mother's Joy warm village voice.
"""

import json
import logging
import re

from google import genai

from bot import config

log = logging.getLogger(__name__)

_MODEL = "gemini-2.5-flash"


def _get_client():
    return genai.Client(api_key=config.get_gemini_key())


# ── Filter ───────────────────────────────────────────────────────────────────

_FILTER_PROMPT = """\
You are a content curator for Mother's Joy — a warm, supportive parenting community \
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
- Video content is equally valuable as image content — do NOT penalise videos

FAIL criteria (any = automatic fail):
- Advertisements, product promotions, discount codes
- Controversial / judgmental / "mommy wars" content
- Medical advice that could be harmful
- Political content
- Sexually explicit or violent content
- Low effort / engagement bait / rage bait
- Content targeting older children (teens+)

IMPORTANT — Diversity & Inclusion:
- Evaluate ONLY on content quality and parenting value
- Do NOT consider or factor in the creator's race, ethnicity, skin colour, nationality, \
religion, disability, body type, or appearance
- Parenting wisdom comes from ALL communities — score diverse voices equally
- Content from creators of any background must be judged purely on warmth, helpfulness, \
and fit with our community values
- Do NOT favour or penalise any demographic group

Post to evaluate:
---
Account: {account}
Caption: {caption}
Likes: {likes}
Media type: {media_type}
---

Respond in EXACTLY this JSON format, nothing else:
{{"pass": true/false, "score": 1-10, "reason": "one sentence explanation"}}
"""


def passes_filter(content: dict) -> tuple[bool, int, str]:
    """
    AI-score content for niche fit.
    Returns (passes: bool, score: 1-10, reason: str).
    """
    client = _get_client()

    prompt = _FILTER_PROMPT.format(
        account=content.get("account", "unknown"),
        caption=content.get("caption", "")[:1500],
        likes=content.get("likes", 0),
        media_type=content.get("media_type", "image"),
    )

    try:
        response = client.models.generate_content(model=_MODEL, contents=prompt)
        text = response.text.strip()

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
        return passes, score, reason

    except Exception as exc:
        log.error("AI filter error: %s", exc)
        return False, 0, f"AI error: {exc}"


# ── Caption generation ───────────────────────────────────────────────────────

_GENERATE_PROMPT = """\
You are the voice of Mother's Joy — a warm parenting community at mothersjoy.app.

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
- Line break then: 🌿 More support → mothersjoy.app
- Line break then: 5-8 relevant hashtags (mix popular + niche)

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
    client = _get_client()

    prompt = _GENERATE_PROMPT.format(
        account=content.get("account", ""),
        caption=content.get("caption", "")[:1500],
        media_type=content.get("media_type", "image"),
        max_chars=config.MAX_CAPTION_LENGTH,
    )

    try:
        response = client.models.generate_content(model=_MODEL, contents=prompt)
        caption = response.text.strip()

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

    except Exception as exc:
        log.error("Caption generation error: %s", exc)
        return {"caption": ""}
