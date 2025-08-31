# NBA API `SEASON_ID` Encoding

This document explains how the NBA API encodes `SEASON_ID` values and what each prefix represents, including whether it counts as an **official game** (regular season / playoffs / play-in) or a **special event**.

---

## Format

Each `SEASON_ID` consists of:

- **Prefix (first digit):** Indicates the phase or type of competition.  
- **Year (remaining digits):** Indicates the starting year of the season.

Example:  
`22023` → prefix **2** = Regular Season, year **2023**.

---

## Prefix Legend

| Prefix | Phase / Competition                     | Official Game? | Notes |
|:------:|-----------------------------------------|:--------------:|-------|
| `1`    | **Preseason**                           | No             | Exhibition warm-up games |
| `2`    | **Regular Season**                      | Yes            | Full 82-game schedule |
| `3`    | **All-Star Weekend** (All-Star Game, etc.) | No          | Showcase events |
| `4`    | **Playoffs**                            | Yes            | Traditional postseason |
| `5`    | **Play-In Tournament**                  | Yes            | Determines final playoff spots |
| `6`    | **NBA Cup Final** (In-Season Tournament Final) | No  | Single championship game at neutral site (e.g., Las Vegas) |

---

## Examples

- `22023` → Regular season 2023 (official)  
- `12023` → Preseason 2023 (not official)  
- `32024` → All-Star 2024 (not official)  
- `42024` → Playoffs 2025 (official)  
- `52024` → Play-In 2025 (official)  
- `62024` → NBA Cup Final 2024 (not official)  

---

## Summary

- Use `2YYYY` for **regular season official games**.  
- Use `4YYYY` for **playoffs**, `5YYYY` for **play-in**.  
- Prefixes `1`, `3`, `6` represent **non-official events** (preseason, All-Star, NBA Cup Final).  

This encoding allows clear separation of official NBA game data from exhibitions and special events.
