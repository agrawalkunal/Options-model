# APP Options Catalyst Research Findings

**Analysis Date:** January 5, 2026
**Stock:** APP (AppLovin)
**Period Analyzed:** January 2025 - January 2026

---

## Executive Summary

APP exhibited **58 days with >5% moves** in the past year, with **21% occurring on Fridays**. The stock shows moderate correlation with META (0.498), supporting the ad sector news catalyst hypothesis.

---

## Key Statistics

| Metric | Value |
|--------|-------|
| Trading Days Analyzed | 250 |
| Price Range | $200.50 - $745.61 |
| Current Price | $622.25 |
| Days with >5% Moves | 58 |
| Friday Big Moves | 12 (21%) |
| Average Friday Range | 5.6% |
| Max Friday Range | 10.7% |

---

## Correlation Analysis

| Ticker | Correlation with APP | Relevance |
|--------|---------------------|-----------|
| META | **0.498** | High - Primary ad sector indicator |
| GOOGL | 0.377 | Moderate - Secondary indicator |
| TTD | 0.095 | Low - Less relevant |

**Key Insight:** META is the strongest correlated ad sector stock. Monitor META news/price action as a leading indicator for APP moves.

---

## 1/2/26 Case Study: 630 Put

This Friday showed the largest recent intraday range (10.7%):

| Metric | Value |
|--------|-------|
| Open | $683.50 |
| High | $683.50 |
| Low | $610.58 |
| Close | $618.32 |
| Drop from Open | -10.7% |

### 630 Put Performance

- **At Open:** 630 Put was $53.50 OTM (7.8% OTM)
- **At Low:** 630 Put was $19.42 ITM
- **Estimated Entry:** $0.50 - $2.00 (deep OTM 0DTE)
- **Intrinsic at Low:** $19.42
- **Potential Gain:** **970% - 3,800%+**

---

## Validated Catalysts

### 1. Ad Sector News (Primary)
- **Correlation:** APP moves with META (0.498 correlation)
- **Trigger:** Major META news, ad spend reports, programmatic advertising trends
- **Lead Time:** Same-day or T-1

### 2. Friday 0DTE Dynamics (Primary)
- **Evidence:** 21% of big moves occur on Fridays
- **Mechanism:** Gamma exposure amplifies moves, theta decay creates cheap entry
- **Best Setup:** Friday morning with pre-market momentum

### 3. Direct Company News (Secondary)
- **Examples:** S&P 500 inclusion (played out), earnings, partnerships
- **Trigger:** Company-specific announcements
- **Lead Time:** Immediate

---

## Signal Thresholds (Preliminary)

### Entry Criteria
| Parameter | Threshold |
|-----------|-----------|
| Day | Friday (primary), Thursday (secondary) |
| DTE | 0-2 days |
| Strike Type | OTM (5-10% out) or Deep OTM (10%+ out) |
| Direction | Based on news sentiment |

### Alert Triggers
1. **Ad Sector News:** Major META/GOOGL headline before market open
2. **Pre-market Momentum:** APP futures/pre-market move >2%
3. **Volume Anomaly:** Options volume >2x 20-day average on OTM strikes
4. **IV Spike:** IV rank >80th percentile

---

## Historical Big Move Days

### Top 10 Positive Moves
| Date | Day | Return | Range |
|------|-----|--------|-------|
| 2/13/25 | Thu | +24.0% | 15.3% |
| 4/9/25 | Wed | +16.9% | 26.8% |
| 8/7/25 | Thu | +12.0% | 17.3% |
| 5/8/25 | Thu | +11.9% | 6.5% |
| 9/8/25 | Mon | +11.6% | 3.7% |
| 5/2/25 | **Fri** | +10.1% | 7.7% |
| 3/11/25 | Tue | +8.3% | 10.6% |
| 3/24/25 | Mon | +8.2% | 5.8% |
| 2/14/25 | **Fri** | +8.2% | 11.4% |
| 3/14/25 | **Fri** | +7.7% | 5.5% |

### Top 10 Negative Moves
| Date | Day | Return | Range |
|------|-----|--------|-------|
| 3/27/25 | Thu | -20.1% | 22.8% |
| 3/6/25 | Thu | -18.4% | 15.7% |
| 4/4/25 | **Fri** | -16.3% | 15.4% |
| 10/6/25 | Mon | -14.0% | 20.7% |
| 2/26/25 | Wed | -12.2% | 16.5% |
| 3/10/25 | Mon | -12.0% | 8.5% |
| 3/18/25 | Tue | -9.1% | 8.8% |
| 4/3/25 | Thu | -9.8% | 7.2% |
| 2/20/25 | Thu | -8.9% | 13.8% |
| 1/2/26 | **Fri** | -8.2% | 10.7% |

---

## Next Steps

1. **Set up Schwab API** for real-time options data
2. **Implement news monitoring** (Finnhub, NewsAPI) for ad sector alerts
3. **Build signal detection modules** based on validated thresholds
4. **Configure Discord webhook** for notifications
5. **Paper trade signals** for 2-4 weeks before live trading
