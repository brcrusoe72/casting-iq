# 🔥 CastingIQ — Investment Casting Analytics Platform

Manufacturing intelligence platform for aerospace investment casting operations. Demonstrates OEE analytics, SPC, defect analysis, and hidden pattern detection applied to vacuum casting of superalloy turbine components.

## Features

- **OEE Dashboard** — Availability × Performance × Quality decomposition for vacuum pour furnaces
- **Yield Analysis** — First-time yield by alloy, shift, furnace with scrap Pareto
- **SPC Charts** — Statistical Process Control on pour temperature and mold preheat with Western Electric rule detection
- **Hidden Pattern Detection** — Four data-driven discoveries:
  1. Shell room humidity >55% RH → 2.3x shell crack defect rate
  2. First pour position (cold furnace) → 1.8x scrap rate
  3. Night shift → elevated dimensional defects
  4. Short stop clusters (3+ in 30 min) precede breakdowns 87% of the time
- **Cycle Time Waterfall** — Wax-to-ship lead time with queue waste identification
- **Downtime Analysis** — Category breakdown, furnace comparison, weekly trends

## Data

Modeled from industry research on aerospace investment casting:
- 3,559 production events across 4 vacuum furnaces
- 25,842 parts cast in 5 superalloys (IN718, IN738, Waspaloy, CMSX-4, René-80)
- 4,931 downtime events including short stops, breakdowns, and planned maintenance
- 5.5 months of operations data (Oct 2025 – Mar 2026)

## Run Locally

```bash
pip install -r requirements.txt
streamlit run app.py
```

## Built By

**Brian Crusoe** — Manufacturing Data Engineer | Six Sigma Black Belt
- [GitHub](https://github.com/brcrusoe72)
- [LinkedIn](https://linkedin.com/in/briancrusoe)

## Related Projects

- **[Operations Intelligence Analyzer](https://github.com/brcrusoe72/operations-intelligence-analyzer)** — AI-powered OEE analysis ([live demo](https://oee.trueaicost.com))
- **[AgentSearch](https://github.com/brcrusoe72/agent-search)** — Free, self-hosted search API for AI agents
- **[Agent Café](https://github.com/brcrusoe72/agent-cafe)** — AI agent marketplace ([live at thecafe.dev](https://thecafe.dev))
- **[Manufacturing Analyst Pro](https://github.com/brcrusoe72/manufacturing-analyst-pro)** — MES data analysis CLI
- **[AI True Cost Calculator](https://trueaicost.com)** — Know what your AI project really costs
