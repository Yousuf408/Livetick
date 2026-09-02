# MULTI-PAGE & MULTI-USER ORB ALGO TRADING PLATFORM ARCHITECTURE PLAN

## 1. Executive Summary & Vision
A scalable, modular, multi-user algorithmic trading platform engineered around Opening Range Breakout (ORB), 15-minute Candlestick Matrix, and real-time execution across multiple brokers (Angel One SmartAPI, DhanHQ, etc.).

---

## 2. System Architecture

```text
┌────────────────────────────────────────────────────────────────────────┐
│                        SHARED LIVE DATA FEED LAYER                     │
│  Single Centralized Backend WebSocket (SmartStream / NSE Feed: 750 Stocks)│
│  - Builds 15-min / 5-min OHLC Candles once in memory                   │
│  - Calculates VWAP, Ranges, Highs, Lows in real-time                  │
│  - Broadcasts live ticks & candle events to all active users via SSE/WS│
└──────────────────────────────────┬─────────────────────────────────────┘
                                   │
      ┌────────────────────────────┴────────────────────────────┐
      ▼                                                         ▼
┌──────────────────────────────┐          ┌──────────────────────────────┐
│       USER A SESSION         │          │       USER B SESSION         │
│ - Angel One / Dhan API Keys  │          │ - Angel One / Dhan API Keys  │
│ - Budget: ₹1,00,000 (5 Parts)│          │ - Budget: ₹5,00,000 (10 Parts)│
│ - Strategy: Advance ORB Buy  │          │ - Strategy: Big Players ORB  │
│ - Mode: Live Trading         │          │ - Mode: Paper Trading (Sim)  │
│ - Independent Order Router   │          │ - Independent Order Router   │
└──────────────────────────────┘          └──────────────────────────────┘
```

---

## 3. Core Modules & Multi-Page Layout

### Page 1: 🏠 Dashboard (Overview)
- Real-time Market Overview (NIFTY 50, NIFTY BANK, India VIX, Market Breadth).
- Total Combined MTM / Day's P&L across all active trading strategies.
- Active Strategy Status (Advance ORB: ON/OFF, Big Players: ON/OFF).
- Real-time alert notifications for new breakouts.

### Page 2: ⚡ 15-Minute Live Matrix
- Real-time 15-minute OHLC candlestick matrix covering 750 stocks.
- Live tick color pulses (Green / Red flash on price update).
- Real-time VWAP and Last Tick timestamp tracking.
- Instant search, token filter, and CSV data export.

### Page 3: 🎯 ORB Strategy Screener
- **Advance ORB**: 
  - Tracks 9:15–9:30 range (High, Low, Range %, VWAP).
  - Breakout detection with criteria filters (Near High, 1st Range <= 3%, High-volume confirmation).
  - Auto Position Sizing Calculator: `MaxQty = floor((Budget / Parts) / LivePrice)`.
- **Big Players Strategy**:
  - Monitors 9:15 High/Low, Today High/Low, New Low formation, and Pullback inside the 9:15 range.
  - Breakout status classification (Confirmed, Inside 9:15, Below 9:15).

### Page 4: 🤖 Algo Execution & Order Automation
- Automated order placement engine triggered upon breakout validation.
- Customizable risk management rules:
  - Stop Loss (SL) % or fixed price levels.
  - Target / Risk-to-Reward (R:R) ratios.
  - Trailing Stop Loss engine.
  - Max Daily Loss cutoff limit per user.
- Execution Mode Toggle: **Paper Trading (Simulation)** vs. **Live Broker Execution**.

### Page 5: 💼 Portfolio & Orders
- Live Intraday Positions with real-time unrealized and realized P&L.
- Emergency Square-Off button (Exit All Intraday Positions).
- Individual position management: Modify SL / Target / Exit.
- Complete Order Book & Trade execution audit logs.

### Page 6: ⚙️ Broker & User Settings
- Multi-Broker API Credential Manager:
  - Angel One: API Key, Client ID, PIN, TOTP Secret Key (Auto-generation).
  - Dhan: Client ID, Access Token.
- Connection Diagnostic Tool (Tests API Server, Market Feed, Orders Endpoint).
- User Profile and preference settings.

---

## 4. Phased Implementation Roadmap

- **Phase 1: Multi-Page Navigation Shell & Routing Structure**
  - Establish a clean, responsive sidebar navigation layout.
  - Modularize page views without breaking the existing live matrix.

- **Phase 2: Centralized Shared Feed Engine**
  - Ensure the backend feeds 750 stocks efficiently through a single shared data pipe.
  - Isolate order routing logic from the market data stream.

- **Phase 3: User Authentication & Multi-Tenant Broker Store**
  - Provide secure credential storage and broker session isolation per user.

- **Phase 4: ORB Strategy & Automation Engine**
  - Implement Advance ORB and Big Players breakout signal calculations.
  - Connect auto-order triggers to paper trading and live broker APIs.

- **Phase 5: Portfolio, Risk Controls & Testing**
  - Build live positions table with dynamic P&L updates and trailing stop-loss.
  - Comprehensive end-to-end verification.

---
*Status: Ready for review and phased execution upon approval.*
