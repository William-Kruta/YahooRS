import math
import polars as pl


def pdf(x: float) -> float:
    """Standard normal probability density function."""
    return math.exp(-0.5 * x * x) / math.sqrt(2.0 * math.pi)


def cdf(x: float) -> float:
    """Standard normal cumulative distribution function (approximation)."""
    t = 1.0 / (1.0 + 0.2316419 * abs(x))
    poly = t * (0.319381530 + t * (-0.356563782 + t * (1.781477937 + t * (-1.821255978 + t * 1.330274429))))
    approx = 1.0 - pdf(x) * poly
    return approx if x >= 0.0 else 1.0 - approx


def d1_d2(s: float, k: float, t: float, r: float, sigma: float) -> tuple[float, float]:
    """Calculate d1 and d2 for Black-Scholes."""
    d1 = (math.log(s / k) + (r + 0.5 * sigma * sigma) * t) / (sigma * math.sqrt(t))
    d2 = d1 - sigma * math.sqrt(t)
    return d1, d2


def calculate_greeks(
    s: float,
    k: float,
    t: float,
    r: float,
    sigma: float,
    last_price: float,
    is_call: bool,
) -> dict:
    """
    Calculate Greeks for a single option.

    s          = current underlying price
    k          = strike price
    t          = time to expiration in years (DTE / 365.0)
    r          = risk-free rate (e.g. 0.05 for 5%)
    sigma      = implied volatility (e.g. 0.30 for 30%)
    last_price = premium paid for the option
    is_call    = True for call, False for put
    """
    if t <= 0.0 or sigma <= 0.0 or s <= 0.0 or k <= 0.0:
        return {"delta": 0.0, "gamma": 0.0, "theta": 0.0, "vega": 0.0, "bs_price": 0.0, "prob_profit": 0.0}

    d1, d2 = d1_d2(s, k, t, r, sigma)
    discount = math.exp(-r * t)

    gamma = pdf(d1) / (s * sigma * math.sqrt(t))
    vega = s * pdf(d1) * math.sqrt(t) / 100.0  # per 1% change in IV

    if is_call:
        delta = cdf(d1)
        theta = (-(s * pdf(d1) * sigma) / (2.0 * math.sqrt(t)) - r * k * discount * cdf(d2)) / 365.0
        bs_price = s * cdf(d1) - k * discount * cdf(d2)
        breakeven = k + last_price
        if breakeven <= 0.0:
            prob_profit = 0.0
        else:
            d_breakeven = (math.log(s / breakeven) + (r - 0.5 * sigma * sigma) * t) / (sigma * math.sqrt(t))
            prob_profit = cdf(d_breakeven)
    else:
        delta = cdf(d1) - 1.0
        theta = (-(s * pdf(d1) * sigma) / (2.0 * math.sqrt(t)) + r * k * discount * cdf(-d2)) / 365.0
        bs_price = k * discount * cdf(-d2) - s * cdf(-d1)
        breakeven = k - last_price
        if breakeven <= 0.0:
            prob_profit = 0.0
        else:
            d_breakeven = (math.log(s / breakeven) + (r - 0.5 * sigma * sigma) * t) / (sigma * math.sqrt(t))
            prob_profit = cdf(-d_breakeven)

    return {
        "delta": delta,
        "gamma": gamma,
        "theta": theta,
        "vega": vega,
        "bs_price": bs_price,
        "prob_profit": prob_profit,
    }



def implied_volatility(
    s: float,
    k: float,
    t: float,
    r: float,
    market_price: float,
    is_call: bool,
    tol: float = 1e-6,
    max_iter: int = 100,
) -> float:
    """
    Compute implied volatility using Newton's method.

    s            = underlying price
    k            = strike price
    t            = time to expiration in years
    r            = risk-free rate
    market_price = observed option price (mid price)
    is_call      = True for call, False for put
    """
    if t <= 0.0 or market_price <= 0.0:
        return 0.0

    # Initial guess: start with 0.3 (30%)
    sigma = 0.3

    for _ in range(max_iter):
        d1, d2 = d1_d2(s, k, t, r, sigma)
        discount = math.exp(-r * t)

        if is_call:
            bs_price = s * cdf(d1) - k * discount * cdf(d2)
        else:
            bs_price = k * discount * cdf(-d2) - s * cdf(-d1)

        diff = bs_price - market_price

        # Vega (not scaled by 100 here, we need the raw derivative)
        vega = s * pdf(d1) * math.sqrt(t)

        if vega < 1e-12:
            break

        sigma -= diff / vega

        # Clamp to reasonable bounds
        sigma = max(sigma, 1e-6)

        if abs(diff) < tol:
            return sigma

    # If we didn't converge, return the last estimate if reasonable, else 0
    return sigma if 0.001 < sigma < 10.0 else 0.0


def add_greeks_to_df(
    df: pl.DataFrame,
    risk_free_rate: float = 0.05,
) -> pl.DataFrame:
    results = {"delta": [], "gamma": [], "theta": [], "vega": [], "bs_price": [], "prob_profit": []}

    strikes = df["strike"].to_list()
    ivs = df["impliedVolatility"].to_list()
    last_prices = df["lastPrice"].to_list()
    bids = df["bid"].to_list()
    asks = df["ask"].to_list()
    dtes = df["dte"].to_list()
    option_types = df["option_type"].to_list()
    stock_prices = df["stock_price"].to_list()

    for i in range(len(strikes)):
        s = stock_prices[i] or 0.0
        strike = strikes[i] or 0.0
        last_price = last_prices[i] or 0.0
        bid = bids[i] or 0.0
        ask = asks[i] or 0.0
        dte = dtes[i] or 0
        is_call = option_types[i] == "call"

        t = max(dte, 0.5) / 365.0

        if ask > 0.0 and bid > 0.0:
            premium = (bid + ask) / 2.0
        elif ask > 0.0:
            premium = ask
        elif bid > 0.0:
            premium = bid
        else:
            premium = last_price

        # Compute our own IV from mid price instead of using yfinance's
        iv = implied_volatility(s, strike, t, risk_free_rate, premium, is_call)

        greeks = calculate_greeks(s, strike, t, risk_free_rate, iv, premium, is_call)

        for key in results:
            results[key].append(greeks[key])

    return df.with_columns([
        pl.Series(name, values) for name, values in results.items()
    ])
