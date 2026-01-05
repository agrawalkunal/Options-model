"""Run catalyst research analysis."""

import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)

print('=' * 60)
print('APP OPTIONS CATALYST RESEARCH')
print('=' * 60)

# =============================================================================
# 1. FETCH APP STOCK DATA (1 YEAR)
# =============================================================================
print('\n1. FETCHING APP STOCK DATA (1 YEAR)')
print('-' * 40)
app = yf.Ticker('APP')
app_history = app.history(period='1y')

print(f'Data points: {len(app_history)} trading days')
print(f'Date Range: {app_history.index[0].date()} to {app_history.index[-1].date()}')
print(f'Price Range: ${app_history["Low"].min():.2f} - ${app_history["High"].max():.2f}')
print(f'Current Price: ${app_history["Close"].iloc[-1]:.2f}')

# Calculate daily returns
app_history['Daily_Return'] = app_history['Close'].pct_change() * 100
app_history['Intraday_Range'] = ((app_history['High'] - app_history['Low']) / app_history['Open']) * 100
app_history['Day_of_Week'] = app_history.index.day_name()

# =============================================================================
# 2. BIG MOVE DAYS
# =============================================================================
print('\n2. BIG MOVE DAYS (>5% daily move)')
print('-' * 40)
big_moves = app_history[abs(app_history['Daily_Return']) > 5].copy()
print(f'Total days with >5% moves: {len(big_moves)}')
print()
for idx, row in big_moves.sort_values('Daily_Return', ascending=False).iterrows():
    direction = '+' if row['Daily_Return'] > 0 else '-'
    print(f'  {idx.date()} ({row["Day_of_Week"]:9s}): {row["Daily_Return"]:+6.1f}% | Range: {row["Intraday_Range"]:5.1f}%')

# =============================================================================
# 3. FRIDAY ANALYSIS
# =============================================================================
print('\n3. FRIDAY ANALYSIS')
print('-' * 40)
fridays = app_history[app_history['Day_of_Week'] == 'Friday'].copy()
print(f'Total Fridays in dataset: {len(fridays)}')
print(f'Average Friday Return: {fridays["Daily_Return"].mean():.2f}%')
print(f'Std Dev of Returns: {fridays["Daily_Return"].std():.2f}%')
print(f'Average Intraday Range: {fridays["Intraday_Range"].mean():.2f}%')
print(f'Max Intraday Range: {fridays["Intraday_Range"].max():.2f}%')

friday_big_moves = big_moves[big_moves['Day_of_Week'] == 'Friday']
print(f'\nFridays with >5% moves: {len(friday_big_moves)}')
print(f'% of big moves on Friday: {len(friday_big_moves)/len(big_moves)*100:.1f}%')

print('\nRecent Fridays (Last 8):')
print('-' * 40)
for idx, row in fridays.tail(8).iterrows():
    print(f'  {idx.date()}: Open ${row["Open"]:.2f} | Close ${row["Close"]:.2f} | Return: {row["Daily_Return"]:+.1f}% | Range: {row["Intraday_Range"]:.1f}%')

# =============================================================================
# 4. INTRADAY DATA (Last 30 days)
# =============================================================================
print('\n4. INTRADAY DATA (5-min intervals, last 30 days)')
print('-' * 40)
app_intraday = app.history(period='30d', interval='5m')
print(f'Intraday data points: {len(app_intraday)}')
print(f'Date range: {app_intraday.index[0]} to {app_intraday.index[-1]}')

app_intraday['Date'] = app_intraday.index.date
app_intraday['Day_of_Week'] = app_intraday.index.day_name()

# Analyze intraday moves on Fridays
friday_intraday = app_intraday[app_intraday['Day_of_Week'] == 'Friday']

print('\nFriday Intraday Analysis:')
friday_stats = []
for date in sorted(friday_intraday['Date'].unique()):
    day_data = friday_intraday[friday_intraday['Date'] == date]

    open_price = day_data['Open'].iloc[0]
    close_price = day_data['Close'].iloc[-1]
    high_price = day_data['High'].max()
    low_price = day_data['Low'].min()

    daily_return = ((close_price - open_price) / open_price) * 100
    intraday_range = ((high_price - low_price) / open_price) * 100
    max_drawdown = ((low_price - open_price) / open_price) * 100
    max_rally = ((high_price - open_price) / open_price) * 100

    friday_stats.append({
        'Date': date,
        'Open': open_price,
        'High': high_price,
        'Low': low_price,
        'Close': close_price,
        'Return%': daily_return,
        'Range%': intraday_range,
        'MaxDown%': max_drawdown,
        'MaxUp%': max_rally,
    })

    print(f'  {date}: Open ${open_price:.2f} | High ${high_price:.2f} | Low ${low_price:.2f} | Return: {daily_return:+.1f}% | Range: {intraday_range:.1f}%')

# =============================================================================
# 5. OPTIONS CHAIN ANALYSIS
# =============================================================================
print('\n5. OPTIONS CHAIN ANALYSIS')
print('-' * 40)
expirations = app.options
print(f'Available expirations: {len(expirations)}')
print('Nearest expirations:')
for exp in expirations[:5]:
    print(f'  - {exp}')

current_price = app.history(period='1d')['Close'].iloc[-1]
print(f'\nCurrent APP Price: ${current_price:.2f}')

if expirations:
    nearest_exp = expirations[0]
    print(f'\nAnalyzing options expiring: {nearest_exp}')

    opt_chain = app.option_chain(nearest_exp)
    calls = opt_chain.calls
    puts = opt_chain.puts

    # OTM Calls
    print(f'\nOTM Calls (Strike > ${current_price:.2f}):')
    otm_calls = calls[calls['strike'] > current_price].head(8)
    for _, row in otm_calls.iterrows():
        print(f'  ${row["strike"]:.0f}C: Last ${row["lastPrice"]:.2f} | Bid ${row["bid"]:.2f} | Ask ${row["ask"]:.2f} | Vol: {row["volume"]} | OI: {row["openInterest"]} | IV: {row["impliedVolatility"]:.1%}')

    # OTM Puts
    print(f'\nOTM Puts (Strike < ${current_price:.2f}):')
    otm_puts = puts[puts['strike'] < current_price].tail(8)
    for _, row in otm_puts.iterrows():
        print(f'  ${row["strike"]:.0f}P: Last ${row["lastPrice"]:.2f} | Bid ${row["bid"]:.2f} | Ask ${row["ask"]:.2f} | Vol: {row["volume"]} | OI: {row["openInterest"]} | IV: {row["impliedVolatility"]:.1%}')

# =============================================================================
# 6. NEWS ANALYSIS
# =============================================================================
print('\n6. RECENT APP NEWS')
print('-' * 40)
news = app.news
for article in news[:8]:
    pub_time = datetime.fromtimestamp(article.get('providerPublishTime', 0))
    print(f'  [{pub_time.strftime("%Y-%m-%d")}] {article.get("title", "No title")[:70]}...')
    print(f'    Source: {article.get("publisher", "Unknown")}')

# =============================================================================
# 7. AD SECTOR CORRELATION
# =============================================================================
print('\n7. AD SECTOR CORRELATION')
print('-' * 40)
ad_tickers = ['META', 'GOOGL', 'TTD']
print('Fetching ad sector data for correlation...')

for ticker in ad_tickers:
    try:
        stock = yf.Ticker(ticker)
        hist = stock.history(period='1y')
        hist['Daily_Return'] = hist['Close'].pct_change() * 100

        # Align with APP data
        merged = pd.merge(
            app_history[['Daily_Return']].rename(columns={'Daily_Return': 'APP'}),
            hist[['Daily_Return']].rename(columns={'Daily_Return': ticker}),
            left_index=True, right_index=True, how='inner'
        )
        correlation = merged['APP'].corr(merged[ticker])
        print(f'  APP vs {ticker}: {correlation:.3f} correlation')
    except Exception as e:
        print(f'  {ticker}: Error - {e}')

# =============================================================================
# 8. SUMMARY
# =============================================================================
print('\n' + '=' * 60)
print('SUMMARY & KEY FINDINGS')
print('=' * 60)

print('\n1. FRIDAY DYNAMICS:')
if friday_stats:
    avg_range = np.mean([s['Range%'] for s in friday_stats])
    max_range = max([s['Range%'] for s in friday_stats])
    print(f'   - Average Friday intraday range: {avg_range:.1f}%')
    print(f'   - Max Friday intraday range: {max_range:.1f}%')

print('\n2. BIG MOVE PATTERNS:')
print(f'   - Total >5% move days in past year: {len(big_moves)}')
print(f'   - Friday big moves: {len(friday_big_moves)} ({len(friday_big_moves)/len(big_moves)*100:.0f}% of all big moves)')

print('\n3. 750% OPTION GAIN REQUIREMENTS:')
print(f'   - Current price: ${current_price:.2f}')
print(f'   - For a $0.50 OTM option to gain 750%, stock must move ~$4.25 beyond strike')
print(f'   - This requires approximately {4.25/current_price*100:.1f}% move beyond strike')

print('\n4. CATALYST SIGNALS TO MONITOR:')
print('   - Ad sector news (META, GOOGL earnings/guidance)')
print('   - Direct APP news (partnerships, S&P changes)')
print('   - Pre-market momentum on Fridays')
print('   - Unusual options volume on OTM strikes')

print('\n' + '=' * 60)
print('Analysis complete!')
print('=' * 60)
