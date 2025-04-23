from matplotlib import pyplot as plt
import pandas as pd

# Load the output file
with open('./src/main/resources/sentiment_output.txt', 'r', encoding='utf-8') as f:
    lines = f.readlines()

# Parse .tsv output file where the value is a comma-separated list of counts
data = [] # will contain objects where each game has all calculated metrics
for line in lines:
    parts = line.strip().split('\t') # split by tab
    if len(parts) != 2:
        continue # skip malformed
    game = parts[0].strip() # game is always first value before tab
    try:
        positive, negative, neutral = map(int, parts[1].split(',')) # split values in order
    except ValueError:
        continue  # skip malformed 

    total = positive + negative + neutral # total # of reviews 
    data.append({
        'game': game,
        'positive': positive,
        'negative': negative,
        'neutral': neutral,
        'total': total,
        'pos_pct': positive / total if total > 0 else 0, # % positive reviews 
        'neg_pct': negative / total if total > 0 else 0, # % negative reviews
        'neu_pct': neutral / total if total > 0 else 0, # % neutral reviews
        'controversy': positive * negative, # controversy level
        'polarity_gap': abs((positive - negative) / total) if total > 0 else 0 # polarity gap
    })

# Load into DataFrame
df = pd.DataFrame(data)

# Analysis 1: Top 10 most positively rated games
top_positive = df.sort_values(by='positive', ascending=False).head(10)

# Analysis 2: Top 10 most negatively reviewed games.
top_negative = df.sort_values(by='negative', ascending=False).head(10)

# Analysis 3: Top 10 most neutrally reviewed games.
most_neutral = df[df['total'] > 50].sort_values(by='neu_pct', ascending=False).head(10)

# Analysis 4: Top 10 most controversial games.
most_controversial = df.sort_values(by='controversy', ascending=False).head(10)

# Analysis 5: Top 10 most polarizing games. 
most_polarizing = df[df['total'] > 50].sort_values(by='polarity_gap', ascending=False).head(10)

# Display results
print("Top 10 Most Positively Reviewed Games:")
print(top_positive[['game', 'positive', 'total', 'pos_pct']])

print("\n Top 10 Most Negatively Reviewed Games:")
print(top_negative[['game', 'negative', 'total', 'neg_pct']])

print("\n Top 10 Most Neutrally Reviewed Games:")
print(most_neutral[['game', 'neutral', 'total', 'neu_pct']])

print("\n Top 10 Most Controversial Games (Positive * Negative):")
print(most_controversial[['game', 'positive', 'negative', 'controversy']])

print("\n Top 10 Most Polarizing Games:")
print(most_polarizing[['game', 'positive', 'negative', 'polarity_gap']])


# Plot charts for visual analysis 
def plot_bar(data, x_col, y_col, title, xlabel, ylabel):
    colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd',
          '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22', '#17becf'] # randomize colors of bar chart: https://stackoverflow.com/a/57182214
    
    plt.figure(figsize=(12, 6))
    
    plt.bar(data[x_col], data[y_col], color=colors)
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.xticks(rotation=75, ha='right')
    plt.tight_layout()
    plt.show()

plot_bar(top_positive, 'game', 'positive', 'Top 10 Most Positively Reviewed Games', 'Game', 'Positive Reviews')
plot_bar(top_negative, 'game', 'negative', 'Top 10 Most Negatively Reviewed Games', 'Game', 'Negative Reviews')
plot_bar(most_neutral, 'game', 'neu_pct', 'Top 10 Most Neutrally Reviewed Games', 'Game', 'Neutral Review Percentage')
plot_bar(most_controversial, 'game', 'controversy', 'Top 10 Most Controversial Games', 'Game', 'Controversy Score')
plot_bar(most_polarizing, 'game', 'polarity_gap', 'Top 10 Most Polarizing Games', 'Game', 'Polarity Gap')