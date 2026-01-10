import pandas as pd
import matplotlib.pyplot as plt

df1 = pd.read_csv('conjunction_benchmark_golden.csv')
df2 = pd.read_csv('conjunction_benchmark_brent.csv')

# Plot 1
fig, ax = plt.subplots(figsize=(10, 6))
ax.plot(df1['tolerance_km'], df1['total_s'], 'o-', color='#2E86AB', linewidth=2, markersize=8, label='Golden section search')
ax.plot(df2['tolerance_km'], df2['total_s'], 's-', color='#D62839', linewidth=2, markersize=8, label='Brent\'s method')
min_time_idx1 = df1['total_s'].idxmin()
min_time_idx2 = df2['total_s'].idxmin()
ax.axvline(x=df1.loc[min_time_idx1, 'tolerance_km'], color='#2E86AB', linestyle='--', linewidth=1, alpha=0.5)
ax.axvline(x=df2.loc[min_time_idx2, 'tolerance_km'], color='#D62839', linestyle='--', linewidth=1, alpha=0.5)
ax.set_xlabel('Tolerance (km)', fontsize=12)
ax.set_ylabel('Total Time (s)', fontsize=12)
ax.set_title('Total Processing Time vs Tolerance', fontsize=14, fontweight='bold')
ax.legend(fontsize=11)
ax.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig('1_total_time.png', dpi=300, bbox_inches='tight')
plt.close()

# Plot 2
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))
ax1.plot(df1['tolerance_km'], df1['coarse_s'], 'o-', color='#2E86AB', linewidth=2, markersize=8, label='Golden section search')
ax1.plot(df2['tolerance_km'], df2['coarse_s'], 's-', color='#D62839', linewidth=2, markersize=8, label='Brent\'s method')
ax1.set_xlabel('Tolerance (km)', fontsize=12)
ax1.set_ylabel('Time (s)', fontsize=12)
ax1.set_title('Coarse Processing Time', fontsize=14, fontweight='bold')
ax1.legend(fontsize=11)
ax1.grid(True, alpha=0.3)
ax2.plot(df1['tolerance_km'], df1['refine_s'], 'o-', color='#2E86AB', linewidth=2, markersize=8, label='Golden section search')
ax2.plot(df2['tolerance_km'], df2['refine_s'], 's-', color='#D62839', linewidth=2, markersize=8, label='Brent\'s method')
ax2.set_xlabel('Tolerance (km)', fontsize=12)
ax2.set_ylabel('Time (s)', fontsize=12)
ax2.set_title('Refine Processing Time', fontsize=14, fontweight='bold')
ax2.legend(fontsize=11)
ax2.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig('2_coarse_vs_refine.png', dpi=300, bbox_inches='tight')
plt.close()

# Plot 3
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))
ax1.fill_between(df1['tolerance_km'], 0, df1['coarse_s'], label='Coarse', alpha=0.7, color='#06A77D')
ax1.fill_between(df1['tolerance_km'], df1['coarse_s'], df1['total_s'], label='Refine', alpha=0.7, color='#D62839')
ax1.set_xlabel('Tolerance (km)', fontsize=12)
ax1.set_ylabel('Time (s)', fontsize=12)
ax1.set_title('Golden section search Processing Time Breakdown', fontsize=14, fontweight='bold')
ax1.legend(fontsize=11)
ax1.grid(True, alpha=0.3)
ax2.fill_between(df2['tolerance_km'], 0, df2['coarse_s'], label='Coarse', alpha=0.7, color='#06A77D')
ax2.fill_between(df2['tolerance_km'], df2['coarse_s'], df2['total_s'], label='Refine', alpha=0.7, color='#D62839')
ax2.set_xlabel('Tolerance (km)', fontsize=12)
ax2.set_ylabel('Time (s)', fontsize=12)
ax2.set_title('Brent\'s Method Processing Time Breakdown', fontsize=14, fontweight='bold')
ax2.legend(fontsize=11)
ax2.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig('3_time_breakdown.png', dpi=300, bbox_inches='tight')
plt.close()

# Plot 4
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))
ax1.plot(df1['tolerance_km'], df1['conj'], 'o-', color='#2E86AB', linewidth=2, markersize=8, label='Golden section search')
ax1.plot(df2['tolerance_km'], df2['conj'], 's-', color='#D62839', linewidth=2, markersize=8, label='Brent\'s method')
ax1.set_xlabel('Tolerance (km)', fontsize=12)
ax1.set_ylabel('Conjunctions', fontsize=12)
ax1.set_title('Conjunctions Detected', fontsize=14, fontweight='bold')
ax1.legend(fontsize=11)
ax1.grid(True, alpha=0.3)

# Bar chart
methods = ['Golden section search', 'Brent\'s method']
avg_conj = [df1['conj'].mean(), df2['conj'].mean()]
colors = ['#2E86AB', '#D62839']
bars = ax2.bar(range(len(methods)), avg_conj, color=colors, alpha=0.7)
ax2.set_xticks(range(len(methods)))
ax2.set_xticklabels(methods)
ax2.set_ylabel('Average Conjunctions', fontsize=12)
ax2.set_title('Average Conjunctions by Method', fontsize=14, fontweight='bold')
ax2.set_ylim(bottom=min(avg_conj) * 0.95)
ax2.grid(True, alpha=0.3, axis='y')
# Add value labels on bars
for bar, val in zip(bars, avg_conj):
    height = bar.get_height()
    ax2.text(bar.get_x() + bar.get_width()/2., height,
             f'{val:.1f}', ha='center', va='bottom', fontsize=10)

plt.tight_layout()
plt.savefig('4_conjunctions.png', dpi=300, bbox_inches='tight')
plt.close()
