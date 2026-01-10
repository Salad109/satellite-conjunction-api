import pandas as pd
import matplotlib.pyplot as plt
import math

df = pd.read_csv('conjunction_benchmark.csv')
prepass_values = df['prepass_km'].unique()
datasets = [(p, df[df['prepass_km'] == p]) for p in sorted(prepass_values)]

# Plot 1
fig, ax = plt.subplots(figsize=(10, 6))
for i, (prepass, df) in enumerate(datasets):
    ax.plot(df['tolerance_km'], df['total_s'], 'o-', linewidth=2, markersize=8, label=f'Prepass {prepass}')
    min_time_idx = df['total_s'].idxmin()
    ax.axvline(x=df.loc[min_time_idx, 'tolerance_km'], linestyle='--', linewidth=1, alpha=0.5)
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
for i, (prepass, df) in enumerate(datasets):
    ax1.plot(df['tolerance_km'], df['coarse_s'], 'o-', linewidth=2, markersize=8, label=f'Prepass {prepass}')
    ax2.plot(df['tolerance_km'], df['refine_s'], 's-', linewidth=2, markersize=8, label=f'Prepass {prepass}')
ax1.set_xlabel('Tolerance (km)', fontsize=12)
ax1.set_ylabel('Time (s)', fontsize=12)
ax1.set_title('Coarse Processing Time', fontsize=14, fontweight='bold')
ax1.legend(fontsize=11)
ax1.grid(True, alpha=0.3)
ax2.set_xlabel('Tolerance (km)', fontsize=12)
ax2.set_ylabel('Time (s)', fontsize=12)
ax2.set_title('Refine Processing Time', fontsize=14, fontweight='bold')
ax2.legend(fontsize=11)
ax2.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig('2_coarse_vs_refine.png', dpi=300, bbox_inches='tight')
plt.close()

# Plot 3
n_datasets = len(datasets)
n_cols = 4
n_rows = math.ceil(n_datasets / n_cols)
fig, axes = plt.subplots(n_rows, n_cols, figsize=(6*n_cols, 6*n_rows))
axes = axes.flatten()
for i, (prepass, df) in enumerate(datasets):
    axes[i].fill_between(df['tolerance_km'], 0, df['coarse_s'], label='Coarse', alpha=0.7)
    axes[i].fill_between(df['tolerance_km'], df['coarse_s'], df['total_s'], label='Refine', alpha=0.5)
    axes[i].set_xlabel('Tolerance (km)', fontsize=12)
    axes[i].set_ylabel('Time (s)', fontsize=12)
    axes[i].set_title(f'Prepass {prepass} Time Breakdown', fontsize=14, fontweight='bold')
    axes[i].legend(fontsize=11)
    axes[i].grid(True, alpha=0.3)
for i in range(n_datasets, len(axes)):
    axes[i].axis('off')
plt.tight_layout()
plt.savefig('3_time_breakdown.png', dpi=300, bbox_inches='tight')
plt.close()

# Plot 4
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))
for i, (prepass, df) in enumerate(datasets):
    ax1.plot(df['tolerance_km'], df['conj'], 'o-', linewidth=2, markersize=8, label=f'Prepass {prepass}')
ax1.set_xlabel('Tolerance (km)', fontsize=12)
ax1.set_ylabel('Conjunctions', fontsize=12)
ax1.set_title('Conjunctions Detected', fontsize=14, fontweight='bold')
ax1.legend(fontsize=11)
ax1.grid(True, alpha=0.3)

# Bar chart of average conjunctions
prepass_list = [p for p, _ in datasets]
avg_conj = [df['conj'].mean() for _, df in datasets]
bars = ax2.bar(range(len(prepass_list)), avg_conj, color='#2E86AB', alpha=0.7)
ax2.set_xticks(range(len(prepass_list)))
ax2.set_xticklabels([f'{p}' for p in prepass_list])
ax2.set_xlabel('Prepass Tolerance (km)', fontsize=12)
ax2.set_ylabel('Average Conjunctions', fontsize=12)
ax2.set_title('Average Conjunctions by Prepass', fontsize=14, fontweight='bold')
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
