import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_csv('conjunction_benchmark.csv')
min_time_idx = df['total_s'].idxmin()

# Plot 1
fig, ax = plt.subplots(figsize=(10, 6))
ax.plot(df['tolerance_km'], df['total_s'], 'o-', color='#2E86AB', linewidth=2, markersize=8)
ax.axvline(x=df.loc[min_time_idx, 'tolerance_km'], color='red', linestyle='--', linewidth=2, label=f"Optimal: {df.loc[min_time_idx, 'tolerance_km']} km")
ax.set_xlabel('Tolerance (km)', fontsize=12)
ax.set_ylabel('Total Time (s)', fontsize=12)
ax.set_title('Total Processing Time vs Tolerance', fontsize=14, fontweight='bold')
ax.legend(fontsize=11)
ax.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig('1_total_time.png', dpi=300, bbox_inches='tight')
plt.close()

# Plot 2
fig, ax = plt.subplots(figsize=(10, 6))
ax.plot(df['tolerance_km'], df['detections']/1e6, 'o-', color='#A23B72', linewidth=2, markersize=8)
ax.set_xlabel('Tolerance (km)', fontsize=12)
ax.set_ylabel('Detections (millions)', fontsize=12)
ax.set_title('Detections vs Tolerance', fontsize=14, fontweight='bold')
ax.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig('2_detections.png', dpi=300, bbox_inches='tight')
plt.close()

# Plot 3
fig, ax = plt.subplots(figsize=(10, 6))
ax.plot(df['tolerance_km'], df['coarse_s'], 'o-', label='Coarse', color='#06A77D', linewidth=2, markersize=8)
ax.plot(df['tolerance_km'], df['refine_s'], 's-', label='Refine', color='#D62839', linewidth=2, markersize=8)
ax.set_xlabel('Tolerance (km)', fontsize=12)
ax.set_ylabel('Time (s)', fontsize=12)
ax.set_title('Coarse vs Refine Processing Time', fontsize=14, fontweight='bold')
ax.legend(fontsize=11)
ax.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig('3_coarse_vs_refine.png', dpi=300, bbox_inches='tight')
plt.close()

# Plot 4
fig, ax = plt.subplots(figsize=(10, 6))
ax.fill_between(df['tolerance_km'], 0, df['coarse_s'], label='Coarse', alpha=0.7, color='#06A77D')
ax.fill_between(df['tolerance_km'], df['coarse_s'], df['total_s'], label='Refine', alpha=0.7, color='#D62839')
ax.set_xlabel('Tolerance (km)', fontsize=12)
ax.set_ylabel('Time (s)', fontsize=12)
ax.set_title('Processing Time Breakdown', fontsize=14, fontweight='bold')
ax.legend(fontsize=11)
ax.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig('4_time_breakdown.png', dpi=300, bbox_inches='tight')
plt.close()

# Plot 5
fig, ax = plt.subplots(figsize=(10, 6))
ax.plot(df['tolerance_km'], df['conj'], 'o-', color='#5E2C99', linewidth=2, markersize=8)
ax.set_xlabel('Tolerance (km)', fontsize=12)
ax.set_ylabel('Conjunctions', fontsize=12)
ax.set_title('Conjunctions Detected', fontsize=14, fontweight='bold')
ax.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig('5_conjunctions.png', dpi=300, bbox_inches='tight')
plt.close()
