import pandas as pd
import numpy as np
import sys
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from stylelib import *

PREFIX = "AGG_"
# default in seconds
y_agg_latency = ["average_latency", "99%_latency"]
y_agg_dist_latency = ["average_dist_latency", "99%_dist_latency"]
# default in us
y_single_part_latency = ['single_part_execute_phase (in us)', 'single_part_precommit_phase (in us)',
                         'single_part_commit_phase (in us)', 'single_part_abort (in us)']
y_multi_part_latency = ['multi_part_execute_phase (in us)', 'multi_part_prepare_phase (in us)',
                        'multi_part_commit_phase (in us)', 'multi_part_abort (in us)']
WEIGHT = {
    "latency": "num_commits",
    "dist_latency": "num_multi_part_txn", 
    "multi_part_latency": "num_multi_part_txn",
    "single_part_latency": "num_single_part_txn",
}

TYPE = {
    "latency": y_agg_latency,
    "dist_latency": y_agg_dist_latency, 
    "multi_part_latency": y_multi_part_latency,
    "single_part_latency": y_single_part_latency,
}

C_PQ = "RED"
C_ORC = "BLUE"

def apply_mask(df, mask):
    for m in mask:
        if len(m) == 3:
            df = df[df[m[0]] != m[1]]
        else:
            if isinstance(m[1], list):
                # format [operation, value], e.g. ["le", 0]
                if m[1][0] == "le":
                    df = df[df[m[0]] <= m[1][1]]
                elif m[1][0] == "ge":
                    df = df[df[m[0]] >= m[1][1]]
                elif m[1][0] == "lt":
                    df = df[df[m[0]] < m[1][1]]
                elif m[1][0] == "gt":
                    df = df[df[m[0]] > m[1][1]]
                elif m[1][0] == "eq":
                    df = df[df[m[0]] == m[1][1]]
                elif m[1][0] == "ne":
                    df = df[df[m[0]] != m[1][1]]
            else:
                df = df[df[m[0]] == m[1]]
    return df

def get_x_pos(num_col=2, bar_space=0.05, group_space=0.1):
    width = (1 - group_space) / num_col
    offsets = np.array([i*width -0.5+width/2.0 for i in range(num_col)])
    return width, offsets

def get_pattern_patches(labels, patterns):
    patches = []
    for i, l in enumerate(labels):
        p = mpatches.Patch(facecolor=(1, 1, 1, 0), edgecolor='black',alpha=.99,hatch=patterns[i],label=l)
        patches.append(p)
    return patches

def get_color_patches(labels, colors):
    patches = []
    for i, l in enumerate(labels):
        p = mpatches.Patch(facecolor=colors[i], edgecolor='black',alpha=.99, label=l)
        patches.append(p)
    return patches

def select(data, target, cols):
    local = data.reset_index().set_index(cols)
    result = []
    return [local.loc[tuple(vs)]["index"] for vs in target[cols].values]

def concat(dfs):
    if len(dfs) == 1:
        return dfs[0]
    lengths = [df.shape[1] for df in dfs]
    n = sum([df.shape[0] for df in dfs])
    if (len(set(lengths)) != 1):
        print("DATAFRAME has different columns!")
        cols = set(dfs[0].columns.values)
        for i, df in enumerate(dfs[1:]):
            for c in cols:
                if c not in df:
                    print("{} is missing in df[{}]".format(c, i+1))
            for c in df:
                if c not in cols:
                    print("{} is missing in df[:{}]".format(c, i+1))
            cols = cols.union(set(df.columns.values))
        return
    df = pd.concat(dfs, ignore_index=True, sort=False)
    if (n != df.shape[0]):
        print("DATAFRAME has missing rows!")
    return df

def append_column(df, name, val):
    df[name] = [val] * df.shape[0]
    return df.reindex(sorted(df.columns), axis=1)

def aggregate(x, df, latency_type="dist_latency", agg="latency", how="minmax"):
    # drop invalid data
    data = df.dropna(subset=y_agg_latency+["Throughput"], axis=0)
    data = data[data['Throughput'] != 0]
    throughput = process_throughput(x, data)
    latency = process_latency(x, data, latency_type=latency_type)
    select_cols = ['COMMIT_ALG', 'i', x]
    if agg != "throughput":
        if how == "minmax":
            idx_max = latency.groupby(['COMMIT_ALG', x])[PREFIX+TYPE[latency_type][0]].idxmin().values
            print("selected trial with min {}".format(PREFIX+TYPE[latency_type][0]))
        elif how == "median":
            agg_col = PREFIX+TYPE[latency_type][0]
            idx_max = []
            for name, group in latency.groupby(['COMMIT_ALG', x]):
                n = int(group.shape[0] / 2) - 1
                idx_max.append(group.sort_values(agg_col).index.values[n])
            print("selected trial with median {}".format(PREFIX+TYPE[latency_type][0]))
        agg_latency = latency.loc[idx_max]
        agg_throughput = throughput.loc[select(throughput, agg_latency, select_cols)]
    else:
        # select the trials with highest throughput
        idx_max = agg_throughput.groupby(['COMMIT_ALG', x])['Throughput'].idxmax().values
        agg_throughput = agg_throughput.loc[idx_max]
        print("selected trial with max throughput")
        agg_latency = latency.loc[select(latency, agg_throughput, select_cols)]
    return agg_throughput, agg_latency
        
def process_throughput(x, df):
    if isinstance(x, list):
        agg_throughput = df.groupby(['COMMIT_ALG', 'i'] + x, as_index=False)['Throughput'].sum()
    else:
        # sum up throughput for each trial
        agg_throughput = df.groupby(['COMMIT_ALG', 'i', x], as_index=False)['Throughput'].sum()
    return agg_throughput

def process_latency(x, df, latency_type="dist_latency", select=None):
    # require agg_throughput to indicate trial id as trials are selected by max throughput
    # process the latency
    latency = []
    if select is not None:
        select = select.set_index(["COMMIT_ALG", x])
    for (alg, i, v), group in df.groupby(['COMMIT_ALG', 'i', x]):
        if select is not None:
            if select.loc[(alg, v), "i"] != i:
                continue
        # calculate weighted sum for each column/attribute
        weight = WEIGHT[latency_type]
        record = {"COMMIT_ALG": alg, "i": i, x: v}
        for y in TYPE[latency_type]:
            if group[weight].sum() == 0:
                print("[WARNING]: zero weight %s" % weight)
                agg = np.nan
                break
            agg = (group[y] * group[weight]).sum() / group[weight].sum()
            record[PREFIX+y] = agg
        if np.isnan(agg):
            continue
        latency.append(record)
    latency = pd.DataFrame(data=latency)
    return latency


def plot_latency(x, data, ys=y_agg_latency, fig=None, ax=None, xlabel="", prefix=PREFIX, ylim=1.5, 
                 line_colors=BLUES, width=5, unit="ms", ylim_speedup=1.5, 
                 plot_speedup=False, speedup_format="text", speedup_yspace=5, speedup_xspace=-1):
    linestyle = ["-", "--"] # avg, tail
    if fig is None:
        fig, ax = plt.subplots()
    marker = ["^", "o"] # 1pc, 2pc
    color = [line_colors[C_1PC], line_colors[C_2PC]]
    alglabel = ["Cornus", "2PC"]
    scale = 1
    if unit == "ms":
        scale = 1000
    elif unit == "us":
        scale = 1000000    
    ymax = 0
    for i, alg in enumerate(["ONE_PC", "TWO_PC"]):
        y_data = data[data['COMMIT_ALG'] == alg].sort_values(by=x)
        for j, y in enumerate(ys):
            # default unit is second
            y_data[prefix+y] = y_data[prefix+y] * scale
            ymax = max(y_data[prefix+y].max(), ymax)
            ax.plot(x, prefix+y, marker=marker[i], data=y_data, linestyle=linestyle[j], 
                    label=alglabel[i] + " "+ y.replace('_',' '), color=color[i])
    ax.legend(["Cornus avg", "Cornus 99%", "2PC avg", "2PC 99%"],
              loc='upper left', ncol=1, labelspacing=0.1, prop={'size': 14},
              columnspacing=0, handletextpad=0.1, frameon=False, bbox_to_anchor=(-0.01,1.05))
    ax.set_ylim([0, ymax*ylim])
    ax.set_xlabel(xlabel)
    ax.set_ylabel("Txn Latency ({})".format(unit))
    plt.grid(axis='y', linestyle='--', linewidth=0.35)
    fig.set_size_inches(5, 2.8)
    if plot_speedup:
        twopc = apply_mask(data, [("COMMIT_ALG", "TWO_PC")])[prefix+ys[0]].values * scale
        onepc = apply_mask(data, [("COMMIT_ALG", "ONE_PC")])[prefix+ys[0]].values * scale
        speedup = twopc / onepc
        print("speedup: {}".format(speedup))
        x_data = data[x].unique()
        if speedup_format == "text":
            for x, y, s in zip(x_data, twopc, speedup):
                ax.text(x + speedup_xspace, y + speedup_yspace, "%.1fx"%(s), fontsize=12)
            return fig, ax, None
        else:
            ax2 = ax.twinx()
            ax2_legend = "Speedup in avg latency"
            ax2_name = "Speedup of 1PC \nw.r.t. 2PC"
            ax2.bar(data[x].unique(), y_data, width=width, color=tuple(list(GREYS[0])[:3]+[0.5]))
            ax2.set_ylim(0, y_data.max() * ylim_speedup)
            ax2.legend([ax2_legend], loc="upper left", prop={'size': 14}, bbox_to_anchor=(-0.015,1.2), frameon=False)
            ax2.set_ylabel(ax2_name)
            ax2.set_xticks(data[x].unique())
            return fig, ax, ax2
    return fig, ax

def plot_throughput(x, data, ys=y_agg_latency, fig=None, ax=None, xlabel="", prefix=PREFIX, ylim=1.5, 
                 line_colors=BLUES, plot_reduction=False, reduction=True, width=5):
    linestyle = ["-", "--"] # avg, tail
    if fig is None:
        fig, ax = plt.subplots()
    marker = ["^", "o"] # 1pc, 2pc
    color = [line_colors[C_1PC], line_colors[C_2PC]]
    alglabel = ["1PC", "2PC"]
    for i, alg in enumerate(["ONE_PC", "TWO_PC"]):
        y_data = data[data['COMMIT_ALG'] == alg].sort_values(by=x)
        for j, y in enumerate(ys):
            ax.plot(x, prefix+y, marker=marker[i], data=y_data, linestyle=linestyle[j], 
                    label=alglabel[i] + " "+ y.replace('_',' '), color=color[i])
    ax.legend(["1PC avg latency", "1PC 99% latency", "2PC avg latency", "2PC 99% latency"],
              loc='upper left', ncol=1, labelspacing=0.1, prop={'size': 14},
              columnspacing=0, handletextpad=0.1, frameon=False, bbox_to_anchor=(-0.01,1.05))
    ymax = np.array([data[prefix+y].max() for y in ys]).max()
    ax.set_ylim([0, ymax*ylim])
    ax.set_xlabel(xlabel)
    ax.set_ylabel("Throughput (txns/s)")
    plt.grid(axis='y', linestyle='--', linewidth=0.35)
    fig.set_size_inches(5, 2.8)
    if plot_reduction:
        ax2 = ax.twinx()
        twopc = apply_mask(data, [("COMMIT_ALG", "TWO_PC")])[prefix+ys[0]].values
        onepc = apply_mask(data, [("COMMIT_ALG", "ONE_PC")])[prefix+ys[0]].values
        if reduction:
            y_data = (onepc - twopc) 
            ax2_legend = "Improvements in throughput"
            ax2_name = "AVG Throughput Improvements"
        else:
            y_data = onepc / twopc
            ax2_legend = "Speedup in throughput"
            ax2_name = "Speedup of 1PC \nw.r.t. 2PC"
        print("speedup:")
        print(y_data)
        ax2.bar(data[x].unique(), y_data, width=width, color=tuple(list(BLUES[2])[:3]+[0.5]))
        ax2.set_ylim(0, y_data.max() * 1.5)
        ax2.legend([ax2_legend], loc="upper left", prop={'size': 14}, 
                   bbox_to_anchor=(-0.015,1.2), frameon=False)
        ax2.set_ylabel(ax2_name)
        ax2.set_xticks(data[x].unique())
        return fig, ax, ax2
    return fig, ax


def plot_breakdown(x, zipper, xcategories, bar_colors, grad_colors, how="max", agg_multi_latency=None, ax=None, fig=None,
                   extra_legend=True, unit="ms", ylim=1.5):
    if ax is None:
        fig, ax = plt.subplots()
    color_patches = [bar_colors[c] for c in [C_1PC, C_2PC]]
    color = [grad_colors[C_1PC+"S"], grad_colors[C_2PC+"S"]]
    pattern = ALL_PATTERNS
    offset = [-0.1, 0.1]
    
    for idx, (d, ys) in enumerate(zipper):
        if how == "max":
            data = apply_mask(d, [(x, d[x].max())])
        else:
            data = apply_mask(agg_multi_latency, [(x, d)])
        for j, alg in enumerate(["ONE_PC", "TWO_PC"]):
            bottom = 0
            plot_data = apply_mask(data, [("COMMIT_ALG", alg)])
            for i, y in enumerate(ys):
                v = plot_data[PREFIX+y].values
                if unit == "ms":
                    v = v / 1000
                elif unit == "s":
                    v = v / 1000000
                p = ax.bar(idx/2.0+offset[j], v, bottom=bottom, width=0.18, 
                           color=color[j][i], hatch=pattern[i], edgecolor='black')
                bottom += v

    plt.grid(axis='y', linestyle='--', linewidth=0.35)
    if extra_legend:
        legend2 = plt.legend(get_color_patches(["1PC", "2PC"], color_patches), ["Cornus (left)", "2PC (right)"], 
                             loc="upper left", bbox_to_anchor=((-0.015,1.2)), ncol=2, frameon=False,
                             prop={'size': 14})
        plt.gca().add_artist(legend2)
    labels = ["execution", "prepare", "commit", "abort"]
    ax.legend(get_pattern_patches(labels, pattern), labels,prop={'size': 14}, 
              loc='upper left', ncol=1, labelspacing=0.1, 
              columnspacing=0, handletextpad=0.5, frameon=False)
    ax.set_xlabel("Txn Type")
    ax.set_ylabel("Latency Breakdown \n(%s)"%unit)
    plt.ylim(0, bottom.max()*ylim)
    plt.xticks(np.arange(0, len(xcategories)*0.5, 0.5), xcategories)
    fig.set_size_inches(5, 2.5)
    return fig, ax


def export_legend(legend, filename="legend.png"):
    fig  = legend.figure
    fig.canvas.draw()
    bbox  = legend.get_window_extent().transformed(fig.dpi_scale_trans.inverted())
    fig.savefig(filename, dpi="figure", bbox_inches=bbox)