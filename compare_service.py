import os
import tempfile
import pandas as pd
import json
import plotly.express as px
import plotly.graph_objects as go
from zipfile import ZipFile
from pathlib import Path
import re
from datetime import datetime

# KPI columns
KPI_COLS = ["Reported Value to Regulator", "Reported Value to Group", "Actual Value MAPS Networks"]

# Custom colour palette
COLORWAY = ["#003f5c", "#bc5090", "#ffa600", "#2f4b7c", "#ff6361"]

def run_comparison_pipeline(zip_bytes, selected_year, country="All Countries"):
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            # Save and extract ZIP
            zip_path = os.path.join(tmpdir, "uploaded.zip")
            with open(zip_path, "wb") as f:
                f.write(zip_bytes)
            with ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(tmpdir)

            # Find Excel files for the given year
            excel_files = list(Path(tmpdir).rglob("*.xlsx"))
            year_files = {}
            for file in excel_files:
                name = file.stem
                if re.match(r"[A-Za-z]{3}\d{4}", name):
                    month_str = name[:3]
                    year_str = name[3:]
                    if year_str == str(selected_year):
                        year_files[month_str] = str(file)

            if not year_files:
                return None, None, f"No files found for year {selected_year}."

            # Load and merge all year files
            df_list = []
            for month_str, filepath in year_files.items():
                df_month = pd.read_excel(filepath)
                df_month["Month"] = month_str
                df_list.append(df_month)
            df = pd.concat(df_list, ignore_index=True)

            # Ensure Country column exists
            if "Country" not in df.columns:
                df["Country"] = "Unknown"

            # Filter by country
            if country != "All Countries":
                df = df[df["Country"] == country]

            if df.empty:
                return None, None, f"No data found for {country} in {selected_year}."

            # Calculate differences
            df["Diff_Reg_vs_Grp"] = (df[KPI_COLS[0]] - df[KPI_COLS[1]]).abs()
            df["Diff_Reg_vs_Act"] = (df[KPI_COLS[0]] - df[KPI_COLS[2]]).abs()
            df["Diff_Grp_vs_Act"] = (df[KPI_COLS[1]] - df[KPI_COLS[2]]).abs()
            df["All_Match"] = df[KPI_COLS].nunique(axis=1) == 1

            # Summary stats
            total = len(df)
            all_match = df["All_Match"].sum()
            mismatch = total - all_match
            summary = (
                f"KPI Summary for {selected_year} ({country}):\n"
                f"- Total Records: {total}\n"
                f"- ✅ All KPIs Match: {all_match} ({all_match/total:.1%})\n"
                f"- ⚠️ Mismatches: {mismatch} ({mismatch/total:.1%})\n"
                f"- Avg Diff Reg vs Group: {df['Diff_Reg_vs_Grp'].mean():.2f}\n"
                f"- Avg Diff Reg vs Actual: {df['Diff_Reg_vs_Act'].mean():.2f}\n"
                f"- Avg Diff Group vs Actual: {df['Diff_Grp_vs_Act'].mean():.2f}"
            )

            plots = {}

            # 1. KPI Agreement Pie Chart
            pie_chart = px.pie(
                names=["All Match", "Mismatch"],
                values=[all_match, mismatch],
                title="KPI Agreement Overview",
                color_discrete_sequence=COLORWAY
            )
            plots["agreement_pie"] = json.loads(pie_chart.to_json())

            # 2. Pairwise KPI Difference Bar
            diff_means = pd.DataFrame({
                "Comparison": ["Reg vs Group", "Reg vs Actual", "Group vs Actual"],
                "Avg_Diff": [
                    df["Diff_Reg_vs_Grp"].mean(),
                    df["Diff_Reg_vs_Act"].mean(),
                    df["Diff_Grp_vs_Act"].mean()
                ]
            })
            diff_bar = px.bar(
                diff_means, x="Comparison", y="Avg_Diff", text="Avg_Diff",
                title="Average KPI Differences", color="Comparison",
                color_discrete_sequence=COLORWAY
            )
            plots["pairwise_diff_bar"] = json.loads(diff_bar.to_json())

            # 3. Top 10 Largest Mismatches Table
            df["Max_Diff"] = df[["Diff_Reg_vs_Grp", "Diff_Reg_vs_Act", "Diff_Grp_vs_Act"]].max(axis=1)
            top_mismatches = df.nlargest(10, "Max_Diff")[
                ["Country", "Month"] + KPI_COLS + ["Max_Diff"]
            ]
            table_fig = go.Figure(data=[go.Table(
                header=dict(values=list(top_mismatches.columns),
                            fill_color="#003f5c", font=dict(color="white")),
                cells=dict(values=[top_mismatches[col] for col in top_mismatches.columns])
            )])
            table_fig.update_layout(title="Top 10 Largest KPI Mismatches")
            plots["top_mismatches_table"] = json.loads(table_fig.to_json())

            # 4. Deviation Heatmap
            heatmap_data = df.groupby("Country")[["Diff_Reg_vs_Grp", "Diff_Reg_vs_Act", "Diff_Grp_vs_Act"]].mean().reset_index()
            heatmap_fig = px.imshow(
                heatmap_data.set_index("Country"),
                color_continuous_scale="RdYlGn_r",
                title="Average KPI Differences by Country"
            )
            plots["deviation_heatmap"] = json.loads(heatmap_fig.to_json())

            # 5. Country-wise Monthly Trend (Agreement %)
            trend_data = (
                df.groupby(["Month", "Country"])["All_Match"]
                .mean().reset_index()
            )
            trend_fig = px.line(
                trend_data, x="Month", y="All_Match", color="Country",
                title="KPI Agreement % Trend by Country",
                markers=True
            )
            trend_fig.update_yaxes(tickformat=".0%")
            plots["monthly_trend"] = json.loads(trend_fig.to_json())

            return plots, summary, None

    except Exception as e:
        return None, None, f"Processing error: {str(e)}"
