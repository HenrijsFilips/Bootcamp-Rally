# rally app interface.
# let users manage teams and cars and run races with money stuff.
# three pages:
# 1. teams. make and see teams
# 2. cars. make and see cars 
# 3. race. run races and see who wins

from __future__ import annotations

import random
from typing import Dict, List, Tuple

import pandas as pd
import streamlit as st

from rally_data_access import (
    add_car,
    add_team,
    credit_winner_prize,
    create_race,
    get_all_cars,
    get_all_teams,
    get_race_results,
    insert_race_results,
    pay_participation_fee_for_all_teams_with_cars,
    set_race_winner,
)
from snowflake_db import get_connection, transaction


# streamlit stuff

st.set_page_config(
    page_title="Bootcamp Rally",
    page_icon="🏁",
    layout="wide",
)


# db connection for session

@st.cache_resource
def _conn():
    # get connection for the session.
    # returns a snowflake connection.
    return get_connection()


# helper stuff

def teams_df() -> pd.DataFrame:
    # get all teams as dataframe.
    # returns teams or empty frame if none.
    data = get_all_teams(_conn())
    if not data:
        return pd.DataFrame(columns=["TEAM_ID", "TEAM_NAME", "MEMBERS", "BUDGET"])
    return pd.DataFrame(data)


def cars_df() -> pd.DataFrame:
    # get all cars as dataframe.
    # returns cars or empty frame if none.
    data = get_all_cars(_conn())
    if not data:
        return pd.DataFrame(
            columns=[
                "CAR_ID",
                "CAR_NAME",
                "TEAM_ID",
                "TEAM_NAME",
                "SPEED",
                "DURABILITY",
                "ACCELERATION",
            ]
        )
    return pd.DataFrame(data)


def simulate_time_minutes(
    speed_kmh: float,
    durability: float,
    acceleration: float,
    track_factor: float,
    variability: Tuple[float, float] = (0.95, 1.05),
    distance_km: float = 100.0,
) -> float:
    # figure out how long car takes to finish race.
    # uses speed durability acceleration and track stuff.
    # adds some random variation too.
    # returns time in minutes.
    # performance stuff
    perf_d = 0.5 + 0.5 * max(0.0, min(1.0, durability))
    perf_a = 0.5 + 0.5 * max(0.0, min(1.0, acceleration))
    
    # random stuff
    rand = random.uniform(variability[0], variability[1])
    
    # speed calc
    eff_speed = speed_kmh * perf_d * perf_a * rand * track_factor
    
    # no crazy times
    eff_speed = max(50.0, eff_speed)
    
    # minutes calc
    time_hours = distance_km / eff_speed
    return round(time_hours * 60.0, 3)


def build_track_catalog() -> Dict[str, Dict[str, float]]:
    # make list of tracks we can use.
    # returns tracks and their details.
    return {
        "Asphalt Sprint": {"factor": 1.00, "desc": "Fast, clean asphalt"},
        "Desert Loop": {"factor": 0.92, "desc": "Sand reduces grip"},
        "Forest Run": {"factor": 0.96, "desc": "Mixed terrain, narrow"},
        "Mountain Pass": {"factor": 0.90, "desc": "Steep climbs, tricky"},
    }


def compute_budgets_map(df: pd.DataFrame) -> Dict[int, float]:
    # match team ids to their money.
    # takes team data and gives back id to budget map.
    return {int(r["TEAM_ID"]): float(r["BUDGET"]) for _, r in df.iterrows()}


def team_name_map(df: pd.DataFrame) -> Dict[int, str]:
    # match team ids to their names.
    # takes team data and gives back id to name map.
    return {int(r["TEAM_ID"]): str(r["TEAM_NAME"]) for _, r in df.iterrows()}


# sidebar nav

st.sidebar.title("Bootcamp Rally")
page = st.sidebar.radio(
    "Navigation", 
    ["Teams", "Cars", "Race"], 
    captions=["Manage teams", "Manage cars", "Run races"]
)


# teams page

if page == "Teams":
    st.header("Teams Management")

    col_l, col_r = st.columns([1, 1])

    with col_l:
        st.subheader("Add new team")
        with st.form("form_add_team", clear_on_submit=True):
            t_name = st.text_input("Team name")
            t_members = st.text_area(
                "Members (comma-separated)", placeholder="Alice,Bob"
            )
            t_budget = st.number_input(
                "Initial budget (USD)", min_value=0.0, value=10000.0, step=500.0
            )
            submitted = st.form_submit_button("Add team")
            if submitted:
                if not t_name.strip():
                    st.error("Team name is required.")
                else:
                    add_team(_conn(), t_name.strip(), t_members.strip(), float(t_budget))
                    st.success(f"Team '{t_name}' added.")
                    st.rerun()

    with col_r:
        st.subheader("Teams")
        df_t = teams_df()
        st.dataframe(df_t, use_container_width=True)


# cars page

elif page == "Cars":
    st.header("Cars Management")

    col_l, col_r = st.columns([1, 1])
    df_t = teams_df()

    with col_l:
        st.subheader("Add new car")
        if df_t.empty:
            st.info("Create a team first.")
        else:
            # team display
            team_display = [f'{r["TEAM_NAME"]} (ID {r["TEAM_ID"]})' for _, r in df_t.iterrows()]
            team_ids = [int(r["TEAM_ID"]) for _, r in df_t.iterrows()]
            
            # pick team
            idx = st.selectbox(
                "Assign to team", 
                options=list(range(len(team_ids))), 
                format_func=lambda i: team_display[i]
            )
            
            # car stuff
            c_name = st.text_input("Car name")
            c_speed = st.number_input(
                "Base speed (km/h)", min_value=120.0, value=220.0, step=5.0
            )
            c_dur = st.slider("Durability", 0.0, 1.0, 0.85, 0.01)
            c_acc = st.slider("Acceleration", 0.0, 1.0, 0.90, 0.01)
            
            # car button
            if st.button("Add car"):
                if not c_name.strip():
                    st.error("Car name is required.")
                else:
                    add_car(
                        _conn(),
                        c_name.strip(),
                        int(team_ids[idx]),
                        float(c_speed),
                        float(c_dur),
                        float(c_acc),
                    )
                    st.success(f"Car '{c_name}' added.")
                    st.rerun()

    with col_r:
        st.subheader("Cars")
        df_c = cars_df()
        st.dataframe(df_c, use_container_width=True)


# race page

else:
    st.header("Rally Race")
    tracks = build_track_catalog()

    # race settings
    with st.expander("Race settings", expanded=True):
        c1, c2, c3 = st.columns([1, 1, 2])
        with c1:
            track_name = st.selectbox("Track", list(tracks.keys()))
        with c2:
            fee = st.number_input(
                "Participation fee (per team)", min_value=0.0, value=1000.0, step=100.0
            )
        with c3:
            prize = st.number_input(
                "Winner prize", min_value=0.0, value=5000.0, step=500.0
            )
        st.caption(f"Track info: {tracks[track_name]['desc']}")

    # show cars
    df_c = cars_df()
    df_t = teams_df()
    st.subheader("Eligible cars")
    st.dataframe(df_c, use_container_width=True)

    # race button
    if st.button("Start race! 🏁", type="primary"):
        if df_c.empty:
            st.warning("No cars available. Add cars first.")
            st.stop()

        # budgets before
        budgets_before = compute_budgets_map(df_t)
        team_names = team_name_map(df_t)

        # make race
        conn = _conn()
        race_id = create_race(conn, track_name=track_name)
        t_factor = float(tracks[track_name]["factor"])

        # calc car times
        rows = []
        for _, row in df_c.iterrows():
            time_min = simulate_time_minutes(
                speed_kmh=float(row["SPEED"]),
                durability=float(row["DURABILITY"]),
                acceleration=float(row["ACCELERATION"]),
                track_factor=t_factor,
            )
            rows.append(
                {
                    "CAR_ID": int(row["CAR_ID"]),
                    "TEAM_ID": int(row["TEAM_ID"]),
                    "CAR_NAME": str(row["CAR_NAME"]),
                    "TEAM_NAME": str(row.get("TEAM_NAME", "")),
                    "TIME_MIN": time_min,
                }
            )

        # sort by time
        rows_sorted = sorted(rows, key=lambda r: r["TIME_MIN"])
        
        # prep results
        results_payload: List[Tuple[int, float, int]] = []
        for pos, r in enumerate(rows_sorted, start=1):
            results_payload.append((int(r["CAR_ID"]), float(r["TIME_MIN"]), int(pos)))

        # who won
        winner_team_id = int(rows_sorted[0]["TEAM_ID"])

        # save everything at once
        with transaction(conn):
            insert_race_results(conn, int(race_id), results_payload)
            set_race_winner(conn, int(race_id), winner_team_id)
            pay_participation_fee_for_all_teams_with_cars(conn, float(fee))
            credit_winner_prize(conn, winner_team_id, float(prize))

        # race done message
        st.success(f"Race {race_id} finished! Winner: {team_names.get(winner_team_id)}")

        # show results
        rr = get_race_results(conn, int(race_id))
        df_rr = pd.DataFrame(rr)
        # sort properly
        if "POSITION" in df_rr.columns:
            df_rr = df_rr.sort_values("POSITION")

        st.subheader("Race results (minutes, lower is better)")
        st.dataframe(df_rr, use_container_width=True)

        # budget changes
        df_t_after = teams_df()
        budgets_after = compute_budgets_map(df_t_after)

        budget_rows = []
        for tid, name in team_names.items():
            before = float(budgets_before.get(tid, 0.0))
            after = float(budgets_after.get(tid, 0.0))
            delta = round(after - before, 2)
            budget_rows.append(
                {"TEAM": name, "BEFORE": before, "AFTER": after, "DELTA": delta}
            )
        df_budget = pd.DataFrame(budget_rows).sort_values("TEAM")
        st.subheader("Budgets (before/after race)")
        st.dataframe(df_budget, use_container_width=True)

    # maybe show budgets
    with st.expander("Current teams and budgets"):
        st.dataframe(df_t, use_container_width=True)